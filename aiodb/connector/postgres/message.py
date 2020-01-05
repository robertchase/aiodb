import struct

import aiodb.connector.postgres.constants as constants
from aiodb.connector.postgres.serializer import from_postgres


class Message:

    def __init__(self, name, **kwargs):
        self.name = name
        self.__dict__.update(kwargs)

    def __repr__(self):
        return f'{self.name} {self.__dict__}'


def string(msg, *values):
    for value in values:
        msg.extend(map(ord, value))
        msg.extend((0,))


def startup(user, database=None, **options):
    msg = bytearray(8)
    msg[5] = 3  # version 3
    string(msg, 'user', user)
    if database:
        string(msg, 'database', database)
    for n, v in options.items():
        string(msg, n, v)
    msg.extend((0,))
    struct.pack_into('>I', msg, 0, len(msg))
    return msg


def serialize(tag, payload):
    """create a message suitable for socket send"""
    msg = bytearray(5)
    msg[0] = ord(tag)
    msg.extend(map(ord, payload))
    msg.extend((0,))
    struct.pack_into('>I', msg, 1, len(msg) - 1)
    return msg


class Context:

    def __init__(self, on_message=None):
        self.data = b''
        self.on_message = on_message
        self.clear()

    def clear(self):
        self.type = None
        self.length = None
        self.payload = None


def act_data(context, data=None):
    if data:
        context.data += data


def act_clear(context):
    context.clear()
    if len(context.data):
        return 'data'


def act_length(context):
    if len(context.data) >= 4:
        context.length = struct.unpack_from('>I', context.data, 0)[0]
        context.data = context.data[4:]
        return 'complete'


def act_message(context):
    message = parse(context.type, context.payload)
    context.on_message(message)


def act_payload(context):
    length = context.length - 4
    if len(context.data) >= length:
        context.payload = context.data[:length]
        context.data = context.data[length:]
        return 'complete'


def act_type(context):
    context.type = struct.unpack_from('c', context.data, 0)[0].decode()
    context.data = context.data[1:]


# ---


def parse(tag, payload):
    return {
        constants.TAG_AUTHENTICATION: parse_authentication,
        constants.TAG_PARAMETER_STATUS: parse_parameter_status,
        constants.TAG_BACKEND_KEY_DATA: parse_backend_key_data,
        constants.TAG_READY_FOR_QUERY: parse_ready_for_query,
        constants.TAG_ROW_DESCRIPTION: parse_row_description,
        constants.TAG_DATA_ROW: parse_data_row,
        constants.TAG_COMMAND_COMPLETE: parse_command_complete,
        constants.TAG_COMMAND_ERROR: parse_command_error,
    }[tag](payload)
    raise Exception('Unhandled packet type {}'.format(tag))


def parse_authentication(payload):
    kwargs = {}
    name = None
    auth_type = struct.unpack_from('>I', payload, 0)[0]
    if auth_type == constants.AUTHENTICATION_OK:
        name = constants.NAME_AUTHENTICATION_OK
    elif auth_type == constants.AUTHENTICATION_MD5_PASSWORD:
        name = constants.NAME_AUTHENTICATION_MD5_PASSWORD
        kwargs['salt'] = payload[4:]
    else:
        raise Exception('unhandled authentication type: {}'.format(auth_type))
    return Message(name, **kwargs)


def parse_parameter_status(payload):
    key, value = payload[:-1].split(b'\x00')
    return Message(
        constants.NAME_PARAMETER_STATUS,
        key=key.decode(),
        value=value.decode(),
    )


def parse_backend_key_data(payload):
    process_id, secret_key = struct.unpack('>II', payload)
    return Message(
        constants.NAME_BACKEND_KEY_DATA,
        process_id=process_id,
        secret_key=secret_key,
    )


def parse_ready_for_query(payload):
    status = chr(payload[0])
    return Message(
        constants.NAME_READY_FOR_QUERY,
        status=status
    )


class Descriptor:

    def __repr__(self):
        return f'column description: {self.__dict__}'

    @classmethod
    def parse(cls, payload):
        self = cls()
        name_index = payload.find(b'\x00')
        self.name = payload[:name_index].decode()
        self.table_id, self.column_id, self.data_type_id, \
            self.data_type_size, self.type_modifier, self.format_code = \
            struct.unpack_from('>IHIHIH', payload, name_index + 1)
        self.convert = from_postgres(self.data_type_id)
        return payload[name_index + 1 + 18:], self


def parse_row_description(payload):
    count = struct.unpack_from('>H', payload, 0)[0]
    payload = payload[2:]
    columns = []
    for i in range(count):
        payload, descriptor = Descriptor.parse(payload)
        columns.append(descriptor)
    return Message(
        constants.NAME_ROW_DESCRIPTION,
        columns=columns,
    )


def parse_data_row(payload):
    count = struct.unpack_from('>H', payload, 0)[0]
    index = 2
    columns = []
    for i in range(count):
        size = struct.unpack_from('>i', payload, index)[0]
        index += 4
        if size == -1:
            value = None
        else:
            value = payload[index: index + size].decode()
            index += size
        columns.append(value)
    return Message(
        constants.NAME_DATA_ROW,
        columns=columns,
    )


def parse_command_complete(payload):
    return Message(
        constants.NAME_COMMAND_COMPLETE,
    )


def parse_command_error(payload):
    e = {e[0]: e[1:] for e in payload.decode().split('\x00') if len(e)}
    return Message(
        constants.NAME_COMMAND_ERROR,
        **e
    )
