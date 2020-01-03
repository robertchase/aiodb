import struct

from aiodb.connector.mysql.constants import COMMAND, FIELD_TYPE
import aiodb.connector.mysql.charset as charset
import aiodb.connector.mysql.serializer as serializer
import aiodb.connector.mysql.packet as packet


class Context:

    def __init__(self, send, user=None, password=None, database=None,
                 autocommit=False, isolation=None):
        self._send = send
        self.user = user
        self.password = password
        self.database = database
        self.autocommit = autocommit
        self.isolation = isolation

        self.sequence = 0
        self.parameter = {}
        self._query = None
        self.error = None
        self.column_definitions = []
        self.tuples = []

        self.is_connected = False
        self.is_running = False

        self.encoding = charset.charset_by_name('utf8').encoding

    def increment(self):
        self.sequence = (self.sequence + 1) % 256

    def send(self, payload):
        self._send(packet.serialize(payload, self.sequence))
        self.increment()

    @property
    def row_count(self):
        if self.packet.type == 'ok':
            return self.packet.affected_rows
        return None

    @property
    def last_id(self):
        if self.packet.type == 'ok':
            return self.packet.last_insert_id
        return None

    @property
    def message(self):
        if self.packet.type == 'ok':
            return self.packet.message.decode()
        return None

    @property
    def result_set(self):
        columns = [cd.name.decode() for cd in self.column_definitions]
        return columns, self.tuples

    def execute_command(self, command, sql):
        sql = sql.encode(self.encoding)
        sql = struct.pack('B', command) + sql
        self.sequence = 0
        while len(sql):
            packet_size = min(packet.MAX_PACKET_LEN, len(sql))
            self.send(sql[:packet_size])
            sql = sql[packet_size:]

    def on_query_start(self, query):
        pass  # print(f'query start: {query}')


def act_authenticate(context):
    response = packet.handshake_response(
        context.handshake,
        context.user,
        context.password,
        context.database,
    )
    context.send(response)


def act_autocommit(context):
    if context.autocommit == context.handshake.autocommit:
        return 'ok'
    autocommit = 1 if context.autocommit else 0
    sql = f'SET AUTOCOMMIT = {autocommit}'
    context.execute_command(COMMAND.COM_QUERY, sql)


def act_clear(context):
    context.error = None
    context.column_definitions = []
    context.tuples = []


def act_close(context):
    context.is_connected = False


def act_columndefinition(context):
    if len(context.column_definitions) == context.field_length:
        return 'done'

    defn = packet.ColumnDefinition(context.data)

    if defn.type == FIELD_TYPE.JSON:
        encoding = context.encoding
    elif defn.type in FIELD_TYPE.TEXT_TYPES:
        if defn.character_set == FIELD_TYPE.ENCODING_BINARY:
            encoding = None
        else:
            encoding = context.encoding
    else:
        encoding = 'ascii'
    converter = serializer.from_mysql.get(defn.type)

    def convert(encoding, converter):
        def _convert(value):
            if encoding:
                value = value.decode(encoding)
            if converter:
                value = converter(value)
            return value
        return _convert

    defn.convert = convert(encoding, converter)

    context.column_definitions.append(defn)


def act_connected(context):
    context.is_connected = True


def act_dump(context):
    print('dump', len(context.data), context.data)


def act_error(context):
    context.error = context.packet.error_message
    raise Exception(context.error)


def act_field_length(context):
    context.field_length = context.packet.field_length


def act_generic(context):
    context.packet = packet.generic(context.data)
    return context.packet.type


def act_handshake(context):
    context.handshake = packet.Handshake(context.data)


def act_isolation(context):
    if context.isolation is None:
        return 'ok'
    sql = f'SET SESSION TRANSACTION ISOLATION LEVEL {context.isolation}'
    context.execute_command(COMMAND.COM_QUERY, sql)


def act_query(context, query):
    context.is_running = True
    context.on_query_start(query)
    context.execute_command(COMMAND.COM_QUERY, query)


def act_query_complete(context):
    context.is_running = False


def act_query_response(context):
    context.packet = packet.query_response(context.data)
    return context.packet.type


def act_sequence(context, packet, sequence):
    if sequence != context.sequence:
        raise Exception(f'Packet sequence {sequence} != {context.sequence}')
    context.increment()
    context.data = packet


def act_tuple(context):
    if packet.is_ok(context.data) or packet.is_eof(context.data):
        context.packet = packet.generic(context.data)
        return 'done'

    p = packet.Packet(context.data)
    row = []
    for defn in context.column_definitions:
        value = defn.convert(p.read_length_coded_string())
        row.append(value)
    context.tuples.append(row)
