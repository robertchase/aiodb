"""manage mysql connection"""
import struct

from aiodb.connector.mysql.constants import COMMAND, FIELD_TYPE
import aiodb.connector.mysql.charset as charset
import aiodb.connector.mysql.serializer as serializer
import aiodb.connector.mysql.packet as packet


class Context:  # pylint: disable=too-many-instance-attributes
    """fsm context for the sql connection"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 send, user=None, password=None, database=None,
                 autocommit=False, isolation=None):
        self._send = send
        self.user = user
        self.password = password
        self.database = database
        self.autocommit = autocommit
        self.isolation = isolation

        self.packet = None
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
        """bump the sequence counter"""
        self.sequence = (self.sequence + 1) % 256

    def send(self, payload):
        """send a payload the mysql server"""
        self._send(packet.serialize(payload, self.sequence))
        self.increment()

    @property
    def row_count(self):
        """return the affected_rows from the current "ok" packet"""
        if self.packet.type == 'ok':
            return self.packet.affected_rows
        return None

    @property
    def last_id(self):
        """return the last_insert_id from the current "ok" packet"""
        if self.packet.type == 'ok':
            return self.packet.last_insert_id
        return None

    @property
    def message(self):
        """return the message from the current "ok" packet"""
        if self.packet.type == 'ok':
            return self.packet.message.decode()
        return None

    @property
    def result_set(self):
        """return a tuple of (columns, rows)"""
        columns = [cd.name.decode() for cd in self.column_definitions]
        return columns, self.tuples

    def execute_command(self, command_type, sql):
        """send off sql command"""
        sql = sql.encode(self.encoding)
        sql = struct.pack('B', command_type) + sql
        self.sequence = 0
        while sql:
            packet_size = min(packet.MAX_PACKET_LEN, len(sql))
            self.send(sql[:packet_size])
            sql = sql[packet_size:]

    def on_query_start(self, query):
        """do something at query start"""


def act_authenticate(context):
    """action routine for authenticate"""
    response = packet.handshake_response(
        context.handshake,
        context.user,
        context.password,
        context.database,
    )
    context.send(response)


def act_autocommit(context):
    """action routine for autocommit"""
    if context.autocommit == context.handshake.autocommit:
        return 'ok'
    autocommit = 1 if context.autocommit else 0
    sql = f'SET AUTOCOMMIT = {autocommit}'
    context.execute_command(COMMAND.COM_QUERY, sql)
    return None


def act_clear(context):
    """action routine for clear"""
    context.error = None
    context.column_definitions = []
    context.tuples = []


def act_close(context):
    """action routine for close"""
    context.is_connected = False


def act_columndefinition(context):
    """action routine for columndefinition"""
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
    """action routine for connected"""
    context.is_connected = True


def act_dump(context):
    """action routine for dump"""
    print('dump', len(context.data), context.data)


def act_error(context):
    """action routine for error"""
    context.error = context.packet.error_message
    raise Exception(context.error)


def act_field_length(context):
    """action routine for field length"""
    context.field_length = context.packet.field_length


def act_generic(context):
    """action routine for generic"""
    context.packet = packet.generic(context.data)
    return context.packet.type


def act_handshake(context):
    """action routine for handshake"""
    context.handshake = packet.Handshake(context.data)


def act_isolation(context):
    """action routine for isolation"""
    if context.isolation is None:
        return 'ok'
    sql = f'SET SESSION TRANSACTION ISOLATION LEVEL {context.isolation}'
    context.execute_command(COMMAND.COM_QUERY, sql)
    return None


def act_query(context, query):
    """action routine for query"""
    context.is_running = True
    context.on_query_start(query)
    context.execute_command(COMMAND.COM_QUERY, query)


def act_query_complete(context):
    """action routine for query complete"""
    context.is_running = False


def act_query_response(context):
    """action routine for query response"""
    context.packet = packet.query_response(context.data)
    return context.packet.type


def act_sequence(context, pkt, sequence):
    """action routine for sequence"""
    if sequence != context.sequence:
        raise Exception(f'Packet sequence {sequence} != {context.sequence}')
    context.increment()
    context.data = pkt


def act_tuple(context):
    """action routine for tuple"""
    if packet.is_ok(context.data) or packet.is_eof(context.data):
        context.packet = packet.generic(context.data)
        return 'done'

    pkt = packet.Packet(context.data)
    row = []
    for defn in context.column_definitions:
        value = pkt.read_length_coded_string()
        if value is not None:
            value = defn.convert(value)
        row.append(value)
    context.tuples.append(row)
    return None
