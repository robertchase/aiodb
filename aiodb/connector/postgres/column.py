from datetime import datetime
import decimal
import struct

TYPE_BOOLEAN = 16
TYPE_INT8 = 20
TYPE_INT2 = 21
TYPE_INT4 = 23
TYPE_TEXT = 25
TYPE_DATE = 1082
TYPE_TIME = 1083
TYPE_TIMESTAMP = 1114
TYPE_NUMERIC = 1700

TYPE_TIMESTAMPTZ = 1184  # 2019-05-26 16:43:26.949167+00
TYPE_DATE = 1082  # 2019-05-26
TYPE_TIMEZ = 1266  # 16:43:26.949167+00


class Descriptor:

    def __repr__(self):
        return 'column description: {}'.format(self.__dict__)

    @classmethod
    def parse(cls, payload):
        self = cls()
        name_index = payload.find(b'\x00')
        self.name = payload[:name_index].decode()
        self.table_id, self.column_id, self.data_type_id, \
            self.data_type_size, self.type_modifier, self.format_code = \
            struct.unpack_from('>IHIHIH', payload, name_index + 1)
        return payload[name_index + 1 + 18:], self

    def convert(self, value):
        if self.data_type_id == TYPE_BOOLEAN:
            return True if value == 't' else False
        if self.data_type_id in (
            TYPE_INT8, TYPE_INT4, TYPE_INT2
        ):
            return int(value)
        if self.data_type_id == TYPE_TEXT:
            return value
        if self.data_type_id == TYPE_NUMERIC:
            return decimal.Decimal(value)
        if self.data_type_id == TYPE_TIMESTAMP:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        if self.data_type_id == TYPE_TIME:
            return datetime.strptime(value, '%H:%M:%S.%f').time()
        if self.data_type_id == TYPE_DATE:
            return datetime.strptime(value, '%Y-%m-%d').date()
        return value
