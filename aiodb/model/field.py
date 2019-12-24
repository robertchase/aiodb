from ergaleia import import_by_path

from aiodb.model.types import String


class Field():
    def __init__(self, type=String, default=None, column=None,
                 is_nullable=False, is_primary=False, foreign=None,
                 expression=None, is_readonly=False, is_database=True):
        self.type = type
        self.parse = type.parse
        self.default = default
        self.column = column
        self.name = None
        self.is_nullable = is_nullable or is_primary
        self.is_primary = is_primary
        self._foreign = foreign
        self.is_foreign = foreign is not None
        self.expression = expression
        self.is_readonly = is_readonly or expression is not None
        self.is_database = is_database

    @property
    def foreign(self):
        if self.is_foreign and isinstance(self._foreign, str):
            self._foreign = import_by_path(self._foreign)
        return self._foreign
