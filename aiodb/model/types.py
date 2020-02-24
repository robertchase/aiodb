import datetime


def CHAR(length, is_strict=False):

    class _char:

        @classmethod
        def parse(cls, value):
            value = String.parse(value)
            if is_strict and len(value) > length:
                raise ValueError('value is too long for field')
            return (value + ' ' * length)[:length]

    return _char


class Binary:

    @classmethod
    def parse(cls, value):
        return value


class String:

    @classmethod
    def parse(cls, value):
        if value is None:
            raise ValueError('None is not a string value')
        return str(value)


class Integer:

    @classmethod
    def parse(cls, value):
        if value is None:
            raise ValueError('None is not an integer value')
        if int(value) != float(value):
            raise ValueError(f"'{value}' is not an integer")
        return int(value)


class Boolean:

    @classmethod
    def parse(cls, value):
        if value is None:
            raise ValueError('None is not a boolean value')
        if value in (True, 1):
            return 1
        if value in (False, 0):
            return 0
        if str(value).upper() in ('TRUE', 'T'):
            return 1
        if str(value).upper() in ('FALSE', 'F'):
            return 0
        raise ValueError(f"'{value}' is not a boolean")


class Date:

    @classmethod
    def parse(cls, value):
        if value is None:
            raise ValueError('None is not a date value')
        if type(value) == datetime.date:
            return value
        if type(value) == datetime.datetime:
            return value.date()
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()


class Datetime:

    @classmethod
    def parse(cls, value):
        if value is None:
            raise ValueError('None is not a datetime value')
        if type(value) == datetime.datetime:
            return value
        if type(value) == datetime.date:
            return datetime.datetime(value.year, value.month, value.day)
        try:
            return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            pass
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')


class Time:

    @classmethod
    def parse(cls, value):
        if value is None:
            raise ValueError('None is not a time value')
        if type(value) == datetime.time:
            return value
        if type(value) == datetime.timedelta:
            return value
        value = str(value)
        try:
            time, partial = value.split('.')
            microseconds = int((partial + '0000000')[:6])
        except ValueError:
            time = value
            microseconds = 0
        hours, minutes, seconds = (int(t) for t in time.split(':'))
        if hours <= 24:
            return datetime.time(hours, minutes, seconds, microseconds)
        return datetime.timedelta(hours=hours, minutes=minutes,
                                  seconds=seconds, microseconds=microseconds)
