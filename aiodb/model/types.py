"""manage generic database types"""
import datetime


def CHAR(length, is_strict=False):  # pylint: disable=invalid-name
    """represent a char string"""

    class _char:  # pylint: disable=too-few-public-methods

        @classmethod
        def parse(cls, value):
            """normalize a char"""
            value = String.parse(value)
            if is_strict and len(value) > length:
                raise ValueError('value is too long for field')
            return (value + ' ' * length)[:length]

    return _char


class Binary:  # pylint: disable=too-few-public-methods
    """represent a binary (not boolean) value"""

    @classmethod
    def parse(cls, value):
        """normalize a binary value"""
        return value


class String:  # pylint: disable=too-few-public-methods
    """represent a string"""

    @classmethod
    def parse(cls, value):
        """normalize a string"""
        if value is None:
            raise ValueError('None is not a string value')
        return str(value)


class Integer:  # pylint: disable=too-few-public-methods
    """represent an integer"""

    @classmethod
    def parse(cls, value):
        """normalize an integer"""
        if value is None:
            raise ValueError('None is not an integer value')
        if int(value) != float(value):
            raise ValueError(f"'{value}' is not an integer")
        return int(value)


class Boolean:  # pylint: disable=too-few-public-methods
    """represent a boolean"""

    @classmethod
    def parse(cls, value):
        """normalize a boolean value"""
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


class Date:  # pylint: disable=too-few-public-methods
    """represent a date"""

    @classmethod
    def parse(cls, value):
        """normalize a date"""
        if value is None:
            raise ValueError('None is not a date value')
        # a date is not a datetime, but a datetime is a date (order matters)
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()


def to_datetime(value):
    """cast string value to datetime.datetime"""
    try:
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        pass
    return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')


class Datetime:  # pylint: disable=too-few-public-methods
    """represent a datetime"""

    @classmethod
    def parse(cls, value):
        """normalize datetime value"""
        if value is None:
            raise ValueError('None is not a datetime value')
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        return to_datetime(value)


def to_time(value):
    """cast time string to datetime.timedelta"""
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


class Time:  # pylint: disable=too-few-public-methods
    """represent a time interval"""

    @classmethod
    def parse(cls, value):
        """normalize time value"""
        if value is None:
            raise ValueError('None is not a time value')
        if isinstance(value, datetime.time):
            return value
        if isinstance(value, datetime.timedelta):
            return value
        return to_time(value)
