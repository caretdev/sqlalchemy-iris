import datetime
from decimal import Decimal
from sqlalchemy.sql import sqltypes

HOROLOG_ORDINAL = datetime.date(1840, 12, 31).toordinal()


class IRISBoolean(sqltypes.Boolean):
    def _should_create_constraint(self, compiler, **kw):
        return False

    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, int):
                return 1 if value > 0 else 0
            elif isinstance(value, bool):
                return 1 if value is True else 0
            return None
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, int):
                return value > 0
            return value
        return process


class IRISDate(sqltypes.Date):
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            horolog = value.toordinal() - HOROLOG_ORDINAL
            return str(horolog)

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, str) and '-' in value[1:]:
                return datetime.datetime.strptime(value, '%Y-%m-%d').date()
            horolog = int(value) + HOROLOG_ORDINAL
            return datetime.date.fromordinal(horolog)

        return process


class IRISTimeStamp(sqltypes.DateTime):
    def bind_processor(self, dialect):
        def process(value: datetime.datetime):
            if value is not None:
                # value = int(value.timestamp() * 1000000)
                # value += (2 ** 60) if value > 0 else -(2 ** 61 * 3)
                return value.strftime('%Y-%m-%d %H:%M:%S.%f')
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                if '.' not in value:
                    value += '.0'
                return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
            if isinstance(value, int):
                value -= (2 ** 60) if value > 0 else -(2 ** 61 * 3)
                value = value / 1000000
                value = datetime.datetime.utcfromtimestamp(value)
            return value

        return process


class IRISDateTime(sqltypes.DateTime):
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.strftime('%Y-%m-%d %H:%M:%S.%f')
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                if '.' not in value:
                    value += '.0'
                return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
            return value

        return process


class IRISTime(sqltypes.DateTime):
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.strftime('%H:%M:%S.%f')
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                if '.' not in value:
                    value += '.0'
                return datetime.datetime.strptime(value, '%H:%M:%S.%f').time()
            if isinstance(value, int) or isinstance(value, Decimal):
                horolog = value
                hour = int(horolog // 3600)
                horolog -= int(hour * 3600)
                minute = int(horolog // 60)
                second = int(horolog % 60)
                micro = int(value % 1 * 1000000)
                return datetime.time(hour, minute, second, micro)
            return value

        return process
