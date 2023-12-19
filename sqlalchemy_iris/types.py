import datetime
from decimal import Decimal
from sqlalchemy.sql import sqltypes
from uuid import UUID as _python_UUID

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
            if isinstance(value, str) and "-" in value[1:]:
                return datetime.datetime.strptime(value, "%Y-%m-%d").date()
            horolog = int(value) + HOROLOG_ORDINAL
            return datetime.date.fromordinal(horolog)

        return process


class IRISTimeStamp(sqltypes.DateTime):
    def bind_processor(self, dialect):
        def process(value: datetime.datetime):
            if value is not None:
                # value = int(value.timestamp() * 1000000)
                # value += (2 ** 60) if value > 0 else -(2 ** 61 * 3)
                return value.strftime("%Y-%m-%d %H:%M:%S.%f")
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                if "." not in value:
                    value += ".0"
                return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
            if isinstance(value, int):
                value -= (2**60) if value > 0 else -(2**61 * 3)
                value = value / 1000000
                value = datetime.datetime.utcfromtimestamp(value)
            return value

        return process


class IRISDateTime(sqltypes.DateTime):
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.strftime("%Y-%m-%d %H:%M:%S.%f")
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                if "." not in value:
                    value += ".0"
                return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
            return value

        return process


class IRISTime(sqltypes.DateTime):
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.strftime("%H:%M:%S.%f")
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                if "." not in value:
                    value += ".0"
                return datetime.datetime.strptime(value, "%H:%M:%S.%f").time()
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


class IRISUniqueIdentifier(sqltypes.Uuid):
    def literal_processor(self, dialect):
        if not self.as_uuid:

            def process(value):
                return f"""'{value.replace("'", "''")}'"""

            return process
        else:

            def process(value):
                return f"""'{str(value).replace("'", "''")}'"""

            return process

    def bind_processor(self, dialect):
        character_based_uuid = not dialect.supports_native_uuid or not self.native_uuid

        if character_based_uuid:
            if self.as_uuid:

                def process(value):
                    if value is not None:
                        value = str(value)
                    return value

                return process
            else:

                def process(value):
                    return value

                return process
        else:
            return None

    def result_processor(self, dialect, coltype):
        character_based_uuid = not dialect.supports_native_uuid or not self.native_uuid

        if character_based_uuid:
            if self.as_uuid:

                def process(value):
                    if value and not isinstance(value, _python_UUID):
                        value = _python_UUID(value)
                    return value

                return process
            else:

                def process(value):
                    if value and isinstance(value, _python_UUID):
                        value = str(value)
                    return value

                return process
        else:
            if not self.as_uuid:

                def process(value):
                    if value and isinstance(value, _python_UUID):
                        value = str(value)
                    return value

                return process
            else:
                return None


class BIT(sqltypes.TypeEngine):
    __visit_name__ = "BIT"


class TINYINT(sqltypes.Integer):
    __visit_name__ = "TINYINT"


class DOUBLE(sqltypes.Float):
    __visit_name__ = "DOUBLE"


class LONGVARCHAR(sqltypes.VARCHAR):
    __visit_name__ = "LONGVARCHAR"


class LONGVARBINARY(sqltypes.VARBINARY):
    __visit_name__ = "LONGVARBINARY"
