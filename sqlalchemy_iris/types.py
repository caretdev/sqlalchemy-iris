import datetime
from decimal import Decimal
from sqlalchemy import func, text
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import UserDefinedType
from uuid import UUID as _python_UUID
from intersystems_iris import IRISList
from sqlalchemy import __version__ as sqlalchemy_version

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
            if isinstance(value, datetime.date):
                return value
            if isinstance(value, str) and "-" in value[1:]:
                return datetime.datetime.strptime(value, "%Y-%m-%d").date()
            horolog = int(value) + HOROLOG_ORDINAL
            return datetime.date.fromordinal(horolog)

        return process


class IRISTimeStamp(sqltypes.DateTime):
    __visit_name__ = "timestamp"

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
    __visit_name__ = "datetime"

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.strftime("%Y-%m-%d %H:%M:%S.%f")
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, datetime.datetime):
                return value
            if isinstance(value, str):
                if "." not in value:
                    value += ".0"
                return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
            return value

        return process


class IRISTime(sqltypes.DateTime):
    __visit_name__ = "time"

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return value.strftime("%H:%M:%S.%f")
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, datetime.time):
                return value
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


if sqlalchemy_version.startswith("2."):

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
            character_based_uuid = (
                not dialect.supports_native_uuid or not self.native_uuid
            )

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
            character_based_uuid = (
                not dialect.supports_native_uuid or not self.native_uuid
            )

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


class IRISListBuild(UserDefinedType):
    cache_ok = True

    def __init__(self, max_items: int = None, item_type: type = float):
        super(UserDefinedType, self).__init__()
        self.max_items = max_items
        max_length = None
        if type is float or type is int:
            max_length = max_items * 10
        elif max_items:
            max_length = 65535
        self.max_length = max_length

    def get_col_spec(self, **kw):
        if self.max_length is None:
            return "VARBINARY(65535)"
        return "VARBINARY(%d)" % self.max_length

    def bind_processor(self, dialect):
        def process(value):
            irislist = IRISList()
            if not value:
                return value
            if not isinstance(value, list) and not isinstance(value, tuple):
                raise ValueError("expected list or tuple, got '%s'" % type(value))
            for item in value:
                irislist.add(item)
            return irislist.getBuffer()

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value:
                irislist = IRISList(value)
                return irislist._list_data
            return value

        return process

    class comparator_factory(UserDefinedType.Comparator):
        def func(self, funcname: str, other):
            if not isinstance(other, list) and not isinstance(other, tuple):
                raise ValueError("expected list or tuple, got '%s'" % type(other))
            irislist = IRISList()
            for item in other:
                irislist.add(item)
            return getattr(func, funcname)(self, irislist.getBuffer())


class IRISVector(UserDefinedType):
    cache_ok = True

    def __init__(self, max_items: int = None, item_type: type = float):
        super(UserDefinedType, self).__init__()
        if item_type not in [float, int, Decimal]:
            raise TypeError(
                f"IRISVector expected int, float or Decimal; got {type.__name__}; expected: int, float, Decimal"
            )
        self.max_items = max_items
        self.item_type = item_type
        item_type_server = (
            "decimal"
            if self.item_type is float
            else "float" if self.item_type is Decimal else "int"
        )
        self.item_type_server = item_type_server

    def get_col_spec(self, **kw):
        if self.max_items is None and self.item_type is None:
            return "VECTOR"
        len = str(self.max_items or "")
        return f"VECTOR({self.item_type_server}, {len})"

    def bind_processor(self, dialect):
        def process(value):
            if not value:
                return value
            if not isinstance(value, list) and not isinstance(value, tuple):
                raise ValueError("expected list or tuple, got '%s'" % type(value))
            return f"[{','.join([str(v) for v in value])}]"

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if not value:
                return value
            vals = value.split(",")
            vals = [self.item_type(v) for v in vals]
            return vals

        return process

    class comparator_factory(UserDefinedType.Comparator):
        # def l2_distance(self, other):
        #     return self.func('vector_l2', other)

        def max_inner_product(self, other):
            return self.func("vector_dot_product", other)

        def cosine_distance(self, other):
            return self.func("vector_cosine", other)

        def cosine(self, other):
            return 1 - self.func("vector_cosine", other)

        def func(self, funcname: str, other):
            if not isinstance(other, list) and not isinstance(other, tuple):
                raise ValueError("expected list or tuple, got '%s'" % type(other))
            othervalue = f"[{','.join([str(v) for v in other])}]"
            return getattr(func, funcname)(
                self, func.to_vector(othervalue, text(self.type.item_type_server))
            )


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


class LISTBUILD(sqltypes.VARBINARY):
    __visit_name__ = "VARCHAR"
