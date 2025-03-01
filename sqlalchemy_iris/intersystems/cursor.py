from typing import Any
from sqlalchemy import CursorResult
from sqlalchemy.engine.cursor import CursorFetchStrategy
from sqlalchemy.engine.interfaces import DBAPICursor


class InterSystemsCursorFetchStrategy(CursorFetchStrategy):

    def fetchone(
        self,
        result: CursorResult[Any],
        dbapi_cursor: DBAPICursor,
        hard_close: bool = False,
    ) -> Any:
        row = dbapi_cursor.fetchone()
        return tuple(row) if row else None

