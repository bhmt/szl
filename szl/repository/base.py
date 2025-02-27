from collections.abc import Sequence
from typing import Any, Literal, NamedTuple, Self

from pydantic import BaseModel
from sqlalchemy import (
    BinaryExpression,
    Column,
    Enum,
    Row,
    Select,
    UnaryExpression,
    select,
)
from sqlalchemy.engine.result import Result
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.strategy_options import Load
from sqlalchemy.sql import func
from sqlalchemy.sql.elements import ColumnElement


class FilterStatement(NamedTuple):
    offset: int | None = None
    limit: int | None = None
    order_by: str | None = "id"
    order_by_direction: Literal["asc", "desc"] = "asc"
    extra: dict | None = None


class BaseRepository(DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True)

    order_by_direction: Literal["asc", "desc"] = "asc"
    negation: str = "!"

    @classmethod
    def get_options(cls) -> list[Load]:
        return []

    @classmethod
    def get_cond_list(cls, **kwargs) -> list[BinaryExpression]:
        return []

    @classmethod
    def get_binary_cond(
        cls, field: Column[str | Enum], value: str, /
    ) -> BinaryExpression:
        if value.startswith(cls.negation):
            return field != value[1:]

        return field == value

    @classmethod
    def get_order_by(
        cls,
        *,
        field_name: str | None = None,
        direction: Literal["asc", "desc"] = "asc",
    ) -> UnaryExpression:
        field: InstrumentedAttribute
        if field_name is None:
            field = cls.id
        else:
            field = cls.__table__.c[field_name.lower()]

        if direction == "desc":
            return field.desc()
        return field.asc()

    @classmethod
    def get_filter_statement(
        cls,
        filter_statement: FilterStatement,
        /,
    ) -> Select[tuple[Self]]:
        statement: Select[tuple[BaseRepository]] = select(cls)
        statement = statement.order_by(
            cls.get_order_by(
                field_name=filter_statement.order_by,
                direction=filter_statement.order_by_direction,
            )
        )

        cond_list: list = []
        if filter_statement.extra is not None:
            cond_list = cls.get_cond_list(**filter_statement.extra)
        if cond_list:
            statement = statement.where(*cond_list)
        options: list[Load] = cls.get_options()
        if options:
            statement = statement.options(*options)
        if filter_statement.offset is not None:
            statement = statement.offset(filter_statement.offset)
        if filter_statement.limit is not None:
            statement = statement.limit(filter_statement.limit)
        return statement

    @classmethod
    async def create(
        cls,
        session: AsyncSession,
        data: BaseModel,
        /,
        *,
        extra_fields: dict[
            str,
            Any,
        ]
        | None = None,
    ) -> Self:
        obj: Self = cls(**data.model_dump(by_alias=False))
        if extra_fields is not None:
            for name, value in extra_fields.items():
                setattr(obj, name, value)

        session.add(obj)
        await session.commit()
        await session.refresh(obj)
        return obj

    @classmethod
    async def read(
        cls,
        session: AsyncSession,
        val: Any,
        /,
        *,
        field: InstrumentedAttribute | None = None,
        extra_where: list[ColumnElement[bool]] | None = None,
    ) -> Self | None:
        options: list[Load] = cls.get_options()
        if field is None:
            field = cls.id

        where_cond: list = [field == val]
        if extra_where is not None:
            where_cond.extend(extra_where)

        statement: Select = select(cls).where(*where_cond)
        if options:
            statement = statement.options(*options)

        cursor: Result = await session.execute(statement)
        try:
            return cursor.scalars().one()
        except NoResultFound:
            return None

    @classmethod
    async def update(
        cls,
        session: AsyncSession,
        id_: int,
        data: dict,
        /,
    ) -> Self | None:
        obj: Self = await cls.read(session, id_)
        if not obj:
            return None

        for field, value in data.items():
            if value is not None:
                setattr(obj, field, value)

        await session.commit()
        await session.refresh(obj)
        return obj

    @classmethod
    async def delete(
        cls,
        session: AsyncSession,
        id_: int,
        /,
    ) -> Self | None:
        obj: Self = await cls.read(session, id_)
        if not obj:
            return None

        await session.delete(obj)
        await session.commit()
        # await session.refresh(obj)
        return obj

    @classmethod
    async def count(cls, session: AsyncSession, /) -> int:
        cursor: Result = await session.execute(func.count(cls.id))
        return cursor.scalar()

    @classmethod
    async def filter(
        cls,
        session: AsyncSession,
        filter_statement: FilterStatement,
        /,
    ) -> Sequence[Row[tuple[Any, ...] | Any]]:
        statement = cls.get_filter_statement(filter_statement)
        cursor: Result = await session.execute(statement)
        return cursor.scalars().all()
