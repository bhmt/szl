import uuid

from sqlalchemy import BinaryExpression, Boolean, Column, String
from sqlalchemy.dialects.postgresql import UUID as _UUID
from sqlalchemy.types import CHAR, TypeDecorator

from szl.repository.base import BaseRepository


class UUID(TypeDecorator):
    """Platform-independent GUID type.
    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.
    """

    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(_UUID())
        return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value

        if dialect.name == "postgresql":
            return str(value)

        if not isinstance(value, uuid.UUID):
            return "%.32x" % uuid.UUID(value).int

        # hexstring
        return "%.32x" % value.int

    def _uuid_value(self, value):
        if value and not isinstance(value, uuid.UUID):
            return uuid.UUID(value)

    def sort_key_function(self, value):
        return self._uuid_value(value)


class NamedItem:
    name = Column(String(50), nullable=False, unique=True)

    @classmethod
    def get_cond_list(cls, **kwargs) -> list[BinaryExpression]:
        query: str | None = kwargs.get("query")

        cond_list = []
        if query is not None:
            cond_list.append(cls.name.ilike(f"%{query.lower()}%"))
        return cond_list


class Users(BaseRepository, NamedItem):
    __tablename__ = "users"

    uuid = Column(UUID, nullable=False, default=uuid.uuid4().hex, unique=True)
    email = Column(String(320), nullable=False, unique=True)
    password = Column(String(128), nullable=False)
    superuser = Column(Boolean, nullable=False, default=False)
    deleted = Column(Boolean, nullable=False, default=False)
