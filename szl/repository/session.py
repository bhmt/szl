from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class SessionManager:
    def __init__(self, uri: str, /, *, kwargs: dict[str, Any] | None = None) -> None:
        if kwargs is None:
            kwargs = {}

        self.engine: AsyncEngine | None = create_async_engine(uri, **kwargs)
        self._session_maker: async_sessionmaker[AsyncSession] | None = (
            async_sessionmaker(
                autocommit=False,
                bind=self.engine,
                expire_on_commit=False,
            )
        )

    async def close(self) -> None:
        if self.engine is None:
            raise Exception("SessionManager is not initialized")

        await self.engine.dispose()

        self.engine = None
        self._session_maker = None

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        if self.engine is None:
            raise RuntimeError("SessionManager is not initialized")

        async with self.engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._session_maker is None:
            raise Exception("SessionManager is not initialized")

        session = self._session_maker()
        try:
            yield session
        except:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_async_session(session_manager: SessionManager):
    async with session_manager.session() as session:
        yield session
