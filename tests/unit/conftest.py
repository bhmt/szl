import pytest

from szl.repository.base import BaseRepository
from szl.repository.session import SessionManager, get_async_session

# def pytest_sessionstart(session):
#     BaseRepository.metadata.create_all()


@pytest.fixture(scope="session")
async def session():
    session_manager = SessionManager(
        "sqlite+aiosqlite:///file:test_db?mode=memory&cache=shared&uri=true"
    )
    async with session_manager.engine.begin() as conn:
        await conn.run_sync(BaseRepository.metadata.drop_all)
        await conn.run_sync(BaseRepository.metadata.create_all)

    return await anext(get_async_session(session_manager))
