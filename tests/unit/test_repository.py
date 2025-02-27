import pytest

from szl.repository.base import FilterStatement
from szl.repository.models import Users
from szl.repository.schemas import UserInSchema

pytest_plugins = ("pytest_asyncio",)

TEST_NAME = "szl7000"
TEST_EMAIL = f"{TEST_NAME}@szl.com"
TEST_PASSWORD = "pass"
TEST_SCHEMA = UserInSchema(name=TEST_NAME, email=TEST_EMAIL, password=TEST_PASSWORD)
TEST_REPO = Users()
TEST_FILTER_STATEMENT = FilterStatement(limit=1)

TEST_USER_CREATED = None


async def _create(session):
    global TEST_USER_CREATED
    TEST_USER_CREATED = await TEST_REPO.create(session, TEST_SCHEMA)
    assert TEST_USER_CREATED


async def _read(session):
    user_read = await TEST_REPO.read(session, TEST_EMAIL, field=Users.email)
    assert user_read == TEST_USER_CREATED


async def _filter(session):
    user_filter = await TEST_REPO.filter(session, TEST_FILTER_STATEMENT)
    assert len(user_filter) == 1
    assert user_filter[0] == TEST_USER_CREATED


async def _update(session):
    global TEST_USER_CREATED
    TEST_USER_CREATED = await TEST_REPO.update(
        session, TEST_USER_CREATED.id, {"name": "updated"}
    )
    assert TEST_USER_CREATED.name == "updated"


async def _updated(session):
    user_updated = await TEST_REPO.read(session, TEST_EMAIL, field=Users.email)
    assert user_updated == TEST_USER_CREATED


async def _delete(session):
    user_delete = await TEST_REPO.delete(session, TEST_USER_CREATED.id)
    assert user_delete == TEST_USER_CREATED


async def _not_found(session):
    user_deleted = await TEST_REPO.read(session, TEST_EMAIL, field=Users.email)
    assert user_deleted is None


@pytest.mark.parametrize(
    "fn_ordered", [_create, _read, _filter, _update, _updated, _delete, _not_found]
)
async def test_repository(session, fn_ordered):
    await fn_ordered(session)
