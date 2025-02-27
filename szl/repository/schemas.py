from uuid import UUID

from pydantic import BaseModel


class UserSchema(BaseModel):
    name: str
    email: str


class UserInSchema(UserSchema):
    password: str


class UserOutSchema(UserSchema):
    uuid: UUID
