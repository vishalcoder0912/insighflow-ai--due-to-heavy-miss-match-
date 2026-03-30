"""Declarative base metadata."""

from app.models import *  # noqa: F401,F403
from app.models.base import Base

metadata = Base.metadata
