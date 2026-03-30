"""Domain enumerations."""

from enum import Enum


class UserRole(str, Enum):
    """Application roles."""

    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"


class ProjectPermission(str, Enum):
    """Project-level permissions."""

    VIEWER = "viewer"
    EDITOR = "editor"


class ProjectStatus(str, Enum):
    """Project lifecycle."""

    ACTIVE = "active"
    ARCHIVED = "archived"


class InsightStatus(str, Enum):
    """Insight publication states."""

    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class InsightSeverity(str, Enum):
    """Insight severity scale."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
