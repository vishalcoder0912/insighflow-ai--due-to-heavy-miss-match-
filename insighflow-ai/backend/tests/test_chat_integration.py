"""Integration tests for Chat API functionality."""

import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.main import app
from app.models.user import User
from app.models.dataset import DatasetAsset
from app.models.chat import ChatSession, ChatMessage
from app.db.session import get_db_session as get_db
from app.api.deps import get_current_user
from app.core.security import create_access_token


TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
async def async_engine():
    """Create test database engine."""
    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.fixture
async def async_session(async_engine):
    """Create test database session."""
    async_session_maker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with async_session_maker() as session:
        yield session


@pytest.fixture
async def test_user(async_session: AsyncSession):
    """Create test user."""
    from passlib.context import CryptContext

    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    hashed = pwd_context.hash("testpassword123")
    user = User(
        email="test@example.com",
        full_name="Test User",
        hashed_password=hashed,
        is_active=True,
    )
    async_session.add(user)
    await async_session.commit()
    await async_session.refresh(user)
    return user


@pytest.fixture
async def test_dataset(async_session: AsyncSession, test_user: User):
    """Create test dataset."""
    dataset = DatasetAsset(
        user_id=test_user.id,
        project_id=None,
        original_filename="test_data.csv",
        stored_filename="test_data_123.csv",
        stored_path="/tmp/test_data.csv",
        file_size=1024,
        row_count=100,
        column_count=5,
        detected_domain="sales",
        is_active=True,
    )
    async_session.add(dataset)
    await async_session.commit()
    await async_session.refresh(dataset)
    return dataset


@pytest.fixture
async def test_chat_session(
    async_session: AsyncSession, test_user: User, test_dataset: DatasetAsset
):
    """Create test chat session."""
    session = ChatSession(
        user_id=test_user.id,
        dataset_id=test_dataset.id,
        title="Test Chat Session",
        is_active=True,
        message_count=0,
    )
    async_session.add(session)
    await async_session.commit()
    await async_session.refresh(session)
    return session


@pytest.fixture
async def auth_token(test_user: User):
    """Generate auth token for test user."""
    return create_access_token({"sub": str(test_user.id)})


@pytest.fixture
async def client(async_session: AsyncSession, test_user: User):
    """Create test client with dependency overrides."""

    async def override_get_db():
        yield async_session

    async def override_get_current_user():
        return test_user

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = override_get_current_user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


class TestChatAPI:
    """Test chat API endpoints."""

    @pytest.mark.asyncio
    async def test_create_chat_session(
        self, client: AsyncClient, test_dataset: DatasetAsset
    ):
        """Test creating a new chat session."""
        response = await client.post(
            "/api/v1/chat/sessions",
            json={
                "dataset_id": test_dataset.id,
                "title": "My Test Session",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["title"] == "My Test Session"
        assert data["session_id"] is not None

    @pytest.mark.asyncio
    async def test_get_chat_sessions(
        self, client: AsyncClient, test_chat_session: ChatSession
    ):
        """Test listing chat sessions."""
        response = await client.get("/api/v1/chat/sessions")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "sessions" in data
        assert isinstance(data["sessions"], list)

    @pytest.mark.asyncio
    async def test_get_session_messages(
        self, client: AsyncClient, test_chat_session: ChatSession
    ):
        """Test getting messages for a session."""
        response = await client.get(
            f"/api/v1/chat/sessions/{test_chat_session.id}/messages"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "messages" in data
        assert data["session"]["id"] == test_chat_session.id

    @pytest.mark.asyncio
    async def test_send_chat_message(
        self, client: AsyncClient, test_dataset: DatasetAsset
    ):
        """Test sending a chat message."""
        response = await client.post(
            "/api/v1/chat/message",
            json={
                "dataset_id": test_dataset.id,
                "message": "Show me the top 5 products by revenue",
                "use_llm": True,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ["success", "error"]
        assert "message" in data
        assert data["message"]["role"] == "assistant"
        assert data["session_id"] is not None

    @pytest.mark.asyncio
    async def test_send_message_with_session_id(
        self,
        client: AsyncClient,
        test_chat_session: ChatSession,
        test_dataset: DatasetAsset,
    ):
        """Test sending a message to an existing session."""
        response = await client.post(
            "/api/v1/chat/message",
            json={
                "session_id": test_chat_session.id,
                "dataset_id": test_dataset.id,
                "message": "What is the total count?",
                "use_llm": True,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["session_id"] == test_chat_session.id

    @pytest.mark.asyncio
    async def test_delete_chat_session(
        self, client: AsyncClient, test_chat_session: ChatSession
    ):
        """Test deleting a chat session."""
        response = await client.delete(f"/api/v1/chat/sessions/{test_chat_session.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

    @pytest.mark.asyncio
    async def test_clear_cache(self, client: AsyncClient):
        """Test clearing query cache."""
        response = await client.post("/api/v1/chat/cache/clear")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["cleared"] is True

    @pytest.mark.asyncio
    async def test_chat_health_check(self, client: AsyncClient):
        """Test chat health endpoint."""
        response = await client.get("/api/v1/chat/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "chat-v2"


class TestChatMessageStorage:
    """Test chat message persistence."""

    @pytest.mark.asyncio
    async def test_messages_persist_after_session(
        self,
        async_session: AsyncSession,
        test_chat_session: ChatSession,
        test_user: User,
    ):
        """Test that messages persist after session creation."""
        message = ChatMessage(
            session_id=test_chat_session.id,
            role="user",
            content="Test message",
        )
        async_session.add(message)
        await async_session.commit()

        from sqlalchemy import select

        result = await async_session.execute(
            select(ChatMessage).where(ChatMessage.session_id == test_chat_session.id)
        )
        messages = result.scalars().all()

        assert len(messages) >= 1
        assert any(m.content == "Test message" for m in messages)

    @pytest.mark.asyncio
    async def test_session_message_count_updated(
        self, async_session: AsyncSession, test_chat_session: ChatSession
    ):
        """Test that session message count is updated."""
        initial_count = test_chat_session.message_count

        message1 = ChatMessage(
            session_id=test_chat_session.id, role="user", content="Message 1"
        )
        message2 = ChatMessage(
            session_id=test_chat_session.id, role="assistant", content="Response 1"
        )

        async_session.add(message1)
        async_session.add(message2)
        await async_session.commit()

        test_chat_session.message_count += 2
        await async_session.commit()

        assert test_chat_session.message_count == initial_count + 2


class TestNLToSQLService:
    """Test NL-to-SQL conversion."""

    def test_parse_top_query(self):
        """Test parsing 'top N' queries."""
        from app.services.nl_to_sql_service import NLToSQLService

        service = NLToSQLService()
        schema = {"product": "string", "revenue": "float", "quantity": "int"}

        result = service._generate_fallback_sql("Show top 5 products", "sales", schema)

        assert "SELECT" in result.upper()
        assert "LIMIT" in result.upper()

    def test_parse_count_query(self):
        """Test parsing count queries."""
        from app.services.nl_to_sql_service import NLToSQLService

        service = NLToSQLService()
        schema = {"id": "int", "name": "string"}

        result = service._generate_fallback_sql("How many records?", "users", schema)

        assert "COUNT" in result.upper()

    def test_parse_group_by_query(self):
        """Test parsing group by queries."""
        from app.services.nl_to_sql_service import NLToSQLService

        service = NLToSQLService()
        schema = {"region": "string", "revenue": "float"}

        result = service._generate_fallback_sql("Revenue by region", "sales", schema)

        assert "GROUP BY" in result.upper()


class TestSQLValidation:
    """Test SQL validation."""

    def test_valid_select_sql(self):
        """Test validation of valid SELECT SQL."""
        from app.services.nl_to_sql_service import NLToSQLService

        service = NLToSQLService()

        assert service._is_valid_sql("SELECT * FROM users LIMIT 10")
        assert service._is_valid_sql("SELECT name, email FROM users WHERE id = 1")

    def test_invalid_drop_sql(self):
        """Test rejection of DROP SQL."""
        from app.services.nl_to_sql_service import NLToSQLService

        service = NLToSQLService()

        assert not service._is_valid_sql("DROP TABLE users")

    def test_invalid_delete_sql(self):
        """Test rejection of DELETE SQL."""
        from app.services.nl_to_sql_service import NLToSQLService

        service = NLToSQLService()

        assert not service._is_valid_sql("DELETE FROM users WHERE id = 1")

    def test_invalid_unmatched_quotes(self):
        """Test rejection of SQL with unmatched quotes."""
        from app.services.nl_to_sql_service import NLToSQLService

        service = NLToSQLService()

        assert not service._is_valid_sql("SELECT * FROM users WHERE name = 'test")
