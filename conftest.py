"""Root conftest. It runs before any test imports app code.

Modules under shared/, worker/ and control_plane/ read their config when they are
imported (shared/config.py calls os.getenv at module level). Some also build an LLM
client, an embeddings client or a DB engine at import time. The tests mock the network
and the database, so the env values only need to look valid.

os.environ.setdefault() runs before load_dotenv() in shared/config.py, and load_dotenv()
does not override existing values. So these fake values win over a developer's local .env.
"""
import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test")
os.environ.setdefault("DATABASE_URL_SYNC", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("DATA_WAREHOUSE_URL", "postgresql://test:test@localhost:5432/test_dw")
os.environ.setdefault("API_KEY_SECRET", "test-secret")
os.environ.setdefault("ADMIN_SECRET_KEY", "test-admin-secret")
os.environ.setdefault("GOOGLE_API_KEY", "test-fake-key-not-real")
os.environ.setdefault("GEMINI_MODEL", "gemini-2.5-flash")
os.environ.setdefault("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001")
