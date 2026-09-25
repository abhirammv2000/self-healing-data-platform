"""Root conftest. Runs before any test module imports app code.

Every module under shared/, worker/, control_plane/ reads its config at import
time via shared/config.py's module-level os.getenv() calls, and a few (the
diagnostic agent's LLM client, the embeddings client, both DB engines) build
clients/engines at import time too. These tests mock the network/DB
boundaries, so the env values only need to be syntactically valid.

os.environ.setdefault() runs before shared/config.py's load_dotenv() call
(conftest.py is always imported first), and load_dotenv() defaults to
override=False, so these fakes win over anything in a developer's local .env.
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
