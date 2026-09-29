from shared.db import Base
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from sqlalchemy import ForeignKey, String, Text, JSON, func
from pgvector.sqlalchemy import Vector

class IncidentEmbedding(Base):
    __tablename__="incident_embeddings"

    id: Mapped[int]=mapped_column(primary_key=True)

    #polymorphic discriminator. Tells us which kind of source produced this embedding.
    #the allowed values are 'past_run', 'past_recommendation', 'runbook', enforced by the Pydantic schema layer (when we add one)
    #and by the code that writes these rows. We deliberately do NOT use a DB CHECK constraint here because the set of source types may grow (e.g., 'anomaly_event' later)
    #and we don't want a migration every time we add one.
    source_type: Mapped[str]=mapped_column(String(50), nullable=False, index=True)

    #FK-like reference back to the source row, but NOT a real FK because the target table varies by source_type (polymorphic association).
    #NULL for runbooks (no DB row exists for them, they live as markdown files on disk).
    #We accept the integrity tradeoff: if a past_run gets deleted, its embedding row becomes orphaned. We'll handle that via the tenant cascade below for the common case
    #(tenant deletion sweeps everything), and accept some staleness otherwise. The retrieval layer doesn't care, it reads chunk_text and source_ref, not the live source row.
    source_id: Mapped[int|None]=mapped_column(nullable=True)

    #human-readable citation handle. Used by the Recovery Planning agent to cite its sources in the explanation field.
    #for runbooks: "network.md#examples" or similar, filename plus section heading.
    #for incidents (past_run, past_recommendation): "run_id=42" or "rec_id=17", concrete identifier the operator can look up.
    #we keep this as a string column rather than reconstructing it from source_type + source_id at query time because runbook chunks need a string that doesn't map cleanly to an integer id.
    source_ref: Mapped[str|None]=mapped_column(String(500), nullable=True)

    #the exact text that was embedded. We keep this in the row so retrieval can hand it back to the agent verbatim, embeddings themselves aren't reversible into text.
    #Text (unbounded) rather than String(N) because runbook chunks can vary in length and we don't want to truncate.
    chunk_text: Mapped[str]=mapped_column(Text, nullable=False)

    #the embedding vector itself. 768 dimensions for Gemini's text-embedding-004, must match the dim used at indexing time and at query time, otherwise pgvector throws.
    #if we ever swap embedding models, this column dimension changes and the whole table has to be re-indexed. That's a known tradeoff with embedding storage.
    embedding: Mapped[list[float]]=mapped_column(Vector(768), nullable=False)

    #tenant scoping. NULL for runbooks (runbooks are global operational knowledge, every tenant sees them).
    #Populated for past_run and past_recommendation rows so retrieval can filter by tenant and prevent cross-tenant data leaks.
    #ondelete=CASCADE matches the pattern across the rest of the schema, when a tenant is deleted, their embedded incidents go with them. Runbook rows (tenant_id=NULL) are unaffected.
    tenant_id: Mapped[int|None]=mapped_column(ForeignKey("tenants.id", ondelete="CASCADE"), nullable=True, index=True)

    #flexible metadata bag. JSON (not JSONB) to match the rest of the codebase. Stores things like:
    #  - for past_run: {"pipeline_id": int, "error_type": str, "failed_step": str}
    #  - for past_recommendation: {"failure_classification": str, "recommended_action": str, "status": str}
    #  - for runbook: {"section": str, "category": str}
    #the agent can use these fields downstream for filtering or display without re-joining to the source table.
    meta: Mapped[dict|None]=mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime]=mapped_column(server_default=func.now())

    #NOTE: no back-relationships to Tenant, PipelineRun, or AgentRecommendation. The polymorphic source_id makes ORM traversal awkward,
    #and retrieval queries always go through the embedding table directly with explicit joins where needed. Deliberate departure from the pattern in other tables.