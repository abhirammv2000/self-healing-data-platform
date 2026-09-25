from shared.db import Base
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from sqlalchemy import ForeignKey, String, Text, Integer, UniqueConstraint, func

class PipelineRun(Base):
    __tablename__="pipeline_runs"
    # A plain UNIQUE(pipeline_id, idempotency_key) is enough on its own. Postgres treats every
    # NULL as distinct, so the majority of runs (no key supplied) never collide, and only two
    # runs on the same pipeline with the same key do. No partial index needed to exclude NULLs.
    __table_args__=(UniqueConstraint("pipeline_id", "idempotency_key", name="uq_pipeline_runs_pipeline_id_idempotency_key"),)

    id: Mapped[int]=mapped_column(primary_key=True)
    tenant_id: Mapped[int]=mapped_column(ForeignKey("tenants.id", ondelete="CASCADE"), index=True)
    pipeline_id: Mapped[int]=mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), index=True)
    status: Mapped[str]=mapped_column(String(50), default="queued", nullable=False)
    created_at: Mapped[datetime]=mapped_column(server_default=func.now())
    started_at: Mapped[datetime|None]=mapped_column(nullable=True)
    updated_at: Mapped[datetime|None]=mapped_column(nullable=True)
    ended_at: Mapped[datetime|None]=mapped_column(nullable=True)
    error_type: Mapped[str|None]=mapped_column(String(50), nullable=True)
    error_message: Mapped[str|None]=mapped_column(Text,nullable=True)
    retry_count: Mapped[int]=mapped_column(Integer,default=0, nullable=False)
    # Client-supplied via the Idempotency-Key header on POST. A duplicate submission for the
    # same pipeline and key returns the existing run instead of creating a second one.
    idempotency_key: Mapped[str|None]=mapped_column(String(255), nullable=True)


    pipeline=relationship("Pipeline", back_populates="pipeline_runs")
    tenant=relationship("Tenant", back_populates="pipeline_runs")
    agent_recommendations=relationship("AgentRecommendation",back_populates="pipeline_run", cascade="all, delete-orphan")
    webhook_callbacks=relationship("WebhookCallback",back_populates="pipeline_run", cascade="all, delete-orphan")