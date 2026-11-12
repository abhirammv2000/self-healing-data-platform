from shared.db import Base
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, Index, String, Text, Integer, UniqueConstraint, func, text

class PipelineRun(Base):
    __tablename__="pipeline_runs"
    # A plain UNIQUE(pipeline_id, idempotency_key) is enough on its own. Postgres treats every
    # NULL as distinct, so the majority of runs (no key supplied) never collide, and only two
    # runs on the same pipeline with the same key do. No partial index needed to exclude NULLs.
    __table_args__=(
        UniqueConstraint("pipeline_id", "idempotency_key", name="uq_pipeline_runs_pipeline_id_idempotency_key"),
        # At most one run of a pipeline is running at a time. This is the lock: claiming a second run of the pipeline
        # fails in the database, whichever worker tries, and it is released when the run stops being active
        # (it ends, or the reaper takes it back from a dead worker). See worker/app/executor.py _claim_run.
        Index(
            "uq_pipeline_runs_one_active", "pipeline_id", unique=True,
            postgresql_where=text("status IN ('running', 'retrying')"),
        ),
    )

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
    # When the run id was last put on the queue, and until when the worker that claimed the run owns it. Both are
    # compared only with the database's own now(), see worker/app/reaper.py. attempts counts the claims.
    last_enqueued_at: Mapped[datetime|None]=mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime|None]=mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int]=mapped_column(Integer, default=0, server_default="0", nullable=False)


    pipeline=relationship("Pipeline", back_populates="pipeline_runs")
    tenant=relationship("Tenant", back_populates="pipeline_runs")
    agent_recommendations=relationship("AgentRecommendation",back_populates="pipeline_run", cascade="all, delete-orphan")
    webhook_callbacks=relationship("WebhookCallback",back_populates="pipeline_run", cascade="all, delete-orphan")