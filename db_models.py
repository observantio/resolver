"""
All database models for Resolver are defined in this module. This includes the RcaJob and RcaReport models, which represent the RCA jobs and their corresponding reports stored in the database. The models are defined using SQLAlchemy's DeclarativeBase, allowing for easy interaction with the database using Python objects.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from custom_types.json import JSONDict


class Base(DeclarativeBase):
    pass


class RcaJob(Base):
    __tablename__ = "rca_jobs"
    job_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    report_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    requested_by: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    request_payload: Mapped[JSONDict] = mapped_column(JSON, nullable=False)
    summary_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delete_requested_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    report = relationship("RcaReport", back_populates="job", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_rca_jobs_tenant_created_desc", "tenant_id", "created_at"),
        Index("ix_rca_jobs_tenant_status_created_desc", "tenant_id", "status", "created_at"),
        Index("ix_rca_jobs_requested_by_tenant_created_desc", "requested_by", "tenant_id", "created_at"),
        Index("ix_rca_jobs_tenant_user_status_created_job", "tenant_id", "requested_by", "status", "created_at", "job_id"),
        Index("ix_rca_jobs_tenant_user_created_job", "tenant_id", "requested_by", "created_at", "job_id"),
        Index("ix_rca_jobs_fingerprint_tenant", "request_fingerprint", "tenant_id"),
    )


class RcaReport(Base):
    __tablename__ = "rca_reports"

    report_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("rca_jobs.job_id", ondelete="CASCADE"), unique=True, nullable=False)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    owner_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    result_payload: Mapped[JSONDict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    job = relationship("RcaJob", back_populates="report")

    __table_args__ = (
        Index("ix_rca_reports_tenant_report", "tenant_id", "report_id"),
    )
