import json
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class MessageSender(BaseModel):
    container: str
    name: str


class MessageMeta(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    sender: MessageSender
    sent_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    trace_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    priority: str = "normal"


class ProcessDocumentPayload(BaseModel):
    correlation_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str
    revision_id: str
    file_path: str
    is_new: bool = False
    document_name: str
    customer_name: str
    old_rev_id: Optional[str] = None
    old_rev_file_name: Optional[str] = None


class ProcessDocumentMessage(BaseModel):
    meta: MessageMeta
    payload: ProcessDocumentPayload


class RevisionDocumentPayload(BaseModel):
    correlation_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str
    revision_id: str
    file_path: str
    document_name: str
    customer_name: str
    old_rev_id: str
    old_rev_file_name: str


class RevisionDocumentMessage(BaseModel):
    meta: MessageMeta
    payload: RevisionDocumentPayload


class ExtractGuidelinePayload(BaseModel):
    correlation_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str
    revision_id: str
    raw_text: str


class ExtractGuidelineMessage(BaseModel):
    meta: MessageMeta
    payload: ExtractGuidelinePayload


class Connection(BaseModel):
    category: str
    url: str


class SubmitScrapingJobPayload(BaseModel):
    correlation_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str
    source_id: str
    connection: Connection
    limit: Optional[int]
    customer_name: str

    @field_validator("connection", mode="before")
    @classmethod
    def parse_connection(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError("connection must be valid JSON") from e
        return v


class SubmitScrapingJobMessage(BaseModel):
    meta: MessageMeta
    payload: SubmitScrapingJobPayload


class ScrapingJobCompleteRequest(BaseModel):
    job_id: str
    downloaded_count: int
    skipped_count: int
    error_count: int
    error_message: Optional[str] = ""
    error_details: Optional[str] = ""


class ScrapingJobStartRequest(BaseModel):
    job_id: str


class ScrapingJobUpdateRequest(BaseModel):
    job_id: str
    file_path: Optional[str] = ""
    revision_id: Optional[str] = ""
    effective_date: Optional[str] = ""
    policy_number: Optional[str] = ""
    error_message: Optional[str] = ""
    error_details: Optional[str] = ""
    category: Optional[str] = None
    number_of_pages: Optional[int] = None


class DupeCheckIdRequest(BaseModel):
    file_name: str
    policy_name: str = ""
    policy_id: str = ""
    job_id: str
    source_id: str


class DupCheckIdResponse(BaseModel):
    documentRevision: Optional[dict] = None
    exists: Optional[bool] = None
    success: Optional[bool] = None
    error: Optional[str] = None
