"""Revision document processing worker.

Listens on REVISION_PROCESSING_SUBSCRIPTION and runs the RevisionPipeline
for each incoming message. Mirrors the structure of DocumentProcessingWorker.
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from domain.entities.document_completion_payload import DocumentCompletionPayload
from domain.entities.document_revision import DocumentRevision  # noqa: F401 — used in type hints
from domain.revision.revision_pipeline import RevisionPipeline
from workers.base_worker import BaseWorker
from workers.context import Context
from workers.models import RevisionDocumentMessage, RevisionDocumentPayload


class RevisionProcessingWorker(BaseWorker):
    """Worker for processing document revision messages from Pub/Sub."""

    def __init__(self) -> None:
        subscription_name = os.getenv("REVISION_PROCESSING_SUBSCRIPTION")
        if not subscription_name:
            raise ValueError("REVISION_PROCESSING_SUBSCRIPTION environment variable is required")
        super().__init__(subscription_name=subscription_name)

    async def process_message(self, message_data: Dict[str, Any], correlation_id: str) -> bool:
        try:
            if message_data.get("payload") is None:
                raise ValueError("Invalid payload: payload is empty")
            message = self.validate_message(message_data, RevisionDocumentMessage)
            payload = message.payload
            context = Context(correlation_id=correlation_id, job_id=payload.job_id)
            self.logger.info("Processing revision job %s", context.job_id)
            return await self._process_revision(payload, context)
        except Exception as e:
            self.logger.error("Message validation/parsing failed: %s", e)
            payload_raw = message_data.get("payload")
            job_id = payload_raw.get("job_id", "unknown") if isinstance(payload_raw, dict) else "unknown"
            await self._send_failure(job_id, f"Message validation failed: {str(e)}")
            return True  # ack to avoid retry loop on bad message

    async def _process_revision(self, payload: RevisionDocumentPayload, context: Context) -> bool:
        try:
            parts = payload.file_path.split("/")
            if len(parts) < 3:
                raise ValueError(f"Invalid file_path format: {payload.file_path}")
            source_id = parts[0]
            revision_id = parts[1]
            file_name_with_ext = parts[-1]
            file_name = file_name_with_ext.rsplit(".", 1)[0] if "." in file_name_with_ext else file_name_with_ext
            file_type = file_name_with_ext.rsplit(".", 1)[1] if "." in file_name_with_ext else ""

            document_revision = DocumentRevision(
                revision_id=payload.revision_id,
                source_id=source_id,
                file_name=file_name,
                file_type=file_type or None,
                customer_name=payload.customer_name,
                file_path=payload.file_path,
                old_rev_id=payload.old_rev_id,
                old_rev_file_name=payload.old_rev_file_name,
            )

            pipeline = RevisionPipeline(customer_name=payload.customer_name)
            completion = await asyncio.to_thread(
                pipeline.run,
                document_revision,
                "processing_results",
                payload.job_id,
            )

            # Send metadata update (pages, category etc.) — mirrors doc_processing_worker
            doc_dict = document_revision.model_dump()
            self._update_doc_request(doc_dict, context)

            callback_data = self._build_callback(payload.job_id, completion, document_revision)
            complete_url = os.getenv("DOCUMENT_PROCESSING_COMPLETE_URL")
            if complete_url:
                await self.send_callback(complete_url, callback_data, headers=self._secret_headers())
            else:
                self.logger.error("DOCUMENT_PROCESSING_COMPLETE_URL environment variable is not set")

            return completion.status == "completed"

        except Exception as e:
            self.logger.error("Revision processing failed: %s", e, exc_info=True)
            await self._send_failure(payload.job_id, str(e))
            return False

    async def _send_failure(self, job_id: str, message: str) -> None:
        complete_url = os.getenv("REVISION_PROCESSING_COMPLETE_URL")
        if complete_url:
            data = {
                "job_id": job_id,
                "status": "failed",
                "error_message": message,
                "guidelines": [],
                "linked_documents": [],
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                await self.send_callback(complete_url, data, headers=self._secret_headers())
            except Exception as cb_err:
                self.logger.error("Failed to send failure callback: %s", cb_err)

    def _secret_headers(self) -> Dict[str, str]:
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        secret = os.getenv("SECRET_KEY", "")
        if secret:
            headers["x-secret-key"] = secret
        return headers

    def _update_doc_request(self, document_revision: Dict[str, Any], context: Context) -> None:
        """Send metadata update to main app — mirrors DocumentProcessingWorker._update_doc_request."""
        update_url = os.getenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL")
        if not update_url:
            return
        payload = {
            "job_id": context.job_id,
            "number_of_pages": document_revision.get("num_of_pages"),
        }
        import httpx
        secret = os.getenv("SECRET_KEY", "")
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        if secret:
            headers["x-secret-key"] = secret
        try:
            with httpx.Client(timeout=int(os.getenv("CALLBACK_TIMEOUT", "3000"))) as client:
                resp = client.post(update_url, headers=headers, json=payload)
                resp.raise_for_status()
        except Exception as e:
            self.logger.error("Failed to send revision update to %s: %s", update_url, e)

    def _build_callback(
        self,
        job_id: str,
        completion: DocumentCompletionPayload,
        document_revision: Optional["DocumentRevision"] = None,
    ) -> Dict[str, Any]:
        d = completion.model_dump()
        data: Dict[str, Any] = {
            "job_id": job_id,
            "status": d.get("status"),
            "error_message": d.get("error_message", ""),
            "guidelines": d.get("guidelines", []),
            "linked_documents": d.get("linked_documents", []),
            "coding_section": d.get("coding_section"),
            "processed_at": d.get("processed_at", datetime.now(timezone.utc).isoformat()),
        }
        # Include code diff so receiver knows exactly what changed
        if document_revision:
            data["added_codes"] = document_revision.added_codes or {}
            data["removed_codes"] = document_revision.removed_codes or {}
            data["number_of_pages"] = document_revision.num_of_pages
        return data
