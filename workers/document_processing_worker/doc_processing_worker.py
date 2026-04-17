import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from domain.document_processor import DocumentProcessor
from domain.entities.document_completion_payload import DocumentCompletionPayload
from domain.entities.document_revision import DocumentRevision
from domain.revision.revision_pipeline import RevisionPipeline
from workers.base_worker import BaseWorker
from workers.context import Context
from workers.models import (
    ExtractGuidelineMessage,
    ExtractGuidelinePayload,
    ProcessDocumentMessage,
    ProcessDocumentPayload,
)


class DocumentProcessingWorker(BaseWorker):
    """Worker for processing document processing messages from Pub/Sub."""

    def __init__(self) -> None:
        subscription_name = os.getenv("DOCUMENT_PROCESSING_SUBSCRIPTION")
        if not subscription_name:
            raise ValueError("DOCUMENT_PROCESSING_SUBSCRIPTION environment variable is required")
        super().__init__(subscription_name=subscription_name)

    def _convert_date_to_iso(self, date: Optional[str]) -> Optional[str]:
        if date is None:
            return None
        try:
            dt = datetime.strptime(date, "%m/%d/%Y").replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except ValueError:
            return date

    def _generate_context(self, message: ProcessDocumentMessage, correlation_id: str) -> Context:
        return Context(correlation_id=correlation_id, job_id=message.payload.job_id)

    async def _extract_guideline(self, payload: ExtractGuidelinePayload, context: Context) -> bool:
        processor = DocumentProcessor()
        await asyncio.to_thread(processor.extract_guideline_from_text, payload.raw_text or "")
        self.logger.info("Extract-guideline task completed for job_id=%s", context.job_id)
        return True

    async def process_document(
        self, payload: ProcessDocumentPayload, context: Context
    ) -> bool:
        try:
            result = await self._process_document(payload, context)
            status = "failed" if result.get("processing_failed") or not result.get("success") else "completed"
            completion: Optional[DocumentCompletionPayload] = result.get("completion_payload")
            doc_revision_dict = result.get("document_revision_dict")
            callback_data = self._create_callback_data(
                job_id=payload.job_id,
                status=status,
                message=result.get("message", ""),
                completion_payload=completion,
                document_revision_dict=doc_revision_dict,
            )
            complete_url = os.getenv("DOCUMENT_PROCESSING_COMPLETE_URL")
            secret = os.getenv("SECRET_KEY", "")
            headers = {"accept": "application/json", "Content-Type": "application/json"}
            if secret:
                headers["x-secret-key"] = secret
            if complete_url:
                await self.send_callback(complete_url, callback_data, headers=headers)
            return result.get("success", False)
        except Exception as e:
            self.logger.error("Unexpected error processing document: %s", e)
            failure_data = self._create_callback_data(
                job_id=payload.job_id,
                status="failed",
                message=f"Document processing failed: {str(e)}",
            )
            complete_url = os.getenv("DOCUMENT_PROCESSING_COMPLETE_URL")
            secret = os.getenv("SECRET_KEY", "")
            headers = {"accept": "application/json", "Content-Type": "application/json"}
            if secret:
                headers["x-secret-key"] = secret
            if complete_url:
                try:
                    await self.send_callback(complete_url, failure_data, headers=headers)
                except Exception as cb_err:
                    self.logger.error("Failed to send failure callback: %s", cb_err)
            return False

    async def process_message(self, message_data: Dict[str, Any], correlation_id: str) -> bool:
        try:
            if message_data.get("payload") is None:
                raise ValueError("Invalid payload: payload is empty")
            payload_obj = message_data.get("payload", {})
            task_type = payload_obj.get("task_type", "document-processing")
            if task_type not in ("document-processing", "extract-guideline"):
                task_type = "document-processing"
            if task_type == "extract-guideline":
                message = self.validate_message(message_data, ExtractGuidelineMessage)
                context = Context(correlation_id=correlation_id, job_id=message.payload.job_id)
                self.logger.info("Processing document job %s", context.job_id)
                return await self._extract_guideline(message.payload, context)
            message = self.validate_message(message_data, ProcessDocumentMessage)
            context = self._generate_context(message, correlation_id)
            self.logger.info("Processing document job %s", context.job_id)
            return await self.process_document(message.payload, context)
        except Exception as e:
            self.logger.error("Message validation/parsing failed: %s", e)
            payload_for_job = message_data.get("payload")
            job_id = (
                payload_for_job.get("job_id", "unknown")
                if isinstance(payload_for_job, dict)
                else "unknown"
            )
            failure_data = self._create_callback_data(
                job_id=job_id,
                status="failed",
                message=f"Message validation failed: {str(e)}",
                message_data=message_data,
            )
            complete_url = os.getenv("DOCUMENT_PROCESSING_COMPLETE_URL")
            secret = os.getenv("SECRET_KEY", "")
            headers = {"accept": "application/json", "Content-Type": "application/json"}
            if secret:
                headers["x-secret-key"] = secret
            if complete_url:
                try:
                    await self.send_callback(complete_url, failure_data, headers=headers)
                except Exception as cb_err:
                    self.logger.error("Could not send validation failure callback: %s", cb_err)
            return True

    async def _process_document(
        self, payload: ProcessDocumentPayload, context: Context
    ) -> Dict[str, Any]:
        try:
            self.logger.info("Processing file_path: %s", payload.file_path)
            parts = payload.file_path.split("/")
            if len(parts) < 3:
                raise ValueError(
                    f"Invalid file path format: {payload.file_path}, "
                    "expected format: source_id/revision_id/file_name.ext"
                )
            source_id = parts[0]
            revision_id = parts[1]
            file_name_with_ext = parts[-1]
            file_name = (
                file_name_with_ext.rsplit(".", 1)[0]
                if "." in file_name_with_ext
                else file_name_with_ext
            )
            file_type = (
                file_name_with_ext.rsplit(".", 1)[1] if "." in file_name_with_ext else ""
            )
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
            self.logger.info(
                "Processing document revision %s (source_id=%s, file_name=%s)",
                document_revision.revision_id,
                source_id,
                file_name,
            )

            pipeline_class_by_is_new = {
                True: DocumentProcessor,
                False: RevisionPipeline,
            }

            pipeline_class = pipeline_class_by_is_new[payload.is_new]
            pipeline = pipeline_class(customer_name=payload.customer_name)
            completion = await asyncio.to_thread(
                pipeline.run_complete_workflow,
                document_revision=document_revision,
                output_dir="processing_results",
                job_id=payload.job_id,
            )
            
            doc_dict = document_revision.model_dump()
            if doc_dict.get("status") and not isinstance(doc_dict.get("status"), str):
                doc_dict["status"] = getattr(doc_dict["status"], "value", str(doc_dict["status"]))
            self._update_doc_request(doc_dict, context)
            return {
                "success": completion.status == "completed",
                "message": completion.error_message or "",
                "completion_payload": completion,
                "document_revision_dict": doc_dict,
                "processing_failed": completion.status == "failed",
            }
        except Exception as e:
            self.logger.error("Processing failed: %s", e, exc_info=True)
            return {
                "success": False,
                "message": str(e),
                "revision_id": getattr(payload, "revision_id", ""),
            }

    def _update_doc_request(self, document_revision: Dict[str, Any], context: Context) -> None:
        update_url = os.getenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL")
        if not update_url:
            return
        effective = document_revision.get("policy_effective_date")
        end = document_revision.get("policy_end_date")
        payload = {
            "job_id": context.job_id,
            "category": document_revision.get("category"),
            "category_confidence": document_revision.get("category_confidence_score"),
            "effective_date": self._convert_date_to_iso(effective) if effective else effective,
            "end_date": self._convert_date_to_iso(end) if end else end,
            "is_pre_auth": document_revision.get("prior_auth_required"),
            "number_of_pages": document_revision.get("num_of_pages"),
        }
        secret = os.getenv("SECRET_KEY", "")
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        if secret:
            headers["x-secret-key"] = secret
        try:
            import httpx
            with httpx.Client(timeout=int(os.getenv("CALLBACK_TIMEOUT", "3000"))) as client:
                resp = client.post(update_url, headers=headers, json=payload)
                resp.raise_for_status()
        except Exception as e:
            self.logger.error("Failed to send document processing update to %s: %s", update_url, e)

    def _create_callback_data(
        self,
        job_id: str,
        status: str,
        message: str,
        completion_payload: Optional[DocumentCompletionPayload] = None,
        document_revision_dict: Optional[Dict[str, Any]] = None,
        message_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if completion_payload:
            d = completion_payload.model_dump()
            payload = {
                "job_id": job_id,
                "guidelines": d.get("guidelines", []),
                "linked_documents": d.get("linked_documents", []),
                "status": status,
                "error_message": message or d.get("error_message", ""),
                "processed_at": d.get("processed_at", datetime.now(timezone.utc).isoformat()),
                "doc_diff": d.get("doc_diff"),
                "complexity_score": d.get("complexity_score"),
                "complexity": d.get("complexity_level"),
                "complexity_details": d.get("complexity_breakdown"),
                "coding_section": d.get("coding_section"),
            }
        elif document_revision_dict:
            payload = {
                "job_id": job_id,
                "guidelines": document_revision_dict.get("guidelines", []),
                "linked_documents": document_revision_dict.get("related_policies", []),
                "status": status,
                "error_message": message,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "doc_diff": document_revision_dict.get("doc_diff"),
                "complexity_score": document_revision_dict.get("complexity_score"),
                "complexity": document_revision_dict.get("complexity_level"),
                "complexity_details": document_revision_dict.get("complexity_breakdown"),
                "coding_section": document_revision_dict.get("required_codes"),
            }
        else:
            payload = {
                "job_id": job_id,
                "guidelines": [],
                "linked_documents": [],
                "status": status,
                "error_message": message,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
        if message_data:
            payload["raw_payload"] = message_data
        return payload
