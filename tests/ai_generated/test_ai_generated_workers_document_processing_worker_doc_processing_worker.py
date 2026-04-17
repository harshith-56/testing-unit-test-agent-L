from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from unittest.mock import MagicMock
from unittest.mock import MagicMock, patch
from unittest.mock import patch
from workers.document_processing_worker.doc_processing_worker import DocumentProcessingWorker
from workers.document_processing_worker.doc_processing_worker import DocumentProcessor, RevisionPipeline
from workers.document_processing_worker.doc_processing_worker import ExtractGuidelineMessage, ProcessDocumentMessage, Context
from workers.document_processing_worker.doc_processing_worker import ExtractGuidelinePayload, Context
from workers.document_processing_worker.doc_processing_worker import ProcessDocumentPayload, Context
import asyncio
import os
import pytest

# AI_TEST_AGENT_START function=DocumentProcessingWorker.__init__
def test_init_with_valid_subscription_name(monkeypatch):
    monkeypatch.setenv("DOCUMENT_PROCESSING_SUBSCRIPTION", "valid_subscription")
    worker = DocumentProcessingWorker()
    assert hasattr(worker, "subscription_name")
    assert worker.subscription_name == "valid_subscription"

def test_init_raises_value_error_when_env_var_missing(monkeypatch):
    monkeypatch.delenv("DOCUMENT_PROCESSING_SUBSCRIPTION", raising=False)
    with pytest.raises(ValueError, match="DOCUMENT_PROCESSING_SUBSCRIPTION environment variable is required"):
        DocumentProcessingWorker()

def test_init_raises_value_error_when_env_var_empty(monkeypatch):
    monkeypatch.setenv("DOCUMENT_PROCESSING_SUBSCRIPTION", "")
    with pytest.raises(ValueError, match="DOCUMENT_PROCESSING_SUBSCRIPTION environment variable is required"):
        DocumentProcessingWorker()

def test_init_raises_value_error_when_env_var_is_whitespace(monkeypatch):
    monkeypatch.setenv("DOCUMENT_PROCESSING_SUBSCRIPTION", "   ")
    with pytest.raises(ValueError, match="DOCUMENT_PROCESSING_SUBSCRIPTION environment variable is required"):
        DocumentProcessingWorker()

def test_init_accepts_subscription_name_with_special_characters(monkeypatch):
    special_name = "sub$cription_123-!@#"
    monkeypatch.setenv("DOCUMENT_PROCESSING_SUBSCRIPTION", special_name)
    worker = DocumentProcessingWorker()
    assert worker.subscription_name == special_name

def test_init_accepts_subscription_name_with_numeric_string(monkeypatch):
    numeric_name = "1234567890"
    monkeypatch.setenv("DOCUMENT_PROCESSING_SUBSCRIPTION", numeric_name)
    worker = DocumentProcessingWorker()
    assert worker.subscription_name == numeric_name

def test_init_accepts_subscription_name_with_long_string(monkeypatch):
    long_name = "a" * 1000
    monkeypatch.setenv("DOCUMENT_PROCESSING_SUBSCRIPTION", long_name)
    worker = DocumentProcessingWorker()
    assert worker.subscription_name == long_name

def test_init_raises_value_error_when_env_var_is_none(monkeypatch):
    monkeypatch.setenv("DOCUMENT_PROCESSING_SUBSCRIPTION", "valid")
    with patch.dict(os.environ, {"DOCUMENT_PROCESSING_SUBSCRIPTION": None}):
        with pytest.raises(ValueError, match="DOCUMENT_PROCESSING_SUBSCRIPTION environment variable is required"):
            DocumentProcessingWorker()
# AI_TEST_AGENT_END function=DocumentProcessingWorker.__init__

# AI_TEST_AGENT_START function=DocumentProcessingWorker._convert_date_to_iso
def test_convert_date_to_iso_with_none_returns_none():
    worker = DocumentProcessingWorker()
    result = worker._convert_date_to_iso(None)
    assert result is None

def test_convert_date_to_iso_with_valid_date_returns_iso_format():
    worker = DocumentProcessingWorker()
    input_date = "12/31/2020"
    expected = "2020-12-31T00:00:00.000Z"
    result = worker._convert_date_to_iso(input_date)
    assert result == expected

def test_convert_date_to_iso_with_invalid_date_format_returns_original_string():
    worker = DocumentProcessingWorker()
    input_date = "2020-12-31"
    result = worker._convert_date_to_iso(input_date)
    assert result == input_date

def test_convert_date_to_iso_with_empty_string_returns_original_string():
    worker = DocumentProcessingWorker()
    input_date = ""
    result = worker._convert_date_to_iso(input_date)
    assert result == input_date

def test_convert_date_to_iso_with_malformed_date_returns_original_string():
    worker = DocumentProcessingWorker()
    input_date = "13/40/2020"
    result = worker._convert_date_to_iso(input_date)
    assert result == input_date

def test_convert_date_to_iso_with_non_string_input_raises_type_error():
    worker = DocumentProcessingWorker()
    with pytest.raises(TypeError):
        worker._convert_date_to_iso(12345)

def test_convert_date_to_iso_with_leap_year_date_returns_iso_format():
    worker = DocumentProcessingWorker()
    input_date = "02/29/2020"
    expected = "2020-02-29T00:00:00.000Z"
    result = worker._convert_date_to_iso(input_date)
    assert result == expected

def test_convert_date_to_iso_with_single_digit_month_and_day_returns_iso_format():
    worker = DocumentProcessingWorker()
    input_date = "1/5/2021"
    expected = "2021-01-05T00:00:00.000Z"
    result = worker._convert_date_to_iso(input_date)
    assert result == expected

def test_convert_date_to_iso_with_whitespace_string_returns_original_string():
    worker = DocumentProcessingWorker()
    input_date = "   "
    result = worker._convert_date_to_iso(input_date)
    assert result == input_date
# AI_TEST_AGENT_END function=DocumentProcessingWorker._convert_date_to_iso

# AI_TEST_AGENT_START function=DocumentProcessingWorker._extract_guideline
@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread", new_callable=AsyncMock)
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor.extract_guideline_from_text")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessingWorker.logger")
async def test_extract_guideline_with_valid_raw_text(mock_logger, mock_extract, mock_to_thread):
    worker = DocumentProcessingWorker()
    payload = ExtractGuidelinePayload(raw_text="valid guideline text")
    context = Context(job_id="job123")
    mock_to_thread.return_value = None
    mock_extract.return_value = None

    result = await worker._extract_guideline(payload, context)

    mock_to_thread.assert_awaited_once_with(mock_extract, "valid guideline text")
    mock_logger.info.assert_called_once_with("Extract-guideline task completed for job_id=%s", "job123")
    assert result is True

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread", new_callable=AsyncMock)
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor.extract_guideline_from_text")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessingWorker.logger")
async def test_extract_guideline_with_empty_raw_text(mock_logger, mock_extract, mock_to_thread):
    worker = DocumentProcessingWorker()
    payload = ExtractGuidelinePayload(raw_text="")
    context = Context(job_id="job_empty")
    mock_to_thread.return_value = None
    mock_extract.return_value = None

    result = await worker._extract_guideline(payload, context)

    mock_to_thread.assert_awaited_once_with(mock_extract, "")
    mock_logger.info.assert_called_once_with("Extract-guideline task completed for job_id=%s", "job_empty")
    assert result is True

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread", new_callable=AsyncMock)
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor.extract_guideline_from_text")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessingWorker.logger")
async def test_extract_guideline_with_none_raw_text(mock_logger, mock_extract, mock_to_thread):
    worker = DocumentProcessingWorker()
    payload = ExtractGuidelinePayload(raw_text=None)
    context = Context(job_id="job_none")
    mock_to_thread.return_value = None
    mock_extract.return_value = None

    result = await worker._extract_guideline(payload, context)

    mock_to_thread.assert_awaited_once_with(mock_extract, "")
    mock_logger.info.assert_called_once_with("Extract-guideline task completed for job_id=%s", "job_none")
    assert result is True

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread", new_callable=AsyncMock)
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor.extract_guideline_from_text")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessingWorker.logger")
async def test_extract_guideline_to_thread_raises_exception(mock_logger, mock_extract, mock_to_thread):
    worker = DocumentProcessingWorker()
    payload = ExtractGuidelinePayload(raw_text="text")
    context = Context(job_id="job_error")
    mock_to_thread.side_effect = RuntimeError("thread error")
    mock_extract.return_value = None

    with pytest.raises(RuntimeError, match="thread error"):
        await worker._extract_guideline(payload, context)

    mock_to_thread.assert_awaited_once_with(mock_extract, "text")
    mock_logger.info.assert_not_called()

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread", new_callable=AsyncMock)
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor.extract_guideline_from_text")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessingWorker.logger")
async def test_extract_guideline_extract_guideline_raises_exception(mock_logger, mock_extract, mock_to_thread):
    worker = DocumentProcessingWorker()
    payload = ExtractGuidelinePayload(raw_text="text")
    context = Context(job_id="job_extract_error")
    mock_extract.side_effect = ValueError("extract error")
    mock_to_thread.return_value = None

    with pytest.raises(ValueError, match="extract error"):
        await worker._extract_guideline(payload, context)

    mock_to_thread.assert_awaited_once()
    mock_logger.info.assert_not_called()

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread", new_callable=AsyncMock)
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor.extract_guideline_from_text")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessingWorker.logger")
async def test_extract_guideline_logger_info_called_once_with_correct_args(mock_logger, mock_extract, mock_to_thread):
    worker = DocumentProcessingWorker()
    payload = ExtractGuidelinePayload(raw_text="some text")
    context = Context(job_id="job_logger_test")
    mock_to_thread.return_value = None
    mock_extract.return_value = None

    result = await worker._extract_guideline(payload, context)

    mock_logger.info.assert_called_once()
    args, kwargs = mock_logger.info.call_args
    assert args[0] == "Extract-guideline task completed for job_id=%s"
    assert args[1] == "job_logger_test"
    assert result is True

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread", new_callable=AsyncMock)
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor.extract_guideline_from_text")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessingWorker.logger")
async def test_extract_guideline_with_non_string_raw_text_raises_type_error(mock_logger, mock_extract, mock_to_thread):
    worker = DocumentProcessingWorker()
    payload = ExtractGuidelinePayload(raw_text=12345)  # invalid type
    context = Context(job_id="job_type_error")
    mock_to_thread.return_value = None
    mock_extract.return_value = None

    with pytest.raises(TypeError):
        await worker._extract_guideline(payload, context)

    mock_to_thread.assert_not_awaited()
    mock_logger.info.assert_not_called()
# AI_TEST_AGENT_END function=DocumentProcessingWorker._extract_guideline

# AI_TEST_AGENT_START function=DocumentProcessingWorker.process_document
class DummyPayload:
    job_id = "job123"

class DummyContext:
    pass

@pytest.mark.asyncio
@patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": "http://callback.url", "SECRET_KEY": "secret123"})
async def test_process_document_success_completed_status_and_callback_sent():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    result_dict = {
        "processing_failed": False,
        "success": True,
        "completion_payload": {"data": "completed"},
        "document_revision_dict": {"rev": 1},
        "message": "All good"
    }
    worker._process_document = AsyncMock(return_value=result_dict)
    worker._create_callback_data = MagicMock(return_value={"callback": "data"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    result = await worker.process_document(payload, context)

    assert result is True
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once_with(
        job_id=payload.job_id,
        status="completed",
        message="All good",
        completion_payload=result_dict["completion_payload"],
        document_revision_dict=result_dict["document_revision_dict"],
    )
    worker.send_callback.assert_awaited_once()
    called_url, called_data, called_kwargs = worker.send_callback.call_args[0][0], worker.send_callback.call_args[0][1], worker.send_callback.call_args[1]
    assert called_url == "http://callback.url"
    assert called_data == {"callback": "data"}
    assert called_kwargs["headers"]["x-secret-key"] == "secret123"
    assert called_kwargs["headers"]["accept"] == "application/json"
    assert called_kwargs["headers"]["Content-Type"] == "application/json"
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
@patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": "http://callback.url"})
async def test_process_document_failure_status_and_callback_sent_without_secret_key():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    result_dict = {
        "processing_failed": True,
        "success": False,
        "completion_payload": None,
        "document_revision_dict": None,
        "message": "Failed processing"
    }
    worker._process_document = AsyncMock(return_value=result_dict)
    worker._create_callback_data = MagicMock(return_value={"callback": "faildata"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    result = await worker.process_document(payload, context)

    assert result is False
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once_with(
        job_id=payload.job_id,
        status="failed",
        message="Failed processing",
        completion_payload=None,
        document_revision_dict=None,
    )
    worker.send_callback.assert_awaited_once()
    called_url, called_data, called_kwargs = worker.send_callback.call_args[0][0], worker.send_callback.call_args[0][1], worker.send_callback.call_args[1]
    assert called_url == "http://callback.url"
    assert called_data == {"callback": "faildata"}
    assert "x-secret-key" not in called_kwargs["headers"]
    assert called_kwargs["headers"]["accept"] == "application/json"
    assert called_kwargs["headers"]["Content-Type"] == "application/json"
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
@patch.dict(os.environ, {}, clear=True)
async def test_process_document_success_no_callback_url_returns_success():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    result_dict = {
        "processing_failed": False,
        "success": True,
        "completion_payload": None,
        "document_revision_dict": None,
        "message": ""
    }
    worker._process_document = AsyncMock(return_value=result_dict)
    worker._create_callback_data = MagicMock(return_value={"callback": "data"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    result = await worker.process_document(payload, context)

    assert result is True
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once_with(
        job_id=payload.job_id,
        status="completed",
        message="",
        completion_payload=None,
        document_revision_dict=None,
    )
    worker.send_callback.assert_not_awaited()
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
@patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": "http://callback.url", "SECRET_KEY": "secret123"})
async def test_process_document_raises_exception_and_sends_failure_callback():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    exc = RuntimeError("processing error")
    worker._process_document = AsyncMock(side_effect=exc)
    worker._create_callback_data = MagicMock(return_value={"callback": "faildata"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    result = await worker.process_document(payload, context)

    assert result is False
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once()
    call_args = worker._create_callback_data.call_args[1]
    assert call_args["job_id"] == payload.job_id
    assert call_args["status"] == "failed"
    assert "processing error" in call_args["message"]
    worker.send_callback.assert_awaited_once()
    called_url, called_data, called_kwargs = worker.send_callback.call_args[0][0], worker.send_callback.call_args[0][1], worker.send_callback.call_args[1]
    assert called_url == "http://callback.url"
    assert called_data == {"callback": "faildata"}
    assert called_kwargs["headers"]["x-secret-key"] == "secret123"
    worker.logger.error.assert_any_call("Unexpected error processing document: %s", exc)

@pytest.mark.asyncio
@patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": "http://callback.url"})
async def test_process_document_raises_exception_and_send_callback_raises_logs_error_and_returns_false():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    exc = RuntimeError("processing error")
    cb_exc = RuntimeError("callback error")
    worker._process_document = AsyncMock(side_effect=exc)
    worker._create_callback_data = MagicMock(return_value={"callback": "faildata"})
    worker.send_callback = AsyncMock(side_effect=cb_exc)
    worker.logger = MagicMock()

    result = await worker.process_document(payload, context)

    assert result is False
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once()
    worker.send_callback.assert_awaited_once()
    worker.logger.error.assert_any_call("Unexpected error processing document: %s", exc)
    worker.logger.error.assert_any_call("Failed to send failure callback: %s", cb_exc)

@pytest.mark.asyncio
@patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": "http://callback.url", "SECRET_KEY": ""})
async def test_process_document_secret_key_empty_header_not_included():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    result_dict = {
        "processing_failed": False,
        "success": True,
        "completion_payload": None,
        "document_revision_dict": None,
        "message": "done"
    }
    worker._process_document = AsyncMock(return_value=result_dict)
    worker._create_callback_data = MagicMock(return_value={"callback": "data"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    result = await worker.process_document(payload, context)

    assert result is True
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once()
    worker.send_callback.assert_awaited_once()
    headers = worker.send_callback.call_args[1]["headers"]
    assert "x-secret-key" not in headers
    assert headers["accept"] == "application/json"
    assert headers["Content-Type"] == "application/json"
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
@patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": "http://callback.url"})
async def test_process_document_result_missing_keys_defaults_to_failed_and_false():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    # result missing keys: no processing_failed, no success, no message, no completion_payload, no document_revision_dict
    result_dict = {}
    worker._process_document = AsyncMock(return_value=result_dict)
    worker._create_callback_data = MagicMock(return_value={"callback": "data"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    result = await worker.process_document(payload, context)

    assert result is False
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once_with(
        job_id=payload.job_id,
        status="failed",
        message="",
        completion_payload=None,
        document_revision_dict=None,
    )
    worker.send_callback.assert_awaited_once()
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
@patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": "http://callback.url"})
async def test_process_document_callback_url_none_does_not_send_callback():
    worker = DocumentProcessingWorker()
    payload = DummyPayload()
    context = DummyContext()
    result_dict = {
        "processing_failed": False,
        "success": True,
        "completion_payload": None,
        "document_revision_dict": None,
        "message": "done"
    }
    worker._process_document = AsyncMock(return_value=result_dict)
    worker._create_callback_data = MagicMock(return_value={"callback": "data"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    with patch.dict(os.environ, {"DOCUMENT_PROCESSING_COMPLETE_URL": ""}):
        result = await worker.process_document(payload, context)

    assert result is True
    worker._process_document.assert_awaited_once_with(payload, context)
    worker._create_callback_data.assert_called_once()
    worker.send_callback.assert_not_awaited()
    worker.logger.error.assert_not_called()
# AI_TEST_AGENT_END function=DocumentProcessingWorker.process_document

# AI_TEST_AGENT_START function=DocumentProcessingWorker.process_message
@pytest.mark.asyncio
async def test_process_message_extract_guideline_happy_path():
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": {
            "task_type": "extract-guideline",
            "job_id": "job123",
            "raw_text": "some text"
        }
    }
    correlation_id = "corr-1"

    mock_message = MagicMock()
    mock_message.payload.job_id = "job123"
    worker.validate_message = MagicMock(return_value=mock_message)
    worker._extract_guideline = AsyncMock(return_value=True)
    worker.logger = MagicMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.validate_message.assert_called_once_with(message_data, ExtractGuidelineMessage)
    worker._extract_guideline.assert_awaited_once_with(mock_message.payload, Context(correlation_id=correlation_id, job_id="job123"))
    worker.logger.info.assert_any_call("Processing document job %s", "job123")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_document_processing_happy_path():
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": {
            "task_type": "document-processing",
            "job_id": "job456",
            "content": "doc content"
        }
    }
    correlation_id = "corr-2"

    mock_message = MagicMock()
    mock_message.payload.job_id = "job456"
    worker.validate_message = MagicMock(return_value=mock_message)
    worker._generate_context = MagicMock(return_value=Context(correlation_id=correlation_id, job_id="job456"))
    worker.process_document = AsyncMock(return_value=True)
    worker.logger = MagicMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.validate_message.assert_called_once_with(message_data, ProcessDocumentMessage)
    worker._generate_context.assert_called_once_with(mock_message, correlation_id)
    worker.process_document.assert_awaited_once_with(mock_message.payload, Context(correlation_id=correlation_id, job_id="job456"))
    worker.logger.info.assert_any_call("Processing document job %s", "job456")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_invalid_payload_raises_and_sends_callback(monkeypatch):
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": None
    }
    correlation_id = "corr-3"

    worker.validate_message = MagicMock()
    worker._create_callback_data = MagicMock(return_value={"job_id": "unknown", "status": "failed"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    monkeypatch.setenv("DOCUMENT_PROCESSING_COMPLETE_URL", "http://callback.url")
    monkeypatch.setenv("SECRET_KEY", "secret123")

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", "Invalid payload: payload is empty")
    worker._create_callback_data.assert_called_once_with(
        job_id="unknown",
        status="failed",
        message="Message validation failed: Invalid payload: payload is empty",
        message_data=message_data,
    )
    worker.send_callback.assert_awaited_once()
    assert result is True

@pytest.mark.asyncio
async def test_process_message_invalid_task_type_defaults_to_document_processing():
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": {
            "task_type": "invalid-task",
            "job_id": "job789",
            "content": "doc content"
        }
    }
    correlation_id = "corr-4"

    mock_message = MagicMock()
    mock_message.payload.job_id = "job789"
    worker.validate_message = MagicMock(return_value=mock_message)
    worker._generate_context = MagicMock(return_value=Context(correlation_id=correlation_id, job_id="job789"))
    worker.process_document = AsyncMock(return_value=True)
    worker.logger = MagicMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.validate_message.assert_called_once_with(message_data, ProcessDocumentMessage)
    worker._generate_context.assert_called_once_with(mock_message, correlation_id)
    worker.process_document.assert_awaited_once_with(mock_message.payload, Context(correlation_id=correlation_id, job_id="job789"))
    worker.logger.info.assert_any_call("Processing document job %s", "job789")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_validate_message_raises_and_callback_send_callback_raises(monkeypatch):
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": {
            "task_type": "document-processing",
            "job_id": "job999"
        }
    }
    correlation_id = "corr-5"

    def raise_value_error(*args, **kwargs):
        raise ValueError("validation error")

    worker.validate_message = MagicMock(side_effect=raise_value_error)
    worker._create_callback_data = MagicMock(return_value={"job_id": "job999", "status": "failed"})
    worker.send_callback = AsyncMock(side_effect=Exception("callback send failed"))
    worker.logger = MagicMock()

    monkeypatch.setenv("DOCUMENT_PROCESSING_COMPLETE_URL", "http://callback.url")
    monkeypatch.setenv("SECRET_KEY", "secretkey")

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", "validation error")
    worker._create_callback_data.assert_called_once_with(
        job_id="job999",
        status="failed",
        message="Message validation failed: validation error",
        message_data=message_data,
    )
    worker.send_callback.assert_awaited_once()
    worker.logger.error.assert_any_call("Could not send validation failure callback: %s", Exception("callback send failed"))
    assert result is True

@pytest.mark.asyncio
async def test_process_message_payload_not_dict_job_id_unknown(monkeypatch):
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": "not a dict"
    }
    correlation_id = "corr-6"

    def raise_value_error(*args, **kwargs):
        raise ValueError("validation error")

    worker.validate_message = MagicMock(side_effect=raise_value_error)
    worker._create_callback_data = MagicMock(return_value={"job_id": "unknown", "status": "failed"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    monkeypatch.setenv("DOCUMENT_PROCESSING_COMPLETE_URL", "http://callback.url")
    monkeypatch.delenv("SECRET_KEY", raising=False)

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", "validation error")
    worker._create_callback_data.assert_called_once_with(
        job_id="unknown",
        status="failed",
        message="Message validation failed: validation error",
        message_data=message_data,
    )
    worker.send_callback.assert_awaited_once()
    assert result is True

@pytest.mark.asyncio
async def test_process_message_no_task_type_defaults_to_document_processing():
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": {
            "job_id": "job321",
            "content": "doc content"
        }
    }
    correlation_id = "corr-7"

    mock_message = MagicMock()
    mock_message.payload.job_id = "job321"
    worker.validate_message = MagicMock(return_value=mock_message)
    worker._generate_context = MagicMock(return_value=Context(correlation_id=correlation_id, job_id="job321"))
    worker.process_document = AsyncMock(return_value=True)
    worker.logger = MagicMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.validate_message.assert_called_once_with(message_data, ProcessDocumentMessage)
    worker._generate_context.assert_called_once_with(mock_message, correlation_id)
    worker.process_document.assert_awaited_once_with(mock_message.payload, Context(correlation_id=correlation_id, job_id="job321"))
    worker.logger.info.assert_any_call("Processing document job %s", "job321")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_validate_message_raises_without_callback_url(monkeypatch):
    worker = DocumentProcessingWorker()
    message_data = {
        "payload": {
            "task_type": "document-processing",
            "job_id": "job555"
        }
    }
    correlation_id = "corr-8"

    def raise_value_error(*args, **kwargs):
        raise ValueError("validation error")

    worker.validate_message = MagicMock(side_effect=raise_value_error)
    worker._create_callback_data = MagicMock(return_value={"job_id": "job555", "status": "failed"})
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()

    monkeypatch.delenv("DOCUMENT_PROCESSING_COMPLETE_URL", raising=False)
    monkeypatch.delenv("SECRET_KEY", raising=False)

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", "validation error")
    worker._create_callback_data.assert_called_once_with(
        job_id="job555",
        status="failed",
        message="Message validation failed: validation error",
        message_data=message_data,
    )
    worker.send_callback.assert_not_awaited()
    assert result is True
# AI_TEST_AGENT_END function=DocumentProcessingWorker.process_message

# AI_TEST_AGENT_START function=DocumentProcessingWorker._process_document
@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor")
@patch("workers.document_processing_worker.doc_processing_worker.RevisionPipeline")
async def test_process_document_success_new_revision(
    mock_revision_pipeline, mock_document_processor, mock_to_thread
):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    worker._update_doc_request = MagicMock()
    payload = ProcessDocumentPayload(
        file_path="source1/rev1/file.txt",
        revision_id="rev1",
        customer_name="custA",
        old_rev_id=None,
        old_rev_file_name=None,
        is_new=True,
        job_id="job123",
    )
    context = Context()
    completion_mock = MagicMock()
    completion_mock.status = "completed"
    completion_mock.error_message = ""
    mock_to_thread.return_value = completion_mock
    mock_document_processor.return_value.run_complete_workflow = MagicMock()

    result = await worker._process_document(payload, context)

    worker.logger.info.assert_any_call("Processing file_path: %s", payload.file_path)
    worker.logger.info.assert_any_call(
        "Processing document revision %s (source_id=%s, file_name=%s)",
        payload.revision_id,
        "source1",
        "file",
    )
    mock_document_processor.assert_called_once_with(customer_name="custA")
    mock_to_thread.assert_called_once()
    worker._update_doc_request.assert_called_once()
    assert result["success"] is True
    assert result["message"] == ""
    assert result["completion_payload"] == completion_mock
    assert result["processing_failed"] is False
    assert result["document_revision_dict"]["revision_id"] == "rev1"
    assert result["document_revision_dict"]["file_name"] == "file"
    assert result["document_revision_dict"]["file_type"] == "txt"

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor")
@patch("workers.document_processing_worker.doc_processing_worker.RevisionPipeline")
async def test_process_document_success_existing_revision(
    mock_revision_pipeline, mock_document_processor, mock_to_thread
):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    worker._update_doc_request = MagicMock()
    payload = ProcessDocumentPayload(
        file_path="source2/rev2/file.pdf",
        revision_id="rev2",
        customer_name="custB",
        old_rev_id="oldrev",
        old_rev_file_name="oldfile.pdf",
        is_new=False,
        job_id="job456",
    )
    context = Context()
    completion_mock = MagicMock()
    completion_mock.status = "failed"
    completion_mock.error_message = "error occurred"
    mock_to_thread.return_value = completion_mock
    mock_revision_pipeline.return_value.run_complete_workflow = MagicMock()

    result = await worker._process_document(payload, context)

    worker.logger.info.assert_any_call("Processing file_path: %s", payload.file_path)
    worker.logger.info.assert_any_call(
        "Processing document revision %s (source_id=%s, file_name=%s)",
        payload.revision_id,
        "source2",
        "file",
    )
    mock_revision_pipeline.assert_called_once_with(customer_name="custB")
    mock_to_thread.assert_called_once()
    worker._update_doc_request.assert_called_once()
    assert result["success"] is False
    assert result["message"] == "error occurred"
    assert result["completion_payload"] == completion_mock
    assert result["processing_failed"] is True
    assert result["document_revision_dict"]["revision_id"] == "rev2"
    assert result["document_revision_dict"]["file_name"] == "file"
    assert result["document_revision_dict"]["file_type"] == "pdf"

@pytest.mark.asyncio
async def test_process_document_invalid_file_path_format():
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    worker._update_doc_request = MagicMock()
    payload = ProcessDocumentPayload(
        file_path="invalidpath",
        revision_id="rev3",
        customer_name="custC",
        old_rev_id=None,
        old_rev_file_name=None,
        is_new=True,
        job_id="job789",
    )
    context = Context()

    result = await worker._process_document(payload, context)

    worker.logger.info.assert_called_once_with("Processing file_path: %s", payload.file_path)
    worker.logger.error.assert_called_once()
    assert result["success"] is False
    assert "Invalid file path format" in result["message"]
    assert result["revision_id"] == "rev3"

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor")
async def test_process_document_status_not_str_converted(
    mock_document_processor, mock_to_thread
):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    worker._update_doc_request = MagicMock()
    class StatusEnum:
        value = "completed"
    payload = ProcessDocumentPayload(
        file_path="source3/rev3/file.csv",
        revision_id="rev3",
        customer_name="custD",
        old_rev_id=None,
        old_rev_file_name=None,
        is_new=True,
        job_id="job101",
    )
    context = Context()
    completion_mock = MagicMock()
    completion_mock.status = "completed"
    completion_mock.error_message = ""
    mock_to_thread.return_value = completion_mock
    mock_document_processor.return_value.run_complete_workflow = MagicMock()

    def model_dump_mock():
        return {
            "revision_id": "rev3",
            "status": StatusEnum(),
            "file_name": "file",
            "file_type": "csv",
        }
    # Patch DocumentRevision.model_dump to return dict with status as enum
    with patch(
        "workers.document_processing_worker.doc_processing_worker.DocumentRevision.model_dump",
        new=model_dump_mock,
    ):
        result = await worker._process_document(payload, context)

    worker._update_doc_request.assert_called_once()
    assert result["success"] is True
    assert result["document_revision_dict"]["status"] == "completed"

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor")
async def test_process_document_update_doc_request_raises(
    mock_document_processor, mock_to_thread
):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    def raise_exc(*args, **kwargs):
        raise RuntimeError("update failed")
    worker._update_doc_request = MagicMock(side_effect=raise_exc)
    payload = ProcessDocumentPayload(
        file_path="source4/rev4/file.docx",
        revision_id="rev4",
        customer_name="custE",
        old_rev_id=None,
        old_rev_file_name=None,
        is_new=True,
        job_id="job202",
    )
    context = Context()
    completion_mock = MagicMock()
    completion_mock.status = "completed"
    completion_mock.error_message = ""
    mock_to_thread.return_value = completion_mock
    mock_document_processor.return_value.run_complete_workflow = MagicMock()

    with pytest.raises(RuntimeError, match="update failed"):
        await worker._process_document(payload, context)

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor")
async def test_process_document_completion_error_message_none(
    mock_document_processor, mock_to_thread
):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    worker._update_doc_request = MagicMock()
    payload = ProcessDocumentPayload(
        file_path="source5/rev5/file.json",
        revision_id="rev5",
        customer_name="custF",
        old_rev_id=None,
        old_rev_file_name=None,
        is_new=True,
        job_id="job303",
    )
    context = Context()
    completion_mock = MagicMock()
    completion_mock.status = "completed"
    completion_mock.error_message = None
    mock_to_thread.return_value = completion_mock
    mock_document_processor.return_value.run_complete_workflow = MagicMock()

    result = await worker._process_document(payload, context)

    assert result["message"] == ""
    assert result["success"] is True
    assert result["processing_failed"] is False

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor")
async def test_process_document_file_name_without_extension(
    mock_document_processor, mock_to_thread
):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    worker._update_doc_request = MagicMock()
    payload = ProcessDocumentPayload(
        file_path="source6/rev6/filewithoutextension",
        revision_id="rev6",
        customer_name="custG",
        old_rev_id=None,
        old_rev_file_name=None,
        is_new=True,
        job_id="job404",
    )
    context = Context()
    completion_mock = MagicMock()
    completion_mock.status = "completed"
    completion_mock.error_message = ""
    mock_to_thread.return_value = completion_mock
    mock_document_processor.return_value.run_complete_workflow = MagicMock()

    result = await worker._process_document(payload, context)

    assert result["document_revision_dict"]["file_name"] == "filewithoutextension"
    assert result["document_revision_dict"]["file_type"] is None
    assert result["success"] is True

@pytest.mark.asyncio
@patch("workers.document_processing_worker.doc_processing_worker.asyncio.to_thread")
@patch("workers.document_processing_worker.doc_processing_worker.DocumentProcessor")
async def test_process_document_exception_in_to_thread(
    mock_document_processor, mock_to_thread
):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    worker._update_doc_request = MagicMock()
    payload = ProcessDocumentPayload(
        file_path="source7/rev7/file.txt",
        revision_id="rev7",
        customer_name="custH",
        old_rev_id=None,
        old_rev_file_name=None,
        is_new=True,
        job_id="job505",
    )
    context = Context()
    mock_to_thread.side_effect = RuntimeError("thread failure")
    mock_document_processor.return_value.run_complete_workflow = MagicMock()

    result = await worker._process_document(payload, context)

    worker.logger.error.assert_called_once()
    assert result["success"] is False
    assert "thread failure" in result["message"]
    assert result["revision_id"] == "rev7"
# AI_TEST_AGENT_END function=DocumentProcessingWorker._process_document

# AI_TEST_AGENT_START function=DocumentProcessingWorker._update_doc_request
def test_update_doc_request_no_update_url(monkeypatch):
    worker = DocumentProcessingWorker()
    document_revision = MagicMock()
    context = MagicMock()
    monkeypatch.delenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL", raising=False)
    result = worker._update_doc_request(document_revision, context)
    assert result is None

@patch("workers.document_processing_worker.doc_processing_worker.httpx.Client")
def test_update_doc_request_successful_post(mock_client_class, monkeypatch):
    worker = DocumentProcessingWorker()
    document_revision = MagicMock()
    context = MagicMock()
    monkeypatch.setenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL", "http://example.com/update")
    monkeypatch.setenv("SECRET_KEY", "secret123")
    monkeypatch.setenv("CALLBACK_TIMEOUT", "1000")

    document_revision.get.side_effect = lambda k: {
        "policy_effective_date": "01/01/2020",
        "policy_end_date": "12/31/2020",
        "category": "cat1",
        "category_confidence_score": 0.95,
        "prior_auth_required": True,
        "num_of_pages": 5,
    }.get(k, None)
    context.job_id = "job-123"

    mock_client = MagicMock()
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response

    with patch.object(worker, "_convert_date_to_iso", side_effect=lambda d: "2020-01-01T00:00:00.000Z" if d == "01/01/2020" else "2020-12-31T00:00:00.000Z"):
        worker._update_doc_request(document_revision, context)

    expected_payload = {
        "job_id": "job-123",
        "category": "cat1",
        "category_confidence": 0.95,
        "effective_date": "2020-01-01T00:00:00.000Z",
        "end_date": "2020-12-31T00:00:00.000Z",
        "is_pre_auth": True,
        "number_of_pages": 5,
    }
    expected_headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "x-secret-key": "secret123",
    }
    mock_client.post.assert_called_once_with("http://example.com/update", headers=expected_headers, json=expected_payload)
    mock_response.raise_for_status.assert_called_once()

@patch("workers.document_processing_worker.doc_processing_worker.httpx.Client")
def test_update_doc_request_no_secret_key_header(mock_client_class, monkeypatch):
    worker = DocumentProcessingWorker()
    document_revision = MagicMock()
    context = MagicMock()
    monkeypatch.setenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL", "http://example.com/update")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    monkeypatch.setenv("CALLBACK_TIMEOUT", "2000")

    document_revision.get.side_effect = lambda k: {
        "policy_effective_date": None,
        "policy_end_date": None,
        "category": None,
        "category_confidence_score": None,
        "prior_auth_required": None,
        "num_of_pages": None,
    }.get(k, None)
    context.job_id = "job-456"

    mock_client = MagicMock()
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response

    with patch.object(worker, "_convert_date_to_iso", side_effect=lambda d: d):
        worker._update_doc_request(document_revision, context)

    expected_payload = {
        "job_id": "job-456",
        "category": None,
        "category_confidence": None,
        "effective_date": None,
        "end_date": None,
        "is_pre_auth": None,
        "number_of_pages": None,
    }
    expected_headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    mock_client.post.assert_called_once_with("http://example.com/update", headers=expected_headers, json=expected_payload)
    mock_response.raise_for_status.assert_called_once()

@patch("workers.document_processing_worker.doc_processing_worker.httpx.Client")
def test_update_doc_request_post_raises_logs_error(mock_client_class, monkeypatch):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    context = MagicMock()
    monkeypatch.setenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL", "http://example.com/update")
    monkeypatch.setenv("SECRET_KEY", "secret123")
    monkeypatch.setenv("CALLBACK_TIMEOUT", "3000")

    document_revision.get.side_effect = lambda k: {
        "policy_effective_date": "01/01/2020",
        "policy_end_date": "12/31/2020",
        "category": "cat1",
        "category_confidence_score": 0.95,
        "prior_auth_required": True,
        "num_of_pages": 5,
    }.get(k, None)
    context.job_id = "job-789"

    mock_client = MagicMock()
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_client.post.side_effect = Exception("Network error")

    with patch.object(worker, "_convert_date_to_iso", side_effect=lambda d: "2020-01-01T00:00:00.000Z" if d == "01/01/2020" else "2020-12-31T00:00:00.000Z"):
        worker._update_doc_request(document_revision, context)

    worker.logger.error.assert_called_once()
    args, kwargs = worker.logger.error.call_args
    assert "Failed to send document processing update to" in args[0]
    assert "http://example.com/update" in args[1]
    assert isinstance(args[2], Exception)

@patch("workers.document_processing_worker.doc_processing_worker.httpx.Client")
def test_update_doc_request_post_raises_on_raise_for_status_logs_error(mock_client_class, monkeypatch):
    worker = DocumentProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    context = MagicMock()
    monkeypatch.setenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL", "http://example.com/update")
    monkeypatch.setenv("SECRET_KEY", "secret123")
    monkeypatch.setenv("CALLBACK_TIMEOUT", "3000")

    document_revision.get.side_effect = lambda k: {
        "policy_effective_date": "01/01/2020",
        "policy_end_date": "12/31/2020",
        "category": "cat1",
        "category_confidence_score": 0.95,
        "prior_auth_required": True,
        "num_of_pages": 5,
    }.get(k, None)
    context.job_id = "job-101"

    mock_client = MagicMock()
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("HTTP error")
    mock_client.post.return_value = mock_response

    with patch.object(worker, "_convert_date_to_iso", side_effect=lambda d: "2020-01-01T00:00:00.000Z" if d == "01/01/2020" else "2020-12-31T00:00:00.000Z"):
        worker._update_doc_request(document_revision, context)

    worker.logger.error.assert_called_once()
    args, kwargs = worker.logger.error.call_args
    assert "Failed to send document processing update to" in args[0]
    assert "http://example.com/update" in args[1]
    assert isinstance(args[2], Exception)

@patch("workers.document_processing_worker.doc_processing_worker.httpx.Client")
def test_update_doc_request_effective_and_end_date_none_passed_through(mock_client_class, monkeypatch):
    worker = DocumentProcessingWorker()
    document_revision = MagicMock()
    context = MagicMock()
    monkeypatch.setenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL", "http://example.com/update")
    monkeypatch.setenv("SECRET_KEY", "secret123")
    monkeypatch.setenv("CALLBACK_TIMEOUT", "3000")

    document_revision.get.side_effect = lambda k: {
        "policy_effective_date": None,
        "policy_end_date": None,
        "category": "cat2",
        "category_confidence_score": 0.5,
        "prior_auth_required": False,
        "num_of_pages": 10,
    }.get(k, None)
    context.job_id = "job-202"

    mock_client = MagicMock()
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response

    with patch.object(worker, "_convert_date_to_iso") as mock_convert:
        worker._update_doc_request(document_revision, context)
        mock_convert.assert_not_called()

    expected_payload = {
        "job_id": "job-202",
        "category": "cat2",
        "category_confidence": 0.5,
        "effective_date": None,
        "end_date": None,
        "is_pre_auth": False,
        "number_of_pages": 10,
    }
    expected_headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "x-secret-key": "secret123",
    }
    mock_client.post.assert_called_once_with("http://example.com/update", headers=expected_headers, json=expected_payload)
    mock_response.raise_for_status.assert_called_once()

@patch("workers.document_processing_worker.doc_processing_worker.httpx.Client")
def test_update_doc_request_convert_date_to_iso_returns_original_on_value_error(mock_client_class, monkeypatch):
    worker = DocumentProcessingWorker()
    document_revision = MagicMock()
    context = MagicMock()
    monkeypatch.setenv("DOCUMENT_PROCESSING_UPDATE_DOC_URL", "http://example.com/update")
    monkeypatch.setenv("SECRET_KEY", "secret123")
    monkeypatch.setenv("CALLBACK_TIMEOUT", "3000")

    bad_date = "invalid-date"
    document_revision.get.side_effect = lambda k: {
        "policy_effective_date": bad_date,
        "policy_end_date": bad_date,
        "category": "cat3",
        "category_confidence_score": 0.75,
        "prior_auth_required": True,
        "num_of_pages": 3,
    }.get(k, None)
    context.job_id = "job-303"

    mock_client = MagicMock()
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response

    with patch.object(worker, "_convert_date_to_iso", side_effect=lambda d: d):
        worker._update_doc_request(document_revision, context)

    expected_payload = {
        "job_id": "job-303",
        "category": "cat3",
        "category_confidence": 0.75,
        "effective_date": bad_date,
        "end_date": bad_date,
        "is_pre_auth": True,
        "number_of_pages": 3,
    }
    expected_headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "x-secret-key": "secret123",
    }
    mock_client.post.assert_called_once_with("http://example.com/update", headers=expected_headers, json=expected_payload)
    mock_response.raise_for_status.assert_called_once()
# AI_TEST_AGENT_END function=DocumentProcessingWorker._update_doc_request

# AI_TEST_AGENT_START function=DocumentProcessingWorker._create_callback_data
def test_create_callback_data_with_completion_payload_and_message_data():
    worker = DocumentProcessingWorker()
    job_id = "job123"
    status = "completed"
    message = "All good"
    mock_payload = MagicMock()
    mock_payload.model_dump.return_value = {
        "guidelines": ["guideline1"],
        "linked_documents": ["doc1"],
        "error_message": "error from payload",
        "processed_at": "2023-01-01T00:00:00+00:00",
        "doc_diff": {"diff": "some diff"},
        "complexity_score": 5,
        "complexity_level": "high",
        "complexity_breakdown": {"detail": "info"},
        "coding_section": "section1",
    }
    message_data = {"extra": "data"}

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
        completion_payload=mock_payload,
        message_data=message_data,
    )

    assert result["job_id"] == job_id
    assert result["guidelines"] == ["guideline1"]
    assert result["linked_documents"] == ["doc1"]
    assert result["status"] == status
    assert result["error_message"] == message
    assert result["processed_at"] == "2023-01-01T00:00:00+00:00"
    assert result["doc_diff"] == {"diff": "some diff"}
    assert result["complexity_score"] == 5
    assert result["complexity"] == "high"
    assert result["complexity_details"] == {"detail": "info"}
    assert result["coding_section"] == "section1"
    assert result["raw_payload"] == message_data
    mock_payload.model_dump.assert_called_once()


def test_create_callback_data_with_document_revision_dict_and_no_message_data(monkeypatch):
    worker = DocumentProcessingWorker()
    job_id = "job456"
    status = "failed"
    message = "Failure message"
    document_revision_dict = {
        "guidelines": ["guidelineA"],
        "related_policies": ["policy1", "policy2"],
        "doc_diff": {"diff": "revision diff"},
        "complexity_score": 3,
        "complexity_level": "medium",
        "complexity_breakdown": {"breakdown": "details"},
        "required_codes": "code_section",
    }

    # Patch datetime to control processed_at value
    fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime", datetime)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime.now", lambda tz=None: fixed_time)

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
        document_revision_dict=document_revision_dict,
    )

    assert result["job_id"] == job_id
    assert result["guidelines"] == ["guidelineA"]
    assert result["linked_documents"] == ["policy1", "policy2"]
    assert result["status"] == status
    assert result["error_message"] == message
    assert result["processed_at"] == fixed_time.isoformat()
    assert result["doc_diff"] == {"diff": "revision diff"}
    assert result["complexity_score"] == 3
    assert result["complexity"] == "medium"
    assert result["complexity_details"] == {"breakdown": "details"}
    assert result["coding_section"] == "code_section"
    assert "raw_payload" not in result


def test_create_callback_data_with_neither_completion_payload_nor_document_revision_dict(monkeypatch):
    worker = DocumentProcessingWorker()
    job_id = "job789"
    status = "pending"
    message = "No data"

    fixed_time = datetime(2024, 6, 1, 15, 30, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime", datetime)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime.now", lambda tz=None: fixed_time)

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
    )

    assert result["job_id"] == job_id
    assert result["guidelines"] == []
    assert result["linked_documents"] == []
    assert result["status"] == status
    assert result["error_message"] == message
    assert result["processed_at"] == fixed_time.isoformat()
    assert "doc_diff" not in result
    assert "complexity_score" not in result
    assert "complexity" not in result
    assert "complexity_details" not in result
    assert "coding_section" not in result
    assert "raw_payload" not in result


def test_create_callback_data_completion_payload_error_message_used_when_message_empty():
    worker = DocumentProcessingWorker()
    job_id = "job000"
    status = "error"
    message = ""
    mock_payload = MagicMock()
    mock_payload.model_dump.return_value = {
        "error_message": "payload error",
        "guidelines": [],
        "linked_documents": [],
        "processed_at": "2023-05-05T05:05:05+00:00",
    }

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
        completion_payload=mock_payload,
    )

    assert result["error_message"] == "payload error"
    assert result["processed_at"] == "2023-05-05T05:05:05+00:00"
    mock_payload.model_dump.assert_called_once()


def test_create_callback_data_document_revision_dict_missing_keys(monkeypatch):
    worker = DocumentProcessingWorker()
    job_id = "job111"
    status = "done"
    message = "done message"
    document_revision_dict = {}

    fixed_time = datetime(2024, 6, 2, 10, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime", datetime)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime.now", lambda tz=None: fixed_time)

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
        document_revision_dict=document_revision_dict,
    )

    assert result["guidelines"] == []
    assert result["linked_documents"] == []
    assert result["doc_diff"] is None
    assert result["complexity_score"] is None
    assert result["complexity"] is None
    assert result["complexity_details"] is None
    assert result["coding_section"] is None
    assert result["processed_at"] == fixed_time.isoformat()


def test_create_callback_data_message_data_none_does_not_add_raw_payload():
    worker = DocumentProcessingWorker()
    job_id = "job222"
    status = "ok"
    message = "msg"
    mock_payload = MagicMock()
    mock_payload.model_dump.return_value = {
        "guidelines": [],
        "linked_documents": [],
        "error_message": "",
        "processed_at": "2023-07-07T07:07:07+00:00",
    }

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
        completion_payload=mock_payload,
        message_data=None,
    )

    assert "raw_payload" not in result


def test_create_callback_data_invalid_types_for_message_and_status(monkeypatch):
    worker = DocumentProcessingWorker()
    job_id = "job333"
    status = None
    message = None
    document_revision_dict = {
        "guidelines": None,
        "related_policies": None,
        "doc_diff": None,
        "complexity_score": None,
        "complexity_level": None,
        "complexity_breakdown": None,
        "required_codes": None,
    }

    fixed_time = datetime(2024, 6, 3, 11, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime", datetime)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime.now", lambda tz=None: fixed_time)

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
        document_revision_dict=document_revision_dict,
    )

    assert result["status"] is None
    assert result["error_message"] is None
    assert result["guidelines"] is None
    assert result["linked_documents"] is None
    assert result["processed_at"] == fixed_time.isoformat()


def test_create_callback_data_completion_payload_missing_keys(monkeypatch):
    worker = DocumentProcessingWorker()
    job_id = "job444"
    status = "processing"
    message = "processing message"
    mock_payload = MagicMock()
    mock_payload.model_dump.return_value = {}

    fixed_time = datetime(2024, 6, 4, 9, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime", datetime)
    monkeypatch.setattr("workers.document_processing_worker.doc_processing_worker.datetime.now", lambda tz=None: fixed_time)

    result = worker._create_callback_data(
        job_id=job_id,
        status=status,
        message=message,
        completion_payload=mock_payload,
    )

    assert result["guidelines"] == []
    assert result["linked_documents"] == []
    assert result["error_message"] == message
    assert result["processed_at"] == fixed_time.isoformat()
    assert result["doc_diff"] is None
    assert result["complexity_score"] is None
    assert result["complexity"] is None
    assert result["complexity_details"] is None
    assert result["coding_section"] is None
    mock_payload.model_dump.assert_called_once()
# AI_TEST_AGENT_END function=DocumentProcessingWorker._create_callback_data
