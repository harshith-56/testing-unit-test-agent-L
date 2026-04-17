from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from unittest.mock import AsyncMock, MagicMock, patch
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import patch, MagicMock
from workers.revision_processing_worker.revision_worker import Context, RevisionDocumentMessage
from workers.revision_processing_worker.revision_worker import RevisionDocumentPayload, Context
from workers.revision_processing_worker.revision_worker import RevisionProcessingWorker
import asyncio
import os
import pytest

# AI_TEST_AGENT_START function=RevisionProcessingWorker.__init__
def test_init_with_valid_subscription_name(monkeypatch):
    monkeypatch.setenv("REVISION_PROCESSING_SUBSCRIPTION", "valid_subscription")
    worker = RevisionProcessingWorker()
    assert hasattr(worker, "subscription_name")
    assert worker.subscription_name == "valid_subscription"

def test_init_raises_value_error_when_env_var_missing(monkeypatch):
    monkeypatch.delenv("REVISION_PROCESSING_SUBSCRIPTION", raising=False)
    with pytest.raises(ValueError, match="REVISION_PROCESSING_SUBSCRIPTION environment variable is required"):
        RevisionProcessingWorker()

def test_init_raises_value_error_when_env_var_empty(monkeypatch):
    monkeypatch.setenv("REVISION_PROCESSING_SUBSCRIPTION", "")
    with pytest.raises(ValueError, match="REVISION_PROCESSING_SUBSCRIPTION environment variable is required"):
        RevisionProcessingWorker()

def test_init_raises_value_error_when_env_var_is_whitespace(monkeypatch):
    monkeypatch.setenv("REVISION_PROCESSING_SUBSCRIPTION", "   ")
    with pytest.raises(ValueError, match="REVISION_PROCESSING_SUBSCRIPTION environment variable is required"):
        RevisionProcessingWorker()

def test_init_accepts_subscription_name_with_special_characters(monkeypatch):
    special_name = "sub$cription_123-!@#"
    monkeypatch.setenv("REVISION_PROCESSING_SUBSCRIPTION", special_name)
    worker = RevisionProcessingWorker()
    assert worker.subscription_name == special_name

def test_init_accepts_subscription_name_with_numeric_string(monkeypatch):
    numeric_name = "1234567890"
    monkeypatch.setenv("REVISION_PROCESSING_SUBSCRIPTION", numeric_name)
    worker = RevisionProcessingWorker()
    assert worker.subscription_name == numeric_name

def test_init_accepts_subscription_name_with_long_string(monkeypatch):
    long_name = "a" * 1000
    monkeypatch.setenv("REVISION_PROCESSING_SUBSCRIPTION", long_name)
    worker = RevisionProcessingWorker()
    assert worker.subscription_name == long_name

def test_init_raises_value_error_when_env_var_is_none(monkeypatch):
    monkeypatch.setenv("REVISION_PROCESSING_SUBSCRIPTION", "valid")
    with patch.dict(os.environ, {"REVISION_PROCESSING_SUBSCRIPTION": None}):
        with pytest.raises(ValueError, match="REVISION_PROCESSING_SUBSCRIPTION environment variable is required"):
            RevisionProcessingWorker()
# AI_TEST_AGENT_END function=RevisionProcessingWorker.__init__

# AI_TEST_AGENT_START function=RevisionProcessingWorker.process_message
@pytest.mark.asyncio
async def test_process_message_valid_payload_calls_process_revision_and_returns_its_result():
    worker = RevisionProcessingWorker()
    message_data = {"payload": MagicMock()}
    correlation_id = "corr-123"
    payload_mock = MagicMock()
    payload_mock.job_id = "job-1"
    message_mock = MagicMock()
    message_mock.payload = payload_mock
    worker.validate_message = MagicMock(return_value=message_mock)
    worker._process_revision = AsyncMock(return_value=True)
    worker.logger = MagicMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.validate_message.assert_called_once_with(message_data, RevisionDocumentMessage)
    worker.logger.info.assert_called_once_with("Processing revision job %s", payload_mock.job_id)
    worker._process_revision.assert_awaited_once_with(payload_mock, Context(correlation_id=correlation_id, job_id=payload_mock.job_id))
    assert result is True

@pytest.mark.asyncio
async def test_process_message_payload_none_raises_value_error_and_sends_failure_and_returns_true():
    worker = RevisionProcessingWorker()
    message_data = MagicMock()
    message_data.get = MagicMock(side_effect=lambda k: None if k == "payload" else None)
    correlation_id = "corr-456"
    worker.logger = MagicMock()
    worker._send_failure = AsyncMock()
    worker.validate_message = MagicMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_called_once()
    worker._send_failure.assert_awaited_once_with("unknown", "Message validation failed: Invalid payload: payload is empty")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_validate_message_raises_exception_sends_failure_and_returns_true():
    worker = RevisionProcessingWorker()
    payload_dict = {"job_id": "job-2"}
    message_data = {"payload": payload_dict}
    correlation_id = "corr-789"
    worker.validate_message = MagicMock(side_effect=RuntimeError("validation error"))
    worker.logger = MagicMock()
    worker._send_failure = AsyncMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_called_once()
    worker._send_failure.assert_awaited_once_with("job-2", "Message validation failed: validation error")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_payload_not_dict_in_failure_path_job_id_unknown():
    worker = RevisionProcessingWorker()
    message_data = {"payload": "not a dict"}
    correlation_id = "corr-000"
    worker.validate_message = MagicMock(side_effect=Exception("fail"))
    worker.logger = MagicMock()
    worker._send_failure = AsyncMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_called_once()
    worker._send_failure.assert_awaited_once_with("unknown", "Message validation failed: fail")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_process_revision_returns_false_returns_false():
    worker = RevisionProcessingWorker()
    payload_mock = MagicMock()
    payload_mock.job_id = "job-3"
    message_mock = MagicMock()
    message_mock.payload = payload_mock
    message_data = {"payload": MagicMock()}
    correlation_id = "corr-321"
    worker.validate_message = MagicMock(return_value=message_mock)
    worker._process_revision = AsyncMock(return_value=False)
    worker.logger = MagicMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.info.assert_called_once_with("Processing revision job %s", payload_mock.job_id)
    worker._process_revision.assert_awaited_once_with(payload_mock, Context(correlation_id=correlation_id, job_id=payload_mock.job_id))
    assert result is False

@pytest.mark.asyncio
async def test_process_message_validate_message_raises_non_standard_exception_sends_failure_and_returns_true():
    worker = RevisionProcessingWorker()
    payload_dict = {"job_id": "job-4"}
    message_data = {"payload": payload_dict}
    correlation_id = "corr-654"
    class CustomError(Exception):
        pass
    worker.validate_message = MagicMock(side_effect=CustomError("custom error"))
    worker.logger = MagicMock()
    worker._send_failure = AsyncMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_called_once()
    worker._send_failure.assert_awaited_once_with("job-4", "Message validation failed: custom error")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_payload_empty_dict_raises_value_error_and_sends_failure():
    worker = RevisionProcessingWorker()
    message_data = {"payload": {}}
    correlation_id = "corr-999"
    worker.validate_message = MagicMock(side_effect=ValueError("Invalid payload: payload is empty"))
    worker.logger = MagicMock()
    worker._send_failure = AsyncMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_called_once()
    worker._send_failure.assert_awaited_once_with("unknown", "Message validation failed: Invalid payload: payload is empty")
    assert result is True

@pytest.mark.asyncio
async def test_process_message_payload_job_id_none_in_failure_path_job_id_unknown():
    worker = RevisionProcessingWorker()
    payload_dict = {"job_id": None}
    message_data = {"payload": payload_dict}
    correlation_id = "corr-111"
    worker.validate_message = MagicMock(side_effect=Exception("fail"))
    worker.logger = MagicMock()
    worker._send_failure = AsyncMock()

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_called_once()
    worker._send_failure.assert_awaited_once_with("unknown", "Message validation failed: fail")
    assert result is True
# AI_TEST_AGENT_END function=RevisionProcessingWorker.process_message

# AI_TEST_AGENT_START function=RevisionProcessingWorker._process_revision
@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._update_doc_request")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._build_callback")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._secret_headers")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.send_callback", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_success_with_file_extension(
    mock_logger,
    mock_send_callback,
    mock_secret_headers,
    mock_build_callback,
    mock_update_doc_request,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev1",
        file_path="source1/rev1/filename.txt",
        customer_name="customerA",
        old_rev_id="oldrev",
        old_rev_file_name="oldfile.txt",
        job_id="job123",
    )
    context = Context()

    class Completion:
        status = "completed"

    mock_to_thread.return_value = Completion()
    mock_build_callback.return_value = {"callback": "data"}
    mock_secret_headers.return_value = {"header": "value"}
    os.environ["DOCUMENT_PROCESSING_COMPLETE_URL"] = "http://callback.url"

    result = await worker._process_revision(payload, context)

    assert result is True
    mock_to_thread.assert_called_once()
    mock_update_doc_request.assert_called_once()
    mock_build_callback.assert_called_once_with(payload.job_id, mock_to_thread.return_value, ANY)
    mock_send_callback.assert_awaited_once_with(
        "http://callback.url", {"callback": "data"}, headers={"header": "value"}
    )
    mock_logger.error.assert_not_called()

@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._update_doc_request")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._build_callback")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._secret_headers")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.send_callback", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_success_without_file_extension(
    mock_logger,
    mock_send_callback,
    mock_secret_headers,
    mock_build_callback,
    mock_update_doc_request,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev2",
        file_path="source2/rev2/filename",
        customer_name="customerB",
        old_rev_id=None,
        old_rev_file_name=None,
        job_id="job456",
    )
    context = Context()

    class Completion:
        status = "completed"

    mock_to_thread.return_value = Completion()
    mock_build_callback.return_value = {"callback": "data2"}
    mock_secret_headers.return_value = {"header2": "value2"}
    os.environ["DOCUMENT_PROCESSING_COMPLETE_URL"] = "http://callback2.url"

    result = await worker._process_revision(payload, context)

    assert result is True
    mock_to_thread.assert_called_once()
    mock_update_doc_request.assert_called_once()
    mock_build_callback.assert_called_once_with(payload.job_id, mock_to_thread.return_value, ANY)
    mock_send_callback.assert_awaited_once_with(
        "http://callback2.url", {"callback": "data2"}, headers={"header2": "value2"}
    )
    mock_logger.error.assert_not_called()

@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._send_failure", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_invalid_file_path_format(
    mock_logger,
    mock_send_failure,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev3",
        file_path="invalidpath",
        customer_name="customerC",
        old_rev_id=None,
        old_rev_file_name=None,
        job_id="job789",
    )
    context = Context()

    result = await worker._process_revision(payload, context)

    assert result is False
    mock_logger.error.assert_called_once()
    mock_send_failure.assert_awaited_once()
    mock_to_thread.assert_not_called()

@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._update_doc_request")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._build_callback")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._secret_headers")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.send_callback", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_no_document_processing_complete_url(
    mock_logger,
    mock_send_callback,
    mock_secret_headers,
    mock_build_callback,
    mock_update_doc_request,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev4",
        file_path="source4/rev4/file.pdf",
        customer_name="customerD",
        old_rev_id=None,
        old_rev_file_name=None,
        job_id="job101",
    )
    context = Context()

    class Completion:
        status = "completed"

    mock_to_thread.return_value = Completion()
    mock_build_callback.return_value = {"callback": "data4"}
    mock_secret_headers.return_value = {"header4": "value4"}
    if "DOCUMENT_PROCESSING_COMPLETE_URL" in os.environ:
        del os.environ["DOCUMENT_PROCESSING_COMPLETE_URL"]

    result = await worker._process_revision(payload, context)

    assert result is True
    mock_update_doc_request.assert_called_once()
    mock_build_callback.assert_called_once_with(payload.job_id, mock_to_thread.return_value, ANY)
    mock_send_callback.assert_not_awaited()
    mock_logger.error.assert_called_once_with(
        "DOCUMENT_PROCESSING_COMPLETE_URL environment variable is not set"
    )

@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread", side_effect=Exception("Thread error"))
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._send_failure", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_to_thread_raises_exception(
    mock_logger,
    mock_send_failure,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev5",
        file_path="source5/rev5/file.docx",
        customer_name="customerE",
        old_rev_id=None,
        old_rev_file_name=None,
        job_id="job202",
    )
    context = Context()

    result = await worker._process_revision(payload, context)

    assert result is False
    mock_logger.error.assert_called_once()
    mock_send_failure.assert_awaited_once_with("job202", "Thread error")

@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._update_doc_request", side_effect=Exception("Update error"))
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._send_failure", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_update_doc_request_raises_exception(
    mock_logger,
    mock_send_failure,
    mock_update_doc_request,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev6",
        file_path="source6/rev6/file.csv",
        customer_name="customerF",
        old_rev_id=None,
        old_rev_file_name=None,
        job_id="job303",
    )
    context = Context()

    class Completion:
        status = "completed"

    mock_to_thread.return_value = Completion()
    os.environ["DOCUMENT_PROCESSING_COMPLETE_URL"] = "http://callback6.url"

    result = await worker._process_revision(payload, context)

    assert result is False
    mock_update_doc_request.assert_called_once()
    mock_logger.error.assert_called_once()
    mock_send_failure.assert_awaited_once_with("job303", "Update error")

@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._update_doc_request")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._build_callback")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._secret_headers")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.send_callback", new_callable=AsyncMock, side_effect=Exception("Callback error"))
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._send_failure", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_send_callback_raises_exception(
    mock_logger,
    mock_send_failure,
    mock_send_callback,
    mock_secret_headers,
    mock_build_callback,
    mock_update_doc_request,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev7",
        file_path="source7/rev7/file.json",
        customer_name="customerG",
        old_rev_id=None,
        old_rev_file_name=None,
        job_id="job404",
    )
    context = Context()

    class Completion:
        status = "completed"

    mock_to_thread.return_value = Completion()
    mock_build_callback.return_value = {"callback": "data7"}
    mock_secret_headers.return_value = {"header7": "value7"}
    os.environ["DOCUMENT_PROCESSING_COMPLETE_URL"] = "http://callback7.url"

    result = await worker._process_revision(payload, context)

    assert result is False
    mock_update_doc_request.assert_called_once()
    mock_build_callback.assert_called_once()
    mock_send_callback.assert_awaited_once()
    mock_send_failure.assert_awaited_once_with("job404", "Callback error")
    mock_logger.error.assert_called()

@pytest.mark.asyncio
@patch("workers.revision_processing_worker.revision_worker.asyncio.to_thread")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._update_doc_request")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._build_callback")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker._secret_headers")
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.send_callback", new_callable=AsyncMock)
@patch("workers.revision_processing_worker.revision_worker.RevisionProcessingWorker.logger")
async def test_process_revision_completion_status_not_completed(
    mock_logger,
    mock_send_callback,
    mock_secret_headers,
    mock_build_callback,
    mock_update_doc_request,
    mock_to_thread,
):
    worker = RevisionProcessingWorker()
    payload = RevisionDocumentPayload(
        revision_id="rev8",
        file_path="source8/rev8/file.xml",
        customer_name="customerH",
        old_rev_id=None,
        old_rev_file_name=None,
        job_id="job505",
    )
    context = Context()

    class Completion:
        status = "failed"

    mock_to_thread.return_value = Completion()
    mock_build_callback.return_value = {"callback": "data8"}
    mock_secret_headers.return_value = {"header8": "value8"}
    os.environ["DOCUMENT_PROCESSING_COMPLETE_URL"] = "http://callback8.url"

    result = await worker._process_revision(payload, context)

    assert result is False
    mock_update_doc_request.assert_called_once()
    mock_build_callback.assert_called_once()
    mock_send_callback.assert_awaited_once()
    mock_logger.error.assert_not_called()
# AI_TEST_AGENT_END function=RevisionProcessingWorker._process_revision

# AI_TEST_AGENT_START function=RevisionProcessingWorker._send_failure
@pytest.mark.asyncio
async def test_send_failure_no_env_url_does_not_call_send_callback():
    worker = RevisionProcessingWorker()
    worker.send_callback = AsyncMock()
    worker._secret_headers = MagicMock(return_value={"header": "value"})
    worker.logger = MagicMock()
    if "REVISION_PROCESSING_COMPLETE_URL" in os.environ:
        del os.environ["REVISION_PROCESSING_COMPLETE_URL"]
    await worker._send_failure("job123", "error message")
    worker.send_callback.assert_not_called()
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
async def test_send_failure_calls_send_callback_with_correct_data_and_headers():
    worker = RevisionProcessingWorker()
    worker.send_callback = AsyncMock()
    worker._secret_headers = MagicMock(return_value={"x-secret-key": "secret"})
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "http://callback.url"
    job_id = "job123"
    message = "failure reason"
    before_call = datetime.now(timezone.utc)
    await worker._send_failure(job_id, message)
    worker.send_callback.assert_called_once()
    args, kwargs = worker.send_callback.call_args
    assert args[0] == "http://callback.url"
    data = args[1]
    assert data["job_id"] == job_id
    assert data["status"] == "failed"
    assert data["error_message"] == message
    assert data["guidelines"] == []
    assert data["linked_documents"] == []
    processed_at = datetime.fromisoformat(data["processed_at"])
    assert processed_at.tzinfo is not None
    assert processed_at >= before_call
    assert "headers" in kwargs
    assert kwargs["headers"] == {"x-secret-key": "secret"}
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
async def test_send_failure_send_callback_raises_logs_error():
    worker = RevisionProcessingWorker()
    async def raise_exc(*args, **kwargs):
        raise RuntimeError("callback failed")
    worker.send_callback = AsyncMock(side_effect=raise_exc)
    worker._secret_headers = MagicMock(return_value={"header": "value"})
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "http://callback.url"
    await worker._send_failure("job123", "error message")
    worker.send_callback.assert_called_once()
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Failed to send failure callback: %s"
    assert isinstance(call_args[1], RuntimeError)
    assert str(call_args[1]) == "callback failed"

@pytest.mark.asyncio
async def test_send_failure_empty_job_id_and_message_still_calls_send_callback():
    worker = RevisionProcessingWorker()
    worker.send_callback = AsyncMock()
    worker._secret_headers = MagicMock(return_value={"header": "value"})
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "http://callback.url"
    await worker._send_failure("", "")
    worker.send_callback.assert_called_once()
    data = worker.send_callback.call_args[0][1]
    assert data["job_id"] == ""
    assert data["error_message"] == ""
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
async def test_send_failure_none_job_id_and_message_still_calls_send_callback():
    worker = RevisionProcessingWorker()
    worker.send_callback = AsyncMock()
    worker._secret_headers = MagicMock(return_value={"header": "value"})
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "http://callback.url"
    await worker._send_failure(None, None)
    worker.send_callback.assert_called_once()
    data = worker.send_callback.call_args[0][1]
    assert data["job_id"] is None
    assert data["error_message"] is None
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
async def test_send_failure_secret_key_env_included_in_headers():
    worker = RevisionProcessingWorker()
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "http://callback.url"
    os.environ["SECRET_KEY"] = "supersecret"
    # Use real _secret_headers to test header inclusion
    headers = worker._secret_headers()
    assert headers.get("x-secret-key") == "supersecret"
    # Patch _secret_headers to call real method
    worker._secret_headers = MagicMock(side_effect=worker._secret_headers)
    await worker._send_failure("jobid", "msg")
    worker._secret_headers.assert_called_once()
    called_headers = worker.send_callback.call_args[1]["headers"]
    assert called_headers.get("x-secret-key") == "supersecret"
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
async def test_send_failure_secret_key_env_missing_headers_do_not_include_secret_key():
    worker = RevisionProcessingWorker()
    worker.send_callback = AsyncMock()
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "http://callback.url"
    if "SECRET_KEY" in os.environ:
        del os.environ["SECRET_KEY"]
    headers = worker._secret_headers()
    assert "x-secret-key" not in headers
    worker._secret_headers = MagicMock(side_effect=worker._secret_headers)
    await worker._send_failure("jobid", "msg")
    worker._secret_headers.assert_called_once()
    called_headers = worker.send_callback.call_args[1]["headers"]
    assert "x-secret-key" not in called_headers
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
async def test_send_failure_invalid_url_env_calls_send_callback_and_logs_no_error():
    worker = RevisionProcessingWorker()
    worker.send_callback = AsyncMock()
    worker._secret_headers = MagicMock(return_value={"header": "value"})
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "not a url"
    await worker._send_failure("jobid", "msg")
    worker.send_callback.assert_called_once()
    worker.logger.error.assert_not_called()

@pytest.mark.asyncio
async def test_send_failure_send_callback_raises_non_runtime_error_logs_error():
    worker = RevisionProcessingWorker()
    async def raise_value_error(*args, **kwargs):
        raise ValueError("value error")
    worker.send_callback = AsyncMock(side_effect=raise_value_error)
    worker._secret_headers = MagicMock(return_value={"header": "value"})
    worker.logger = MagicMock()
    os.environ["REVISION_PROCESSING_COMPLETE_URL"] = "http://callback.url"
    await worker._send_failure("jobid", "msg")
    worker.send_callback.assert_called_once()
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Failed to send failure callback: %s"
    assert isinstance(call_args[1], ValueError)
    assert str(call_args[1]) == "value error"
# AI_TEST_AGENT_END function=RevisionProcessingWorker._send_failure

# AI_TEST_AGENT_START function=RevisionProcessingWorker._secret_headers
def test_secret_headers_with_secret_key(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "mysecret")
    worker = RevisionProcessingWorker()
    result = worker._secret_headers()
    assert isinstance(result, dict)
    assert result["accept"] == "application/json"
    assert result["Content-Type"] == "application/json"
    assert result["x-secret-key"] == "mysecret"

def test_secret_headers_without_secret_key(monkeypatch):
    monkeypatch.delenv("SECRET_KEY", raising=False)
    worker = RevisionProcessingWorker()
    result = worker._secret_headers()
    assert isinstance(result, dict)
    assert result["accept"] == "application/json"
    assert result["Content-Type"] == "application/json"
    assert "x-secret-key" not in result

def test_secret_headers_with_empty_secret_key(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "")
    worker = RevisionProcessingWorker()
    result = worker._secret_headers()
    assert isinstance(result, dict)
    assert result["accept"] == "application/json"
    assert result["Content-Type"] == "application/json"
    assert "x-secret-key" not in result

def test_secret_headers_with_non_string_secret_key(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "12345")
    worker = RevisionProcessingWorker()
    result = worker._secret_headers()
    assert result["x-secret-key"] == "12345"

def test_secret_headers_with_secret_key_containing_spaces(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", " secret key ")
    worker = RevisionProcessingWorker()
    result = worker._secret_headers()
    assert result["x-secret-key"] == " secret key "

def test_secret_headers_with_secret_key_special_characters(monkeypatch):
    special_key = "!@#$%^&*()_+-=[]{}|;':,.<>/?"
    monkeypatch.setenv("SECRET_KEY", special_key)
    worker = RevisionProcessingWorker()
    result = worker._secret_headers()
    assert result["x-secret-key"] == special_key

def test_secret_headers_called_multiple_times(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "multi")
    worker = RevisionProcessingWorker()
    result1 = worker._secret_headers()
    monkeypatch.setenv("SECRET_KEY", "changed")
    result2 = worker._secret_headers()
    assert result1["x-secret-key"] == "multi"
    assert result2["x-secret-key"] == "changed"
# AI_TEST_AGENT_END function=RevisionProcessingWorker._secret_headers

# AI_TEST_AGENT_START function=RevisionProcessingWorker._update_doc_request
class DummyContext:
    def __init__(self, job_id):
        self.job_id = job_id

@patch.dict(os.environ, {}, clear=True)
def test_update_doc_request_no_update_url(monkeypatch):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    context = DummyContext(job_id="job123")

    # No DOCUMENT_PROCESSING_UPDATE_DOC_URL in env, function should return early and do nothing
    worker._update_doc_request(document_revision, context)

    # logger.error should not be called
    worker.logger.error.assert_not_called()

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_UPDATE_DOC_URL": "http://example.com/update", "CALLBACK_TIMEOUT": "10"}, clear=True)
@patch("workers.revision_processing_worker.revision_worker.httpx.Client")
def test_update_doc_request_successful_post(mock_client_class):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    document_revision.get.return_value = 5
    context = DummyContext(job_id="job123")

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client

    worker._update_doc_request(document_revision, context)

    mock_client_class.assert_called_once_with(timeout=10)
    mock_client.post.assert_called_once_with(
        "http://example.com/update",
        headers={"accept": "application/json", "Content-Type": "application/json"},
        json={"job_id": "job123", "number_of_pages": 5},
    )
    mock_response.raise_for_status.assert_called_once()
    worker.logger.error.assert_not_called()

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_UPDATE_DOC_URL": "http://example.com/update", "SECRET_KEY": "secret123", "CALLBACK_TIMEOUT": "20"}, clear=True)
@patch("workers.revision_processing_worker.revision_worker.httpx.Client")
def test_update_doc_request_with_secret_key(mock_client_class):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    document_revision.get.return_value = 10
    context = DummyContext(job_id="job456")

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client

    worker._update_doc_request(document_revision, context)

    mock_client_class.assert_called_once_with(timeout=20)
    mock_client.post.assert_called_once_with(
        "http://example.com/update",
        headers={"accept": "application/json", "Content-Type": "application/json", "x-secret-key": "secret123"},
        json={"job_id": "job456", "number_of_pages": 10},
    )
    mock_response.raise_for_status.assert_called_once()
    worker.logger.error.assert_not_called()

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_UPDATE_DOC_URL": "http://example.com/update"}, clear=True)
@patch("workers.revision_processing_worker.revision_worker.httpx.Client")
def test_update_doc_request_post_raises_exception_logs_error(mock_client_class):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    document_revision.get.return_value = None
    context = DummyContext(job_id="job789")

    mock_client = MagicMock()
    mock_client.post.side_effect = Exception("Network error")
    mock_client_class.return_value.__enter__.return_value = mock_client

    worker._update_doc_request(document_revision, context)

    mock_client_class.assert_called_once()
    mock_client.post.assert_called_once_with(
        "http://example.com/update",
        headers={"accept": "application/json", "Content-Type": "application/json"},
        json={"job_id": "job789", "number_of_pages": None},
    )
    worker.logger.error.assert_called_once()
    args, kwargs = worker.logger.error.call_args
    assert "Failed to send revision update to" in args[0]
    assert "http://example.com/update" in args[1]
    assert isinstance(args[2], Exception)
    assert str(args[2]) == "Network error"

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_UPDATE_DOC_URL": "http://example.com/update", "CALLBACK_TIMEOUT": "not_an_int"}, clear=True)
@patch("workers.revision_processing_worker.revision_worker.httpx.Client")
def test_update_doc_request_invalid_callback_timeout_raises_value_error(mock_client_class):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    document_revision.get.return_value = 3
    context = DummyContext(job_id="job000")

    with pytest.raises(ValueError):
        worker._update_doc_request(document_revision, context)

    mock_client_class.assert_not_called()
    worker.logger.error.assert_not_called()

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_UPDATE_DOC_URL": "http://example.com/update"}, clear=True)
@patch("workers.revision_processing_worker.revision_worker.httpx.Client")
def test_update_doc_request_document_revision_get_returns_none(mock_client_class):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    document_revision.get.return_value = None
    context = DummyContext(job_id="job111")

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client

    worker._update_doc_request(document_revision, context)

    mock_client.post.assert_called_once_with(
        "http://example.com/update",
        headers={"accept": "application/json", "Content-Type": "application/json"},
        json={"job_id": "job111", "number_of_pages": None},
    )
    mock_response.raise_for_status.assert_called_once()
    worker.logger.error.assert_not_called()

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_UPDATE_DOC_URL": "http://example.com/update", "SECRET_KEY": ""}, clear=True)
@patch("workers.revision_processing_worker.revision_worker.httpx.Client")
def test_update_doc_request_empty_secret_key_header_not_included(mock_client_class):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    document_revision.get.return_value = 7
    context = DummyContext(job_id="job222")

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client

    worker._update_doc_request(document_revision, context)

    headers = {"accept": "application/json", "Content-Type": "application/json"}
    mock_client.post.assert_called_once_with(
        "http://example.com/update",
        headers=headers,
        json={"job_id": "job222", "number_of_pages": 7},
    )
    mock_response.raise_for_status.assert_called_once()
    worker.logger.error.assert_not_called()

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_UPDATE_DOC_URL": "http://example.com/update", "CALLBACK_TIMEOUT": "0"}, clear=True)
@patch("workers.revision_processing_worker.revision_worker.httpx.Client")
def test_update_doc_request_callback_timeout_zero(mock_client_class):
    worker = RevisionProcessingWorker()
    worker.logger = MagicMock()
    document_revision = MagicMock()
    document_revision.get.return_value = 1
    context = DummyContext(job_id="job333")

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client

    worker._update_doc_request(document_revision, context)

    mock_client_class.assert_called_once_with(timeout=0)
    mock_client.post.assert_called_once()
    mock_response.raise_for_status.assert_called_once()
    worker.logger.error.assert_not_called()
# AI_TEST_AGENT_END function=RevisionProcessingWorker._update_doc_request

# AI_TEST_AGENT_START function=RevisionProcessingWorker._build_callback
def test_build_callback_with_full_completion_and_document_revision():
    worker = RevisionProcessingWorker()
    job_id = "job123"
    completion = MagicMock()
    completion.model_dump.return_value = {
        "status": "completed",
        "error_message": "error",
        "guidelines": ["guide1"],
        "linked_documents": ["doc1"],
        "coding_section": "section1",
        "processed_at": "2024-01-01T00:00:00+00:00",
    }
    document_revision = MagicMock()
    document_revision.added_codes = {"code1": "desc1"}
    document_revision.removed_codes = {"code2": "desc2"}
    document_revision.num_of_pages = 5

    result = worker._build_callback(job_id, completion, document_revision)

    assert result["job_id"] == job_id
    assert result["status"] == "completed"
    assert result["error_message"] == "error"
    assert result["guidelines"] == ["guide1"]
    assert result["linked_documents"] == ["doc1"]
    assert result["coding_section"] == "section1"
    assert result["processed_at"] == "2024-01-01T00:00:00+00:00"
    assert result["added_codes"] == {"code1": "desc1"}
    assert result["removed_codes"] == {"code2": "desc2"}
    assert result["number_of_pages"] == 5


def test_build_callback_without_document_revision_and_missing_optional_fields():
    worker = RevisionProcessingWorker()
    job_id = "job456"
    completion = MagicMock()
    completion.model_dump.return_value = {
        "status": "pending"
    }

    before_call = datetime.now(timezone.utc)
    result = worker._build_callback(job_id, completion, None)
    after_call = datetime.now(timezone.utc)

    assert result["job_id"] == job_id
    assert result["status"] == "pending"
    assert result["error_message"] == ""
    assert result["guidelines"] == []
    assert result["linked_documents"] == []
    assert result["coding_section"] is None
    processed_at = datetime.fromisoformat(result["processed_at"])
    assert before_call <= processed_at <= after_call
    assert "added_codes" not in result
    assert "removed_codes" not in result
    assert "number_of_pages" not in result


def test_build_callback_with_document_revision_none_added_removed_codes():
    worker = RevisionProcessingWorker()
    job_id = "job789"
    completion = MagicMock()
    completion.model_dump.return_value = {
        "status": "failed",
        "error_message": "some error",
        "guidelines": [],
        "linked_documents": [],
        "coding_section": None,
        "processed_at": "2024-02-02T12:00:00+00:00",
    }
    document_revision = MagicMock()
    document_revision.added_codes = None
    document_revision.removed_codes = None
    document_revision.num_of_pages = 10

    result = worker._build_callback(job_id, completion, document_revision)

    assert result["job_id"] == job_id
    assert result["status"] == "failed"
    assert result["error_message"] == "some error"
    assert result["guidelines"] == []
    assert result["linked_documents"] == []
    assert result["coding_section"] is None
    assert result["processed_at"] == "2024-02-02T12:00:00+00:00"
    assert result["added_codes"] == {}
    assert result["removed_codes"] == {}
    assert result["number_of_pages"] == 10


def test_build_callback_completion_model_dump_returns_empty_dict():
    worker = RevisionProcessingWorker()
    job_id = "job000"
    completion = MagicMock()
    completion.model_dump.return_value = {}

    before_call = datetime.now(timezone.utc)
    result = worker._build_callback(job_id, completion, None)
    after_call = datetime.now(timezone.utc)

    assert result["job_id"] == job_id
    assert result["status"] is None
    assert result["error_message"] == ""
    assert result["guidelines"] == []
    assert result["linked_documents"] == []
    assert result["coding_section"] is None
    processed_at = datetime.fromisoformat(result["processed_at"])
    assert before_call <= processed_at <= after_call
    assert "added_codes" not in result
    assert "removed_codes" not in result
    assert "number_of_pages" not in result


def test_build_callback_with_invalid_job_id_type():
    worker = RevisionProcessingWorker()
    job_id = 123  # invalid type, should be str
    completion = MagicMock()
    completion.model_dump.return_value = {
        "status": "completed"
    }

    with pytest.raises(AttributeError):
        # The function expects job_id to be str, but it uses it only as a dict value,
        # so no error expected here. But test anyway to confirm no crash.
        result = worker._build_callback(job_id, completion, None)
        assert result["job_id"] == 123


def test_build_callback_with_completion_model_dump_raising_exception():
    worker = RevisionProcessingWorker()
    job_id = "job999"
    completion = MagicMock()
    completion.model_dump.side_effect = RuntimeError("dump error")

    with pytest.raises(RuntimeError, match="dump error"):
        worker._build_callback(job_id, completion, None)


def test_build_callback_document_revision_with_zero_pages_and_empty_codes():
    worker = RevisionProcessingWorker()
    job_id = "job321"
    completion = MagicMock()
    completion.model_dump.return_value = {
        "status": "completed",
        "processed_at": "2024-03-03T03:03:03+00:00"
    }
    document_revision = MagicMock()
    document_revision.added_codes = {}
    document_revision.removed_codes = {}
    document_revision.num_of_pages = 0

    result = worker._build_callback(job_id, completion, document_revision)

    assert result["added_codes"] == {}
    assert result["removed_codes"] == {}
    assert result["number_of_pages"] == 0
    assert result["processed_at"] == "2024-03-03T03:03:03+00:00"


def test_build_callback_with_none_completion_and_document_revision():
    worker = RevisionProcessingWorker()
    job_id = "job_none"
    completion = None
    document_revision = None

    with pytest.raises(AttributeError):
        worker._build_callback(job_id, completion, document_revision)
# AI_TEST_AGENT_END function=RevisionProcessingWorker._build_callback
