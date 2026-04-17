from datetime import datetime, timezone
from httpx import HTTPStatusError, Response, TimeoutException
from pydantic import BaseModel, ValidationError
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock
from unittest.mock import AsyncMock, MagicMock, patch
from unittest.mock import AsyncMock, patch, MagicMock
from unittest.mock import MagicMock
from unittest.mock import MagicMock, Mock, call
from unittest.mock import patch
from unittest.mock import patch, MagicMock
from workers.base_worker import BaseWorker
import asyncio
import json
import os
import pytest

# AI_TEST_AGENT_START function=BaseWorker.__init__
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_with_full_subscription_path_and_env_vars(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = "test-project"
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    subscription_name = "projects/test-project/subscriptions/test-subscription"
    worker = BaseWorker(subscription_name=subscription_name)

    assert worker.subscription_path == subscription_name
    mock_logger.info.assert_called_once_with("Using subscription path: %s", subscription_name)
    assert worker.flow_control == "flow_control_instance"
    assert worker.subscriber == "subscriber_instance"
    assert worker._project_id == "test-project"
    assert worker._callback_timeout == 3000
    assert worker.http_client == "http_client_instance"
    assert worker._running is False
    assert worker._executor._max_workers == 1

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_SUBSCRIPTION": "my-subscription", "GOOGLE_CLOUD_PROJECT_ID": "env-project", "FLOW_CONTROL_MAX_MESSAGES": "10", "CALLBACK_TIMEOUT": "4000", "WORKER_MAX_WORKERS": "3"}, clear=True)
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_with_subscription_name_from_env_and_custom_env_vars(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = "env-project"
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    worker = BaseWorker(subscription_name=None)

    expected_path = "projects/env-project/subscriptions/my-subscription"
    assert worker.subscription_path == expected_path
    mock_logger.info.assert_called_once_with("Using subscription path: %s", expected_path)
    assert worker.flow_control == "flow_control_instance"
    assert worker._callback_timeout == 4000
    assert worker._executor._max_workers == 3
    assert worker.http_client == "http_client_instance"

@patch.dict(os.environ, {"SCRAPING_SUBSCRIPTION": "scraping-subscription", "GOOGLE_CLOUD_PROJECT_ID": "scraping-project"}, clear=True)
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_with_scraping_subscription_env_var(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = "scraping-project"
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    worker = BaseWorker(subscription_name=None)

    expected_path = "projects/scraping-project/subscriptions/scraping-subscription"
    assert worker.subscription_path == expected_path
    mock_logger.info.assert_called_once_with("Using subscription path: %s", expected_path)
    assert worker.flow_control == "flow_control_instance"
    assert worker.subscriber == "subscriber_instance"
    assert worker._project_id == "scraping-project"
    assert worker.http_client == "http_client_instance"

@patch.dict(os.environ, {}, clear=True)
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_raises_value_error_when_no_project_id_and_subscription_not_full_path(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = None
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    with pytest.raises(ValueError, match="GOOGLE_CLOUD_PROJECT_ID required when subscription is not a full path"):
        BaseWorker(subscription_name="simple-subscription")

@patch.dict(os.environ, {"FLOW_CONTROL_MAX_MESSAGES": "0", "CALLBACK_TIMEOUT": "0", "WORKER_MAX_WORKERS": "0", "GOOGLE_CLOUD_PROJECT_ID": "zero-project"}, clear=True)
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_with_zero_values_in_env_vars(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = "zero-project"
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    worker = BaseWorker(subscription_name="zero-subscription")

    expected_path = "projects/zero-project/subscriptions/zero-subscription"
    assert worker.subscription_path == expected_path
    mock_logger.info.assert_called_once_with("Using subscription path: %s", expected_path)
    assert worker._callback_timeout == 0
    assert worker._executor._max_workers == 0

@patch.dict(os.environ, {"FLOW_CONTROL_MAX_MESSAGES": "-1", "CALLBACK_TIMEOUT": "-10", "WORKER_MAX_WORKERS": "-5", "GOOGLE_CLOUD_PROJECT_ID": "neg-project"}, clear=True)
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_with_negative_values_in_env_vars(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = "neg-project"
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    worker = BaseWorker(subscription_name="neg-subscription")

    expected_path = "projects/neg-project/subscriptions/neg-subscription"
    assert worker.subscription_path == expected_path
    mock_logger.info.assert_called_once_with("Using subscription path: %s", expected_path)
    assert worker._callback_timeout == -10
    assert worker._executor._max_workers == -5

@patch.dict(os.environ, {"FLOW_CONTROL_MAX_MESSAGES": "not-an-int", "CALLBACK_TIMEOUT": "not-an-int", "WORKER_MAX_WORKERS": "not-an-int", "GOOGLE_CLOUD_PROJECT_ID": "badint-project"}, clear=True)
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_raises_value_error_on_invalid_int_env_vars(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = "badint-project"
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    with pytest.raises(ValueError):
        BaseWorker(subscription_name="badint-subscription")

@patch.dict(os.environ, {"DOCUMENT_PROCESSING_SUBSCRIPTION": ""}, clear=True)
@patch("workers.base_worker.get_logger")
@patch("workers.base_worker.pubsub_v1.types.FlowControl")
@patch("workers.base_worker.GCPConfig")
@patch("workers.base_worker.PubSubClient")
@patch("workers.base_worker.httpx.AsyncClient")
def test_init_with_empty_string_subscription_name_and_env_vars(mock_async_client, mock_pubsub_client, mock_gcp_config, mock_flow_control, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_flow_control.return_value = "flow_control_instance"
    mock_gcp_config.load_from_env.return_value.project_id = "empty-project"
    mock_pubsub_client.return_value.subscriber = "subscriber_instance"
    mock_async_client.return_value = "http_client_instance"

    with pytest.raises(ValueError, match="GOOGLE_CLOUD_PROJECT_ID required when subscription is not a full path"):
        BaseWorker(subscription_name="")
# AI_TEST_AGENT_END function=BaseWorker.__init__

# AI_TEST_AGENT_START function=BaseWorker.process_message
@pytest.mark.asyncio
async def test_process_message_not_implemented():
    worker = BaseWorker()
    message_data: Dict[str, Any] = {"key": "value"}
    correlation_id = "corr-123"
    with pytest.raises(NotImplementedError):
        await worker.process_message(message_data, correlation_id)

@pytest.mark.asyncio
async def test_process_message_with_empty_message_data():
    worker = BaseWorker()
    message_data: Dict[str, Any] = {}
    correlation_id = "corr-456"
    with pytest.raises(NotImplementedError):
        await worker.process_message(message_data, correlation_id)

@pytest.mark.asyncio
async def test_process_message_with_none_message_data():
    worker = BaseWorker()
    message_data = None
    correlation_id = "corr-789"
    with pytest.raises(TypeError):
        await worker.process_message(message_data, correlation_id)

@pytest.mark.asyncio
async def test_process_message_with_empty_correlation_id():
    worker = BaseWorker()
    message_data: Dict[str, Any] = {"key": "value"}
    correlation_id = ""
    with pytest.raises(NotImplementedError):
        await worker.process_message(message_data, correlation_id)

@pytest.mark.asyncio
async def test_process_message_with_none_correlation_id():
    worker = BaseWorker()
    message_data: Dict[str, Any] = {"key": "value"}
    correlation_id = None
    with pytest.raises(TypeError):
        await worker.process_message(message_data, correlation_id)
# AI_TEST_AGENT_END function=BaseWorker.process_message

# AI_TEST_AGENT_START function=BaseWorker.send_callback
@pytest.mark.asyncio
async def test_send_callback_no_callback_url_logs_and_returns_true():
    worker = BaseWorker()
    worker.logger = MagicMock()
    result = await worker.send_callback("", {"key": "value"})
    worker.logger.info.assert_any_call("No callback URL provided, would send: %s", {"key": "value"})
    assert result is True

@pytest.mark.asyncio
async def test_send_callback_successful_post_returns_true_and_logs():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    with patch("httpx.AsyncClient", return_value=mock_async_client):
        result = await worker.send_callback("http://test.url", {"data": 123}, headers={"X-Test": "yes"}, auth_token="token123")
    worker.logger.info.assert_any_call("Sending callback to: %s", "http://test.url")
    worker.logger.info.assert_any_call("Callback sent successfully: %s", 200)
    mock_client.post.assert_awaited_once_with("http://test.url", json={"data": 123}, headers={"X-Test": "yes", "Authorization": "Bearer token123"})
    assert result is True

@pytest.mark.asyncio
async def test_send_callback_http_status_error_logs_and_returns_false():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    response_mock = MagicMock()
    response_mock.status_code = 404
    response_mock.text = "Not Found"
    exc = HTTPStatusError("Error", request=MagicMock(), response=response_mock)
    mock_response = AsyncMock()
    mock_response.raise_for_status.side_effect = exc
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    with patch("httpx.AsyncClient", return_value=mock_async_client):
        result = await worker.send_callback("http://test.url", {"data": 123})
    worker.logger.error.assert_called_once_with("Callback HTTP error: %s - %s", 404, "Not Found")
    assert result is False

@pytest.mark.asyncio
async def test_send_callback_timeout_exception_logs_and_returns_false():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.post.side_effect = TimeoutException("Timeout")
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    with patch("httpx.AsyncClient", return_value=mock_async_client):
        result = await worker.send_callback("http://test.url", {"data": 123})
    worker.logger.error.assert_called_once_with("Callback timeout for URL: %s", "http://test.url")
    assert result is False

@pytest.mark.asyncio
async def test_send_callback_generic_exception_logs_and_returns_false():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    mock_client = AsyncMock()
    mock_client.post.side_effect = ValueError("Unexpected error")
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    with patch("httpx.AsyncClient", return_value=mock_async_client):
        result = await worker.send_callback("http://test.url", {"data": 123})
    worker.logger.error.assert_called_once()
    assert "Failed to send callback" in worker.logger.error.call_args[0][0]
    assert result is False

@pytest.mark.asyncio
async def test_send_callback_headers_none_and_auth_token_none_sends_without_auth_header():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    mock_response = AsyncMock()
    mock_response.status_code = 201
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    with patch("httpx.AsyncClient", return_value=mock_async_client):
        result = await worker.send_callback("http://test.url", {"data": "value"})
    mock_client.post.assert_awaited_once_with("http://test.url", json={"data": "value"}, headers={})
    assert result is True

@pytest.mark.asyncio
async def test_send_callback_empty_headers_dict_and_auth_token_adds_auth_header():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    mock_response = AsyncMock()
    mock_response.status_code = 202
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    with patch("httpx.AsyncClient", return_value=mock_async_client):
        result = await worker.send_callback("http://test.url", {"data": "value"}, headers={}, auth_token="abc123")
    mock_client.post.assert_awaited_once_with("http://test.url", json={"data": "value"}, headers={"Authorization": "Bearer abc123"})
    assert result is True

@pytest.mark.asyncio
async def test_send_callback_invalid_callback_url_type_raises_and_logs():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    with pytest.raises(TypeError):
        await worker.send_callback(12345, {"data": "value"})
    # No logger calls expected because exception raised before logging

@pytest.mark.asyncio
async def test_send_callback_callback_data_none_raises_type_error():
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker._callback_timeout = 5
    with pytest.raises(TypeError):
        await worker.send_callback("http://test.url", None)
    # No logger calls expected because exception raised before logging
# AI_TEST_AGENT_END function=BaseWorker.send_callback

# AI_TEST_AGENT_START function=BaseWorker.validate_message
class DummyMessage(BaseModel):
    field1: int
    field2: str

class DummyMessageWithException:
    def __init__(self, **kwargs):
        raise RuntimeError("Unexpected error")

def test_validate_message_success_returns_instance():
    worker = BaseWorker()
    worker.logger = MagicMock()
    data = {"field1": 123, "field2": "abc"}
    result = worker.validate_message(data, DummyMessage)
    assert isinstance(result, DummyMessage)
    assert result.field1 == 123
    assert result.field2 == "abc"
    worker.logger.error.assert_not_called()

def test_validate_message_validation_error_logs_and_raises():
    worker = BaseWorker()
    worker.logger = MagicMock()
    data = {"field1": "not-an-int", "field2": "abc"}
    with pytest.raises(ValidationError) as excinfo:
        worker.validate_message(data, DummyMessage)
    assert "field1" in str(excinfo.value)
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Message validation failed: %s"
    assert isinstance(call_args[1], ValidationError)

def test_validate_message_unexpected_exception_logs_and_raises():
    worker = BaseWorker()
    worker.logger = MagicMock()
    data = {"any": "value"}
    with pytest.raises(RuntimeError) as excinfo:
        worker.validate_message(data, DummyMessageWithException)
    assert "Unexpected error" in str(excinfo.value)
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Unexpected validation error: %s"
    assert isinstance(call_args[1], RuntimeError)

def test_validate_message_empty_dict_for_required_fields_raises_validation_error():
    worker = BaseWorker()
    worker.logger = MagicMock()
    data = {}
    with pytest.raises(ValidationError):
        worker.validate_message(data, DummyMessage)
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Message validation failed: %s"
    assert isinstance(call_args[1], ValidationError)

def test_validate_message_none_as_message_data_raises_type_error():
    worker = BaseWorker()
    worker.logger = MagicMock()
    with pytest.raises(TypeError):
        worker.validate_message(None, DummyMessage)
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Unexpected validation error: %s"
    assert isinstance(call_args[1], TypeError)

def test_validate_message_extra_fields_ignored_or_error_based_on_model():
    class ModelWithNoExtra(BaseModel):
        field1: int

        class Config:
            extra = "forbid"

    worker = BaseWorker()
    worker.logger = MagicMock()
    data = {"field1": 1, "extra_field": "not allowed"}
    with pytest.raises(ValidationError):
        worker.validate_message(data, ModelWithNoExtra)
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Message validation failed: %s"
    assert isinstance(call_args[1], ValidationError)

def test_validate_message_valid_with_minimal_fields():
    class ModelWithOptional(BaseModel):
        field1: int
        field2: str = "default"

    worker = BaseWorker()
    worker.logger = MagicMock()
    data = {"field1": 5}
    result = worker.validate_message(data, ModelWithOptional)
    assert result.field1 == 5
    assert result.field2 == "default"
    worker.logger.error.assert_not_called()

def test_validate_message_field_with_none_value_raises_if_not_optional():
    worker = BaseWorker()
    worker.logger = MagicMock()
    data = {"field1": None, "field2": "abc"}
    with pytest.raises(ValidationError):
        worker.validate_message(data, DummyMessage)
    worker.logger.error.assert_called_once()
    call_args = worker.logger.error.call_args[0]
    assert call_args[0] == "Message validation failed: %s"
    assert isinstance(call_args[1], ValidationError)
# AI_TEST_AGENT_END function=BaseWorker.validate_message

# AI_TEST_AGENT_START function=BaseWorker.create_callback_data
def test_create_callback_data_basic_fields_present(monkeypatch):
    fixed_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    result = worker.create_callback_data(
        correlation_id="cid123",
        job_id="job456",
        source_id="src789",
        status="completed",
        message="All done",
    )
    assert result["correlation_id"] == "cid123"
    assert result["job_id"] == "job456"
    assert result["source_id"] == "src789"
    assert result["status"] == "completed"
    assert result["message"] == "All done"
    assert result["processed_at"] == fixed_time.isoformat()
    assert "result" not in result

def test_create_callback_data_with_result_data(monkeypatch):
    fixed_time = datetime(2023, 5, 5, 15, 30, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    result_data = {"key1": "value1", "key2": 2}
    result = worker.create_callback_data(
        correlation_id="cidX",
        job_id="jobY",
        source_id="srcZ",
        status="failed",
        message="Error occurred",
        result_data=result_data,
    )
    assert result["correlation_id"] == "cidX"
    assert result["job_id"] == "jobY"
    assert result["source_id"] == "srcZ"
    assert result["status"] == "failed"
    assert result["message"] == "Error occurred"
    assert result["processed_at"] == fixed_time.isoformat()
    assert result["result"] == result_data

def test_create_callback_data_empty_strings(monkeypatch):
    fixed_time = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    result = worker.create_callback_data(
        correlation_id="",
        job_id="",
        source_id="",
        status="",
        message="",
        result_data=None,
    )
    assert result["correlation_id"] == ""
    assert result["job_id"] == ""
    assert result["source_id"] == ""
    assert result["status"] == ""
    assert result["message"] == ""
    assert result["processed_at"] == fixed_time.isoformat()
    assert "result" not in result

def test_create_callback_data_none_result_data(monkeypatch):
    fixed_time = datetime(2022, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    result = worker.create_callback_data(
        correlation_id="abc",
        job_id="def",
        source_id="ghi",
        status="pending",
        message="Waiting",
        result_data=None,
    )
    assert "result" not in result
    assert result["processed_at"] == fixed_time.isoformat()

def test_create_callback_data_result_data_empty_dict(monkeypatch):
    fixed_time = datetime(2024, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    result = worker.create_callback_data(
        correlation_id="id1",
        job_id="id2",
        source_id="id3",
        status="ok",
        message="Empty result",
        result_data={},
    )
    assert "result" not in result
    assert result["processed_at"] == fixed_time.isoformat()

def test_create_callback_data_invalid_types(monkeypatch):
    fixed_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    with pytest.raises(AttributeError):
        # Passing int instead of str for correlation_id should cause failure when building dict
        worker.create_callback_data(
            correlation_id=123,
            job_id="job",
            source_id="src",
            status="status",
            message="msg",
            result_data=None,
        )

def test_create_callback_data_result_data_non_dict(monkeypatch):
    fixed_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    # result_data is expected to be dict or None, passing list should still add it as "result"
    result_data = ["not", "a", "dict"]
    result = worker.create_callback_data(
        correlation_id="cid",
        job_id="jid",
        source_id="sid",
        status="stat",
        message="msg",
        result_data=result_data,
    )
    assert result["result"] == result_data
    assert result["processed_at"] == fixed_time.isoformat()

def test_create_callback_data_all_none_strings(monkeypatch):
    fixed_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("workers.base_worker.datetime", datetime)
    monkeypatch.setattr("workers.base_worker.datetime.now", lambda tz=None: fixed_time)

    worker = BaseWorker()
    with pytest.raises(TypeError):
        # Passing None for required string parameters should raise TypeError when building dict
        worker.create_callback_data(
            correlation_id=None,
            job_id=None,
            source_id=None,
            status=None,
            message=None,
            result_data=None,
        )
# AI_TEST_AGENT_END function=BaseWorker.create_callback_data

# AI_TEST_AGENT_START function=BaseWorker._callback
class DummyWorker(BaseWorker):
    async def process_message(self, message_data, correlation_id):
        return await super().process_message(message_data, correlation_id)

@pytest.fixture
def worker():
    return DummyWorker()

@pytest.fixture
def message_mock():
    msg = MagicMock()
    msg.ack = MagicMock()
    msg.data = MagicMock()
    return msg

@pytest.fixture
def logger_mock(worker):
    worker.logger = MagicMock()
    return worker.logger

@pytest.fixture
def event_loop_mock():
    loop_mock = MagicMock()
    loop_mock.run_until_complete = MagicMock()
    loop_mock.close = MagicMock()
    return loop_mock

def test_callback_successful_processing(worker, message_mock, logger_mock, event_loop_mock):
    message_mock.data.decode.return_value = json.dumps({
        "payload": {"correlation_id": "abc123"}
    })
    event_loop_mock.run_until_complete.return_value = True

    with patch("asyncio.new_event_loop", return_value=event_loop_mock) as new_loop_patch, \
         patch("asyncio.set_event_loop") as set_loop_patch, \
         patch.object(worker, "process_message", new=AsyncMock(return_value=True)) as process_mock:

        worker._callback(message_mock)

        message_mock.ack.assert_called_once()
        message_mock.data.decode.assert_called_once_with("utf-8")
        logger_mock.info.assert_any_call("Processing message with correlation_id: %s", "abc123")
        new_loop_patch.assert_called_once()
        set_loop_patch.assert_called_once_with(event_loop_mock)
        process_mock.assert_awaited_once_with({"payload": {"correlation_id": "abc123"}}, "abc123")
        logger_mock.info.assert_any_call("Successfully processed message: %s", "abc123")
        event_loop_mock.close.assert_called_once()

def test_callback_failed_processing_logs_error(worker, message_mock, logger_mock, event_loop_mock):
    message_mock.data.decode.return_value = json.dumps({
        "meta": {"message_id": "msgid456"}
    })
    event_loop_mock.run_until_complete.return_value = False

    with patch("asyncio.new_event_loop", return_value=event_loop_mock), \
         patch("asyncio.set_event_loop"), \
         patch.object(worker, "process_message", new=AsyncMock(return_value=False)):

        worker._callback(message_mock)

        message_mock.ack.assert_called_once()
        logger_mock.error.assert_called_once_with("Failed to process message: %s", "msgid456")
        logger_mock.info.assert_any_call("Processing message with correlation_id: %s", "msgid456")
        event_loop_mock.close.assert_called_once()

def test_callback_missing_correlation_id_uses_unknown(worker, message_mock, logger_mock, event_loop_mock):
    message_mock.data.decode.return_value = json.dumps({
        "payload": {},
        "meta": {}
    })
    event_loop_mock.run_until_complete.return_value = True

    with patch("asyncio.new_event_loop", return_value=event_loop_mock), \
         patch("asyncio.set_event_loop"), \
         patch.object(worker, "process_message", new=AsyncMock(return_value=True)):

        worker._callback(message_mock)

        message_mock.ack.assert_called_once()
        logger_mock.info.assert_any_call("Processing message with correlation_id: %s", "unknown")
        logger_mock.info.assert_any_call("Successfully processed message: %s", "unknown")
        event_loop_mock.close.assert_called_once()

def test_callback_json_decode_error_logs_error(worker, message_mock, logger_mock):
    message_mock.data.decode.return_value = "not a json"

    with patch("asyncio.new_event_loop"), patch("asyncio.set_event_loop"):
        worker._callback(message_mock)

    message_mock.ack.assert_called_once()
    logger_mock.error.assert_called_once()
    assert "Failed to parse message JSON" in logger_mock.error.call_args[0][0]

def test_callback_process_message_raises_exception_logs_error(worker, message_mock, logger_mock, event_loop_mock):
    message_mock.data.decode.return_value = json.dumps({"payload": {"correlation_id": "cid"}})

    async def raise_exc(*args, **kwargs):
        raise RuntimeError("fail")

    with patch("asyncio.new_event_loop", return_value=event_loop_mock), \
         patch("asyncio.set_event_loop"), \
         patch.object(worker, "process_message", new=AsyncMock(side_effect=raise_exc)):

        worker._callback(message_mock)

        message_mock.ack.assert_called_once()
        logger_mock.error.assert_called_once()
        assert "Unexpected error processing message" in logger_mock.error.call_args[0][0]
        event_loop_mock.close.assert_called_once()

def test_callback_data_decode_raises_exception_logs_error(worker, message_mock, logger_mock):
    message_mock.data.decode.side_effect = Exception("decode fail")

    with patch("asyncio.new_event_loop"), patch("asyncio.set_event_loop"):
        worker._callback(message_mock)

    message_mock.ack.assert_called_once()
    logger_mock.error.assert_called_once()
    assert "Unexpected error processing message" in logger_mock.error.call_args[0][0]

def test_callback_process_message_returns_non_bool_logs_error(worker, message_mock, logger_mock, event_loop_mock):
    message_mock.data.decode.return_value = json.dumps({"payload": {"correlation_id": "cid"}})
    event_loop_mock.run_until_complete.return_value = None

    with patch("asyncio.new_event_loop", return_value=event_loop_mock), \
         patch("asyncio.set_event_loop"), \
         patch.object(worker, "process_message", new=AsyncMock(return_value=None)):

        worker._callback(message_mock)

        message_mock.ack.assert_called_once()
        logger_mock.error.assert_called_once_with("Failed to process message: %s", "cid")
        event_loop_mock.close.assert_called_once()

def test_callback_process_message_raises_non_runtime_exception_logs_error(worker, message_mock, logger_mock, event_loop_mock):
    message_mock.data.decode.return_value = json.dumps({"payload": {"correlation_id": "cid"}})

    async def raise_value_error(*args, **kwargs):
        raise ValueError("value error")

    with patch("asyncio.new_event_loop", return_value=event_loop_mock), \
         patch("asyncio.set_event_loop"), \
         patch.object(worker, "process_message", new=AsyncMock(side_effect=raise_value_error)):

        worker._callback(message_mock)

        message_mock.ack.assert_called_once()
        logger_mock.error.assert_called_once()
        assert "Unexpected error processing message" in logger_mock.error.call_args[0][0]
        event_loop_mock.close.assert_called_once()
# AI_TEST_AGENT_END function=BaseWorker._callback

# AI_TEST_AGENT_START function=BaseWorker.start
def test_start_successful_run(monkeypatch):
    worker = BaseWorker.__new__(BaseWorker)
    worker.subscription_path = "projects/test-project/subscriptions/test-sub"
    worker.logger = MagicMock()
    worker.subscriber = MagicMock()
    worker._executor = MagicMock()
    worker.flow_control = None
    worker._callback = Mock()
    worker._running = False

    streaming_pull_future_mock = MagicMock()
    worker.subscriber.get_subscription.return_value = None
    worker.subscriber.subscribe.return_value = streaming_pull_future_mock
    streaming_pull_future_mock.result.side_effect = KeyboardInterrupt()

    worker.start()

    assert worker._running is False
    worker.logger.info.assert_has_calls([
        call("Starting worker for subscription: %s", worker.subscription_path),
        call("Testing subscription access..."),
        call("✅ Subscription access verified"),
        call("Worker started successfully. Listening for messages..."),
        call("Received shutdown signal"),
        call("Worker stopped"),
    ])
    worker.subscriber.get_subscription.assert_called_once_with(request={"subscription": worker.subscription_path})
    worker.subscriber.subscribe.assert_called_once_with(
        worker.subscription_path,
        callback=worker._callback,
        flow_control=worker.flow_control,
    )
    streaming_pull_future_mock.result.assert_called()
    streaming_pull_future_mock.cancel.assert_called_once()
    worker._executor.shutdown.assert_called_once_with(wait=True)

def test_start_subscription_access_raises(monkeypatch):
    worker = BaseWorker.__new__(BaseWorker)
    worker.subscription_path = "projects/test-project/subscriptions/test-sub"
    worker.logger = MagicMock()
    worker.subscriber = MagicMock()
    worker._executor = MagicMock()
    worker.flow_control = None
    worker._callback = Mock()
    worker._running = False

    error = RuntimeError("access denied")
    worker.subscriber.get_subscription.side_effect = error

    with pytest.raises(RuntimeError, match="access denied"):
        worker.start()

    assert worker._running is False
    worker.logger.error.assert_called_once_with("Error starting worker: %s", error)
    worker._executor.shutdown.assert_called_once_with(wait=True)
    worker.logger.info.assert_any_call("Worker stopped")

def test_start_streaming_pull_future_result_raises_non_keyboard(monkeypatch):
    worker = BaseWorker.__new__(BaseWorker)
    worker.subscription_path = "projects/test-project/subscriptions/test-sub"
    worker.logger = MagicMock()
    worker.subscriber = MagicMock()
    worker._executor = MagicMock()
    worker.flow_control = None
    worker._callback = Mock()
    worker._running = False

    streaming_pull_future_mock = MagicMock()
    worker.subscriber.get_subscription.return_value = None
    worker.subscriber.subscribe.return_value = streaming_pull_future_mock
    streaming_pull_future_mock.result.side_effect = RuntimeError("stream error")

    with pytest.raises(RuntimeError, match="stream error"):
        worker.start()

    assert worker._running is False
    worker.logger.error.assert_called_once()
    worker._executor.shutdown.assert_called_once_with(wait=True)
    worker.logger.info.assert_any_call("Worker stopped")

def test_start_streaming_pull_future_cancel_result_raises(monkeypatch):
    worker = BaseWorker.__new__(BaseWorker)
    worker.subscription_path = "projects/test-project/subscriptions/test-sub"
    worker.logger = MagicMock()
    worker.subscriber = MagicMock()
    worker._executor = MagicMock()
    worker.flow_control = None
    worker._callback = Mock()
    worker._running = False

    streaming_pull_future_mock = MagicMock()
    worker.subscriber.get_subscription.return_value = None
    worker.subscriber.subscribe.return_value = streaming_pull_future_mock

    def side_effect_result():
        raise KeyboardInterrupt()

    def side_effect_result_after_cancel():
        raise RuntimeError("cancel error")

    streaming_pull_future_mock.result.side_effect = [side_effect_result(), side_effect_result_after_cancel()]

    worker.start()

    assert worker._running is False
    worker.logger.info.assert_any_call("Received shutdown signal")
    streaming_pull_future_mock.cancel.assert_called_once()
    worker._executor.shutdown.assert_called_once_with(wait=True)
    worker.logger.info.assert_any_call("Worker stopped")

def test_start_subscription_path_empty_string(monkeypatch):
    worker = BaseWorker.__new__(BaseWorker)
    worker.subscription_path = ""
    worker.logger = MagicMock()
    worker.subscriber = MagicMock()
    worker._executor = MagicMock()
    worker.flow_control = None
    worker._callback = Mock()
    worker._running = False

    streaming_pull_future_mock = MagicMock()
    worker.subscriber.get_subscription.return_value = None
    worker.subscriber.subscribe.return_value = streaming_pull_future_mock
    streaming_pull_future_mock.result.side_effect = KeyboardInterrupt()

    worker.start()

    worker.subscriber.get_subscription.assert_called_once_with(request={"subscription": ""})
    worker.subscriber.subscribe.assert_called_once_with(
        "",
        callback=worker._callback,
        flow_control=worker.flow_control,
    )
    assert worker._running is False
    worker.logger.info.assert_any_call("Worker stopped")

def test_start_subscription_path_none_raises(monkeypatch):
    worker = BaseWorker.__new__(BaseWorker)
    worker.subscription_path = None
    worker.logger = MagicMock()
    worker.subscriber = MagicMock()
    worker._executor = MagicMock()
    worker.flow_control = None
    worker._callback = Mock()
    worker._running = False

    with pytest.raises(TypeError):
        worker.start()

    worker.logger.error.assert_called_once()
    worker._executor.shutdown.assert_called_once_with(wait=True)
    worker.logger.info.assert_any_call("Worker stopped")

def test_start_subscribe_raises(monkeypatch):
    worker = BaseWorker.__new__(BaseWorker)
    worker.subscription_path = "projects/test-project/subscriptions/test-sub"
    worker.logger = MagicMock()
    worker.subscriber = MagicMock()
    worker._executor = MagicMock()
    worker.flow_control = None
    worker._callback = Mock()
    worker._running = False

    worker.subscriber.get_subscription.return_value = None
    error = RuntimeError("subscribe failed")
    worker.subscriber.subscribe.side_effect = error

    with pytest.raises(RuntimeError, match="subscribe failed"):
        worker.start()

    assert worker._running is False
    worker.logger.error.assert_called_once_with("Error starting worker: %s", error)
    worker._executor.shutdown.assert_called_once_with(wait=True)
    worker.logger.info.assert_any_call("Worker stopped")
# AI_TEST_AGENT_END function=BaseWorker.start

# AI_TEST_AGENT_START function=BaseWorker.stop
def test_stop_sets_running_false_and_logs_info():
    worker = BaseWorker()
    worker._running = True
    worker.logger = MagicMock()
    worker.stop()
    assert worker._running is False
    worker.logger.info.assert_called_once_with("Stopping worker...")

def test_stop_when_already_stopped_logs_info_and_sets_running_false():
    worker = BaseWorker()
    worker._running = False
    worker.logger = MagicMock()
    worker.stop()
    assert worker._running is False
    worker.logger.info.assert_called_once_with("Stopping worker...")

def test_stop_logger_info_called_once_with_exact_message():
    worker = BaseWorker()
    worker._running = True
    mock_logger = MagicMock()
    worker.logger = mock_logger
    worker.stop()
    mock_logger.info.assert_called_once()
    args, kwargs = mock_logger.info.call_args
    assert args[0] == "Stopping worker..."

def test_stop_with_logger_info_raises_exception_propagates():
    worker = BaseWorker()
    worker._running = True
    def raise_error(msg):
        raise RuntimeError("Logger failure")
    mock_logger = MagicMock()
    mock_logger.info.side_effect = raise_error
    worker.logger = mock_logger
    with pytest.raises(RuntimeError, match="Logger failure"):
        worker.stop()
    assert worker._running is False

def test_stop_multiple_calls_always_sets_running_false_and_logs():
    worker = BaseWorker()
    worker._running = True
    worker.logger = MagicMock()
    for _ in range(3):
        worker.stop()
    assert worker._running is False
    assert worker.logger.info.call_count == 3
    for call in worker.logger.info.call_args_list:
        assert call[0][0] == "Stopping worker..."

def test_stop_with_logger_info_called_with_non_string_message():
    worker = BaseWorker()
    worker._running = True
    mock_logger = MagicMock()
    worker.logger = mock_logger
    worker.stop()
    assert isinstance(mock_logger.info.call_args[0][0], str)

def test_stop_with_logger_info_called_when_running_is_none():
    worker = BaseWorker()
    worker._running = None
    worker.logger = MagicMock()
    worker.stop()
    assert worker._running is False
    worker.logger.info.assert_called_once_with("Stopping worker...")

def test_stop_with_logger_info_called_when_running_is_non_boolean():
    worker = BaseWorker()
    worker._running = "yes"
    worker.logger = MagicMock()
    worker.stop()
    assert worker._running is False
    worker.logger.info.assert_called_once_with("Stopping worker...")
# AI_TEST_AGENT_END function=BaseWorker.stop

# AI_TEST_AGENT_START function=BaseWorker.shutdown
@pytest.mark.asyncio
async def test_shutdown_calls_logger_info_http_client_aclose_and_stop(monkeypatch):
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker.http_client = MagicMock()
    worker.http_client.aclose = AsyncMock()
    worker.stop = MagicMock()

    await worker.shutdown()

    worker.logger.info.assert_called_once_with("Shutting down worker...")
    worker.http_client.aclose.assert_awaited_once()
    worker.stop.assert_called_once()

@pytest.mark.asyncio
async def test_shutdown_raises_if_http_client_aclose_raises(monkeypatch):
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker.http_client = MagicMock()
    worker.http_client.aclose = AsyncMock(side_effect=RuntimeError("aclose failed"))
    worker.stop = MagicMock()

    with pytest.raises(RuntimeError, match="aclose failed"):
        await worker.shutdown()

    worker.logger.info.assert_called_once_with("Shutting down worker...")
    worker.http_client.aclose.assert_awaited_once()
    worker.stop.assert_not_called()

@pytest.mark.asyncio
async def test_shutdown_raises_if_logger_info_raises(monkeypatch):
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker.logger.info.side_effect = ValueError("logger failed")
    worker.http_client = MagicMock()
    worker.http_client.aclose = AsyncMock()
    worker.stop = MagicMock()

    with pytest.raises(ValueError, match="logger failed"):
        await worker.shutdown()

    worker.logger.info.assert_called_once_with("Shutting down worker...")
    worker.http_client.aclose.assert_not_awaited()
    worker.stop.assert_not_called()

@pytest.mark.asyncio
async def test_shutdown_raises_if_stop_raises(monkeypatch):
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker.http_client = MagicMock()
    worker.http_client.aclose = AsyncMock()
    worker.stop = MagicMock(side_effect=RuntimeError("stop failed"))

    with pytest.raises(RuntimeError, match="stop failed"):
        await worker.shutdown()

    worker.logger.info.assert_called_once_with("Shutting down worker...")
    worker.http_client.aclose.assert_awaited_once()
    worker.stop.assert_called_once()

@pytest.mark.asyncio
async def test_shutdown_with_http_client_aclose_none(monkeypatch):
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker.http_client = MagicMock()
    worker.http_client.aclose = None
    worker.stop = MagicMock()

    with pytest.raises(TypeError):
        await worker.shutdown()

    worker.logger.info.assert_called_once_with("Shutting down worker...")
    worker.stop.assert_not_called()

@pytest.mark.asyncio
async def test_shutdown_with_logger_none(monkeypatch):
    worker = BaseWorker()
    worker.logger = None
    worker.http_client = MagicMock()
    worker.http_client.aclose = AsyncMock()
    worker.stop = MagicMock()

    with pytest.raises(AttributeError):
        await worker.shutdown()

    worker.http_client.aclose.assert_not_awaited()
    worker.stop.assert_not_called()

@pytest.mark.asyncio
async def test_shutdown_with_stop_none(monkeypatch):
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker.http_client = MagicMock()
    worker.http_client.aclose = AsyncMock()
    worker.stop = None

    with pytest.raises(TypeError):
        await worker.shutdown()

    worker.logger.info.assert_called_once_with("Shutting down worker...")
    worker.http_client.aclose.assert_awaited_once()

@pytest.mark.asyncio
async def test_shutdown_called_multiple_times(monkeypatch):
    worker = BaseWorker()
    worker.logger = MagicMock()
    worker.http_client = MagicMock()
    worker.http_client.aclose = AsyncMock()
    worker.stop = MagicMock()

    await worker.shutdown()
    await worker.shutdown()

    assert worker.logger.info.call_count == 2
    assert worker.http_client.aclose.await_count == 2
    assert worker.stop.call_count == 2
# AI_TEST_AGENT_END function=BaseWorker.shutdown

# AI_TEST_AGENT_START function=BaseWorker.health_check
class DummyWorker(BaseWorker):
    def __init__(self, subscription_path, project_id, running):
        self.subscription_path = subscription_path
        self._project_id = project_id
        self._running = running

def test_health_check_running_status_and_env_var(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "5")
    worker = DummyWorker(subscription_path="sub/path", project_id="proj123", running=True)
    result = worker.health_check()
    assert result["worker"] == "DummyWorker"
    assert result["subscription"] == "sub/path"
    assert result["project"] == "proj123"
    assert result["status"] == "running"
    assert result["max_messages"] == 5

def test_health_check_stopped_status_and_default_max_messages(monkeypatch):
    monkeypatch.delenv("WORKER_MAX_MESSAGES", raising=False)
    worker = DummyWorker(subscription_path="sub/path2", project_id="proj456", running=False)
    result = worker.health_check()
    assert result["worker"] == "DummyWorker"
    assert result["subscription"] == "sub/path2"
    assert result["project"] == "proj456"
    assert result["status"] == "stopped"
    assert result["max_messages"] == 1

def test_health_check_env_var_non_integer(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "not_an_int")
    worker = DummyWorker(subscription_path="sub/path3", project_id="proj789", running=True)
    with pytest.raises(ValueError):
        _ = worker.health_check()

def test_health_check_env_var_zero(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "0")
    worker = DummyWorker(subscription_path="sub/path4", project_id="proj000", running=True)
    result = worker.health_check()
    assert result["max_messages"] == 0

def test_health_check_env_var_negative(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "-10")
    worker = DummyWorker(subscription_path="sub/path5", project_id="proj111", running=False)
    result = worker.health_check()
    assert result["max_messages"] == -10

def test_health_check_subscription_path_empty_string(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "3")
    worker = DummyWorker(subscription_path="", project_id="proj222", running=True)
    result = worker.health_check()
    assert result["subscription"] == ""
    assert result["max_messages"] == 3

def test_health_check_project_id_none(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "2")
    worker = DummyWorker(subscription_path="sub/path6", project_id=None, running=True)
    result = worker.health_check()
    assert result["project"] is None
    assert result["max_messages"] == 2

def test_health_check_running_flag_none(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "4")
    worker = DummyWorker(subscription_path="sub/path7", project_id="proj333", running=None)
    result = worker.health_check()
    assert result["status"] == "stopped"
    assert result["max_messages"] == 4

def test_health_check_class_name_is_correct(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_MESSAGES", "7")
    class CustomWorker(BaseWorker):
        def __init__(self):
            self.subscription_path = "custom/sub"
            self._project_id = "custom_proj"
            self._running = True
    worker = CustomWorker()
    result = worker.health_check()
    assert result["worker"] == "CustomWorker"
    assert result["subscription"] == "custom/sub"
    assert result["project"] == "custom_proj"
    assert result["status"] == "running"
    assert result["max_messages"] == 7
# AI_TEST_AGENT_END function=BaseWorker.health_check
