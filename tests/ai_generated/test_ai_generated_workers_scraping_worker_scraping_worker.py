from unittest.mock import AsyncMock, MagicMock, patch
from unittest.mock import AsyncMock, patch, MagicMock
from unittest.mock import MagicMock
from unittest.mock import MagicMock, patch
from unittest.mock import patch
from urllib.parse import urlparse
from workers.scraping_worker.scraping_worker import InvalidValue, Context
from workers.scraping_worker.scraping_worker import ScrapingJobCompleteRequest, SubmitScrapingJobMessage
from workers.scraping_worker.scraping_worker import ScrapingWorker
from workers.scraping_worker.scraping_worker import SubmitScrapingJobPayload, ScrapingJobCompleteRequest, Context
import asyncio
import os
import pytest

# AI_TEST_AGENT_START function=ScrapingWorker.__init__
def test_init_with_valid_subscription_name(monkeypatch):
    monkeypatch.setenv("SCRAPING_SUBSCRIPTION", "valid_subscription")
    worker = ScrapingWorker()
    assert hasattr(worker, "subscription_name")
    assert worker.subscription_name == "valid_subscription"

def test_init_raises_value_error_when_env_var_missing(monkeypatch):
    monkeypatch.delenv("SCRAPING_SUBSCRIPTION", raising=False)
    with pytest.raises(ValueError, match="SCRAPING_SUBSCRIPTION environment variable is required"):
        ScrapingWorker()

def test_init_raises_value_error_when_env_var_empty(monkeypatch):
    monkeypatch.setenv("SCRAPING_SUBSCRIPTION", "")
    with pytest.raises(ValueError, match="SCRAPING_SUBSCRIPTION environment variable is required"):
        ScrapingWorker()

def test_init_raises_value_error_when_env_var_is_whitespace(monkeypatch):
    monkeypatch.setenv("SCRAPING_SUBSCRIPTION", "   ")
    with pytest.raises(ValueError, match="SCRAPING_SUBSCRIPTION environment variable is required"):
        ScrapingWorker()

def test_init_accepts_subscription_name_with_special_characters(monkeypatch):
    special_name = "sub$cription_123-!@#"
    monkeypatch.setenv("SCRAPING_SUBSCRIPTION", special_name)
    worker = ScrapingWorker()
    assert worker.subscription_name == special_name

def test_init_accepts_subscription_name_with_numeric_string(monkeypatch):
    numeric_name = "1234567890"
    monkeypatch.setenv("SCRAPING_SUBSCRIPTION", numeric_name)
    worker = ScrapingWorker()
    assert worker.subscription_name == numeric_name

def test_init_accepts_subscription_name_with_long_string(monkeypatch):
    long_name = "a" * 1000
    monkeypatch.setenv("SCRAPING_SUBSCRIPTION", long_name)
    worker = ScrapingWorker()
    assert worker.subscription_name == long_name

def test_init_raises_value_error_when_env_var_is_none(monkeypatch):
    monkeypatch.setenv("SCRAPING_SUBSCRIPTION", "valid")
    with patch.dict("os.environ", {"SCRAPING_SUBSCRIPTION": None}):
        with pytest.raises(ValueError, match="SCRAPING_SUBSCRIPTION environment variable is required"):
            ScrapingWorker()
# AI_TEST_AGENT_END function=ScrapingWorker.__init__

# AI_TEST_AGENT_START function=ScrapingWorker.process_message
@pytest.mark.asyncio
async def test_process_message_successful_flow():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.validate_message = MagicMock()
    worker.generate_context = MagicMock()
    worker._perform_scraping = AsyncMock()
    worker._send_callback = AsyncMock()

    payload_mock = MagicMock()
    payload_mock.job_id = "job123"
    payload_mock.source_id = "source456"
    message_mock = MagicMock()
    message_mock.payload = payload_mock
    worker.validate_message.return_value = message_mock
    worker.generate_context.return_value = MagicMock()
    scraping_result = MagicMock()
    scraping_result.model_dump.return_value = {"key": "value"}
    worker._perform_scraping.return_value = scraping_result

    message_data = {"some": "data"}
    correlation_id = "corr-1"

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.info.assert_any_call("Processing message: %s", message_data)
    worker.validate_message.assert_called_once_with(message_data, SubmitScrapingJobMessage)
    worker.generate_context.assert_called_once_with(message_mock, correlation_id)
    worker.logger.info.assert_any_call(
        "Processing scraping job %s for source %s",
        payload_mock.job_id,
        payload_mock.source_id,
    )
    worker._perform_scraping.assert_awaited_once_with(payload_mock, worker.generate_context.return_value)
    worker._send_callback.assert_awaited_once_with(scraping_result.model_dump())
    assert result is True

@pytest.mark.asyncio
async def test_process_message_callback_raises_error_logs_error_and_returns_true():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.validate_message = MagicMock()
    worker.generate_context = MagicMock()
    worker._perform_scraping = AsyncMock()
    worker._send_callback = AsyncMock(side_effect=Exception("callback failure"))

    payload_mock = MagicMock()
    payload_mock.job_id = "job123"
    payload_mock.source_id = "source456"
    message_mock = MagicMock()
    message_mock.payload = payload_mock
    worker.validate_message.return_value = message_mock
    worker.generate_context.return_value = MagicMock()
    scraping_result = MagicMock()
    scraping_result.model_dump.return_value = {"key": "value"}
    worker._perform_scraping.return_value = scraping_result

    message_data = {"some": "data"}
    correlation_id = "corr-2"

    result = await worker.process_message(message_data, correlation_id)

    worker._send_callback.assert_awaited_once()
    worker.logger.error.assert_any_call("Failed to send callback: %s", Exception("callback failure"))
    assert result is True

@pytest.mark.asyncio
async def test_process_message_validation_raises_and_job_id_extracted_and_failure_callback_sent():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.validate_message = MagicMock(side_effect=Exception("validation error"))
    worker._send_callback = AsyncMock()
    worker.generate_context = MagicMock()
    worker._perform_scraping = AsyncMock()

    message_data = {
        "payload": {
            "job_id": "job789"
        }
    }
    correlation_id = "corr-3"

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", Exception("validation error"))
    worker._send_callback.assert_awaited_once()
    sent_arg = worker._send_callback.call_args[0][0]
    assert sent_arg["job_id"] == "job789"
    assert "Unexpected error during scraping: validation error" in sent_arg["error_message"]
    assert result is True

@pytest.mark.asyncio
async def test_process_message_validation_raises_and_no_job_id_logs_warning_and_returns_true():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.validate_message = MagicMock(side_effect=Exception("validation error"))
    worker._send_callback = AsyncMock()
    worker.generate_context = MagicMock()
    worker._perform_scraping = AsyncMock()

    message_data = {
        "payload": {
            "not_job_id": "missing"
        }
    }
    correlation_id = "corr-4"

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", Exception("validation error"))
    worker._send_callback.assert_not_awaited()
    worker.logger.warning.assert_called_once_with(
        "Could not extract job_id from message_data, skipping failure callback"
    )
    assert result is True

@pytest.mark.asyncio
async def test_process_message_validation_raises_and_payload_not_dict_logs_warning_and_returns_true():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.validate_message = MagicMock(side_effect=Exception("validation error"))
    worker._send_callback = AsyncMock()
    worker.generate_context = MagicMock()
    worker._perform_scraping = AsyncMock()

    message_data = {
        "payload": "not a dict"
    }
    correlation_id = "corr-5"

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", Exception("validation error"))
    worker._send_callback.assert_not_awaited()
    worker.logger.warning.assert_called_once_with(
        "Could not extract job_id from message_data, skipping failure callback"
    )
    assert result is True

@pytest.mark.asyncio
async def test_process_message_perform_scraping_raises_logs_error_and_sends_failure_callback():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    payload_mock = MagicMock()
    payload_mock.job_id = "job999"
    payload_mock.source_id = "source999"
    message_mock = MagicMock()
    message_mock.payload = payload_mock
    worker.validate_message = MagicMock(return_value=message_mock)
    worker.generate_context = MagicMock(return_value=MagicMock())
    worker._perform_scraping = AsyncMock(side_effect=Exception("scraping failure"))
    worker._send_callback = AsyncMock()

    message_data = {"payload": {"job_id": "job999"}}
    correlation_id = "corr-6"

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", Exception("scraping failure"))
    worker._send_callback.assert_awaited_once()
    sent_arg = worker._send_callback.call_args[0][0]
    assert sent_arg["job_id"] == "job999"
    assert "Unexpected error during scraping: scraping failure" in sent_arg["error_message"]
    assert result is True

@pytest.mark.asyncio
async def test_process_message_failure_callback_raises_logs_error_and_returns_true():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.validate_message = MagicMock(side_effect=Exception("validation error"))
    worker._send_callback = AsyncMock(side_effect=Exception("callback failure"))
    worker.generate_context = MagicMock()
    worker._perform_scraping = AsyncMock()

    message_data = {
        "payload": {
            "job_id": "job555"
        }
    }
    correlation_id = "corr-7"

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", Exception("validation error"))
    worker._send_callback.assert_awaited_once()
    worker.logger.error.assert_any_call("Failed to send failure callback: %s", Exception("callback failure"))
    assert result is True

@pytest.mark.asyncio
async def test_process_message_message_data_not_dict_logs_warning_and_returns_true():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.validate_message = MagicMock(side_effect=Exception("validation error"))
    worker._send_callback = AsyncMock()
    worker.generate_context = MagicMock()
    worker._perform_scraping = AsyncMock()

    message_data = "not a dict"
    correlation_id = "corr-8"

    result = await worker.process_message(message_data, correlation_id)

    worker.logger.error.assert_any_call("Message validation/parsing failed: %s", Exception("validation error"))
    worker._send_callback.assert_not_awaited()
    worker.logger.warning.assert_called_once_with(
        "Could not extract job_id from message_data, skipping failure callback"
    )
    assert result is True
# AI_TEST_AGENT_END function=ScrapingWorker.process_message

# AI_TEST_AGENT_START function=ScrapingWorker.generate_context
class DummyPayload:
    def __init__(self, url, source_id='source123', job_id='job456'):
        self.connection = MagicMock()
        self.connection.url = url
        self.source_id = source_id
        self.job_id = job_id

class DummyMessage:
    def __init__(self, url, source_id='source123', job_id='job456'):
        self.payload = DummyPayload(url, source_id, job_id)

def test_generate_context_valid_url_calls_logger_and_returns_context():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    url = "https://example.com/path"
    message = DummyMessage(url)
    correlation_id = "corr-1"

    result = worker.generate_context(message, correlation_id)

    worker.logger.info.assert_called_once_with("[%s] Scraping: %s", correlation_id, url)
    assert isinstance(result, Context)
    assert result.correlation_id == correlation_id
    assert result.source_id == message.payload.source_id
    assert result.job_id == message.payload.job_id

def test_generate_context_raises_invalidvalue_when_url_is_none():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    message = DummyMessage(None)
    correlation_id = "corr-2"

    with pytest.raises(InvalidValue, match="No URL provided"):
        worker.generate_context(message, correlation_id)

    worker.logger.info.assert_not_called()

def test_generate_context_raises_invalidvalue_when_url_is_empty_string():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    message = DummyMessage("")
    correlation_id = "corr-3"

    with pytest.raises(InvalidValue, match="No URL provided"):
        worker.generate_context(message, correlation_id)

    worker.logger.info.assert_not_called()

@pytest.mark.parametrize("url", [
    "http:///missingnetloc",
    "://missing.scheme.com",
    "http://",
    "ftp://",
    "http://?query=param",
])
def test_generate_context_raises_invalidvalue_for_malformed_urls(url):
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    message = DummyMessage(url)
    correlation_id = "corr-4"

    with pytest.raises(InvalidValue, match=f"Invalid URL: {url}"):
        worker.generate_context(message, correlation_id)

    worker.logger.info.assert_not_called()

def test_generate_context_accepts_url_with_non_http_scheme():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    url = "ftp://example.com/resource"
    message = DummyMessage(url)
    correlation_id = "corr-5"

    result = worker.generate_context(message, correlation_id)

    worker.logger.info.assert_called_once_with("[%s] Scraping: %s", correlation_id, url)
    assert result.correlation_id == correlation_id
    assert result.source_id == message.payload.source_id
    assert result.job_id == message.payload.job_id

def test_generate_context_raises_invalidvalue_when_payload_missing_url_attribute():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    class PayloadWithoutUrl:
        def __init__(self):
            self.connection = MagicMock()
            del self.connection.url
            self.source_id = "sourceX"
            self.job_id = "jobY"
    class MessageWithoutUrl:
        def __init__(self):
            self.payload = PayloadWithoutUrl()
    message = MessageWithoutUrl()
    correlation_id = "corr-6"

    with pytest.raises(AttributeError):
        worker.generate_context(message, correlation_id)

    worker.logger.info.assert_not_called()

def test_generate_context_raises_invalidvalue_when_url_is_whitespace_only():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    url = "   "
    message = DummyMessage(url)
    correlation_id = "corr-7"

    with pytest.raises(InvalidValue, match=f"Invalid URL: {url}"):
        worker.generate_context(message, correlation_id)

    worker.logger.info.assert_not_called()

def test_generate_context_raises_invalidvalue_when_url_is_only_scheme():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    url = "http://"
    message = DummyMessage(url)
    correlation_id = "corr-8"

    with pytest.raises(InvalidValue, match=f"Invalid URL: {url}"):
        worker.generate_context(message, correlation_id)

    worker.logger.info.assert_not_called()
# AI_TEST_AGENT_END function=ScrapingWorker.generate_context

# AI_TEST_AGENT_START function=ScrapingWorker._get_scraping_utility
class DummyUtility:
    def __init__(self, context):
        self.context = context

def test_get_scraping_utility_customer_name_match():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = MagicMock()
    customer_name.lower.return_value = "acme corp"
    url = MagicMock()
    url.lower.return_value = "http://example.com"

    dummy_utility = DummyUtility
    worker._CUSTOMER_REGISTRY = {"acme": dummy_utility}
    worker._URL_REGISTRY = []
    worker.logger = MagicMock()

    result = worker._get_scraping_utility(url, customer_name, context)

    assert isinstance(result, dummy_utility)
    assert result.context == context
    worker.logger.info.assert_called_once_with(
        "Using %s for customer: %s", dummy_utility.__name__, customer_name
    )
    customer_name.lower.assert_called_once()
    url.lower.assert_not_called()

def test_get_scraping_utility_url_match():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = MagicMock()
    customer_name.lower.return_value = ""
    url = MagicMock()
    url.lower.return_value = "https://shop.example.com/product/123"

    dummy_utility = DummyUtility
    worker._CUSTOMER_REGISTRY = {}
    worker._URL_REGISTRY = [ (["shop", "product"], dummy_utility) ]
    worker.logger = MagicMock()

    result = worker._get_scraping_utility(url, customer_name, context)

    assert isinstance(result, dummy_utility)
    assert result.context == context
    worker.logger.info.assert_called_once_with(
        "Using %s based on URL: %s", dummy_utility.__name__, url
    )
    customer_name.lower.assert_called_once()
    url.lower.assert_called_once()

def test_get_scraping_utility_customer_name_none_url_match():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = None
    url = MagicMock()
    url.lower.return_value = "https://shop.example.com/product/123"

    dummy_utility = DummyUtility
    worker._CUSTOMER_REGISTRY = {}
    worker._URL_REGISTRY = [ (["shop", "product"], dummy_utility) ]
    worker.logger = MagicMock()

    result = worker._get_scraping_utility(url, customer_name, context)

    assert isinstance(result, dummy_utility)
    assert result.context == context
    worker.logger.info.assert_called_once_with(
        "Using %s based on URL: %s", dummy_utility.__name__, url
    )
    url.lower.assert_called_once()

def test_get_scraping_utility_customer_name_empty_url_no_match():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = ""
    url = MagicMock()
    url.lower.return_value = "https://unknownsite.com/page"

    worker._CUSTOMER_REGISTRY = {"acme": DummyUtility}
    worker._URL_REGISTRY = [ (["shop", "product"], DummyUtility) ]
    worker.logger = MagicMock()

    with pytest.raises(ValueError) as excinfo:
        worker._get_scraping_utility(url, customer_name, context)

    assert "No scraping utility found for customer_name '' and URL" in str(excinfo.value)
    customer_name.lower.assert_called_once()
    url.lower.assert_called_once()
    worker.logger.info.assert_not_called()

def test_get_scraping_utility_customer_name_case_insensitive_match():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = MagicMock()
    customer_name.lower.return_value = "ACME CORP"
    url = MagicMock()
    url.lower.return_value = "http://example.com"

    dummy_utility = DummyUtility
    worker._CUSTOMER_REGISTRY = {"acme": dummy_utility}
    worker._URL_REGISTRY = []
    worker.logger = MagicMock()

    result = worker._get_scraping_utility(url, customer_name, context)

    assert isinstance(result, dummy_utility)
    assert result.context == context
    worker.logger.info.assert_called_once_with(
        "Using %s for customer: %s", dummy_utility.__name__, customer_name
    )
    customer_name.lower.assert_called_once()
    url.lower.assert_not_called()

def test_get_scraping_utility_url_patterns_partial_match_fails():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = MagicMock()
    customer_name.lower.return_value = ""
    url = MagicMock()
    url.lower.return_value = "https://shop.example.com/other"

    dummy_utility = DummyUtility
    worker._CUSTOMER_REGISTRY = {}
    worker._URL_REGISTRY = [ (["shop", "product"], dummy_utility) ]
    worker.logger = MagicMock()

    with pytest.raises(ValueError) as excinfo:
        worker._get_scraping_utility(url, customer_name, context)

    assert "No scraping utility found for customer_name '' and URL" in str(excinfo.value)
    customer_name.lower.assert_called_once()
    url.lower.assert_called_once()
    worker.logger.info.assert_not_called()

def test_get_scraping_utility_customer_name_none_url_no_match():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = None
    url = MagicMock()
    url.lower.return_value = "https://unknownsite.com/page"

    worker._CUSTOMER_REGISTRY = {"acme": DummyUtility}
    worker._URL_REGISTRY = [ (["shop", "product"], DummyUtility) ]
    worker.logger = MagicMock()

    with pytest.raises(ValueError) as excinfo:
        worker._get_scraping_utility(url, customer_name, context)

    assert "No scraping utility found for customer_name 'None' and URL" in str(excinfo.value)
    url.lower.assert_called_once()
    worker.logger.info.assert_not_called()

def test_get_scraping_utility_customer_name_empty_string_url_empty_string():
    worker = ScrapingWorker()
    context = MagicMock()
    customer_name = ""
    url = MagicMock()
    url.lower.return_value = ""

    worker._CUSTOMER_REGISTRY = {"acme": DummyUtility}
    worker._URL_REGISTRY = [ (["shop", "product"], DummyUtility) ]
    worker.logger = MagicMock()

    with pytest.raises(ValueError) as excinfo:
        worker._get_scraping_utility(url, customer_name, context)

    assert "No scraping utility found for customer_name '' and URL ''" in str(excinfo.value)
    customer_name.lower.assert_called_once()
    url.lower.assert_called_once()
    worker.logger.info.assert_not_called()
# AI_TEST_AGENT_END function=ScrapingWorker._get_scraping_utility

# AI_TEST_AGENT_START function=ScrapingWorker._perform_scraping
@pytest.mark.asyncio
async def test_perform_scraping_happy_path():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id="job123",
        customer_name="customerA",
        connection=MagicMock(url="http://example.com"),
        limit=10,
    )
    context = Context()

    mock_utility = MagicMock()
    mock_utility.scrape = MagicMock(return_value=(5, 2, 1, None))

    with patch.object(worker, "_get_scraping_utility", return_value=mock_utility) as mock_get_util, \
         patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:

        mock_to_thread.return_value = (5, 2, 1, None)

        result = await worker._perform_scraping(payload, context)

        mock_get_util.assert_called_once_with(
            url="http://example.com",
            customer_name="customerA",
            context=context,
        )
        mock_to_thread.assert_awaited_once_with(
            mock_utility.scrape, "http://example.com", 10
        )
        assert isinstance(result, ScrapingJobCompleteRequest)
        assert result.job_id == "job123"
        assert result.downloaded_count == 5
        assert result.skipped_count == 2
        assert result.error_count == 1

@pytest.mark.asyncio
async def test_perform_scraping_zero_counts():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id="job_zero",
        customer_name="customerZero",
        connection=MagicMock(url="http://zero.com"),
        limit=0,
    )
    context = Context()

    mock_utility = MagicMock()
    mock_utility.scrape = MagicMock(return_value=(0, 0, 0, None))

    with patch.object(worker, "_get_scraping_utility", return_value=mock_utility), \
         patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:

        mock_to_thread.return_value = (0, 0, 0, None)

        result = await worker._perform_scraping(payload, context)

        assert result.job_id == "job_zero"
        assert result.downloaded_count == 0
        assert result.skipped_count == 0
        assert result.error_count == 0

@pytest.mark.asyncio
async def test_perform_scraping_negative_limit():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id="job_neg",
        customer_name="customerNeg",
        connection=MagicMock(url="http://neg.com"),
        limit=-5,
    )
    context = Context()

    mock_utility = MagicMock()
    mock_utility.scrape = MagicMock(return_value=(3, 1, 0, None))

    with patch.object(worker, "_get_scraping_utility", return_value=mock_utility), \
         patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:

        mock_to_thread.return_value = (3, 1, 0, None)

        result = await worker._perform_scraping(payload, context)

        assert result.job_id == "job_neg"
        assert result.downloaded_count == 3
        assert result.skipped_count == 1
        assert result.error_count == 0

@pytest.mark.asyncio
async def test_perform_scraping_empty_url():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id="job_empty_url",
        customer_name="customerEmpty",
        connection=MagicMock(url=""),
        limit=5,
    )
    context = Context()

    mock_utility = MagicMock()
    mock_utility.scrape = MagicMock(return_value=(1, 0, 0, None))

    with patch.object(worker, "_get_scraping_utility", return_value=mock_utility) as mock_get_util, \
         patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:

        mock_to_thread.return_value = (1, 0, 0, None)

        result = await worker._perform_scraping(payload, context)

        mock_get_util.assert_called_once_with(
            url="",
            customer_name="customerEmpty",
            context=context,
        )
        assert result.job_id == "job_empty_url"
        assert result.downloaded_count == 1
        assert result.skipped_count == 0
        assert result.error_count == 0

@pytest.mark.asyncio
async def test_perform_scraping_utility_scrape_raises_exception():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id="job_exc",
        customer_name="customerExc",
        connection=MagicMock(url="http://exc.com"),
        limit=3,
    )
    context = Context()

    mock_utility = MagicMock()
    mock_utility.scrape = MagicMock(side_effect=ValueError("scrape failed"))

    with patch.object(worker, "_get_scraping_utility", return_value=mock_utility), \
         patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:

        mock_to_thread.side_effect = ValueError("scrape failed")

        with pytest.raises(ValueError, match="scrape failed"):
            await worker._perform_scraping(payload, context)

@pytest.mark.asyncio
async def test_perform_scraping_get_scraping_utility_raises_exception():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id="job_get_util_exc",
        customer_name="customerGetUtilExc",
        connection=MagicMock(url="http://getutil.com"),
        limit=1,
    )
    context = Context()

    with patch.object(worker, "_get_scraping_utility", side_effect=RuntimeError("utility error")):
        with pytest.raises(RuntimeError, match="utility error"):
            await worker._perform_scraping(payload, context)

@pytest.mark.asyncio
async def test_perform_scraping_none_payload_fields():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id=None,
        customer_name=None,
        connection=MagicMock(url=None),
        limit=0,
    )
    context = Context()

    mock_utility = MagicMock()
    mock_utility.scrape = MagicMock(return_value=(0, 0, 0, None))

    with patch.object(worker, "_get_scraping_utility", return_value=mock_utility), \
         patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:

        mock_to_thread.return_value = (0, 0, 0, None)

        result = await worker._perform_scraping(payload, context)

        assert result.job_id is None
        assert result.downloaded_count == 0
        assert result.skipped_count == 0
        assert result.error_count == 0

@pytest.mark.asyncio
async def test_perform_scraping_scrape_returns_unexpected_tuple_length():
    worker = ScrapingWorker()
    payload = SubmitScrapingJobPayload(
        job_id="job_bad_tuple",
        customer_name="customerBadTuple",
        connection=MagicMock(url="http://badtuple.com"),
        limit=2,
    )
    context = Context()

    mock_utility = MagicMock()
    # Return tuple with 3 elements instead of 4
    mock_utility.scrape = MagicMock(return_value=(1, 1, 1))

    with patch.object(worker, "_get_scraping_utility", return_value=mock_utility), \
         patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:

        mock_to_thread.return_value = (1, 1, 1)

        with pytest.raises(ValueError):
            await worker._perform_scraping(payload, context)
# AI_TEST_AGENT_END function=ScrapingWorker._perform_scraping

# AI_TEST_AGENT_START function=ScrapingWorker._send_callback
@pytest.mark.asyncio
async def test_send_callback_no_callback_url_logs_warning_and_returns():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.send_callback = AsyncMock()
    with patch.dict(os.environ, {}, clear=True):
        await worker._send_callback({"data": "value"})
    worker.logger.warning.assert_called_once_with("No scraping callback URL configured")
    worker.send_callback.assert_not_called()

@pytest.mark.asyncio
async def test_send_callback_with_callback_url_and_secret_key_calls_send_callback_with_headers():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.send_callback = AsyncMock()
    env = {
        "SCRAPING_JOB_COMPLETE_URL": "http://callback.url",
        "SECRET_KEY": "secret123"
    }
    callback_data = {"key": "value"}
    with patch.dict(os.environ, env, clear=True):
        await worker._send_callback(callback_data)
    worker.logger.warning.assert_not_called()
    worker.send_callback.assert_awaited_once_with(
        "http://callback.url",
        callback_data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-secret-key": "secret123",
        }
    )

@pytest.mark.asyncio
async def test_send_callback_with_callback_url_and_no_secret_key_calls_send_callback_with_empty_secret_key():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.send_callback = AsyncMock()
    env = {
        "SCRAPING_JOB_COMPLETE_URL": "http://callback.url"
    }
    callback_data = {"key": "value"}
    with patch.dict(os.environ, env, clear=True):
        await worker._send_callback(callback_data)
    worker.logger.warning.assert_not_called()
    worker.send_callback.assert_awaited_once_with(
        "http://callback.url",
        callback_data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-secret-key": "",
        }
    )

@pytest.mark.asyncio
async def test_send_callback_with_empty_callback_data_calls_send_callback():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.send_callback = AsyncMock()
    env = {
        "SCRAPING_JOB_COMPLETE_URL": "http://callback.url",
        "SECRET_KEY": "secret123"
    }
    callback_data = {}
    with patch.dict(os.environ, env, clear=True):
        await worker._send_callback(callback_data)
    worker.logger.warning.assert_not_called()
    worker.send_callback.assert_awaited_once_with(
        "http://callback.url",
        callback_data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-secret-key": "secret123",
        }
    )

@pytest.mark.asyncio
async def test_send_callback_with_none_callback_data_calls_send_callback():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.send_callback = AsyncMock()
    env = {
        "SCRAPING_JOB_COMPLETE_URL": "http://callback.url",
        "SECRET_KEY": "secret123"
    }
    callback_data = None
    with patch.dict(os.environ, env, clear=True):
        await worker._send_callback(callback_data)
    worker.logger.warning.assert_not_called()
    worker.send_callback.assert_awaited_once_with(
        "http://callback.url",
        None,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-secret-key": "secret123",
        }
    )

@pytest.mark.asyncio
async def test_send_callback_send_callback_raises_propagates_exception():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.send_callback = AsyncMock(side_effect=RuntimeError("fail"))
    env = {
        "SCRAPING_JOB_COMPLETE_URL": "http://callback.url",
        "SECRET_KEY": "secret123"
    }
    callback_data = {"key": "value"}
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(RuntimeError, match="fail"):
            await worker._send_callback(callback_data)
    worker.logger.warning.assert_not_called()
    worker.send_callback.assert_awaited_once()

@pytest.mark.asyncio
async def test_send_callback_callback_url_empty_string_logs_warning_and_returns():
    worker = ScrapingWorker()
    worker.logger = MagicMock()
    worker.send_callback = AsyncMock()
    env = {
        "SCRAPING_JOB_COMPLETE_URL": "",
        "SECRET_KEY": "secret123"
    }
    callback_data = {"key": "value"}
    with patch.dict(os.environ, env, clear=True):
        await worker._send_callback(callback_data)
    worker.logger.warning.assert_called_once_with("No scraping callback URL configured")
    worker.send_callback.assert_not_called()
# AI_TEST_AGENT_END function=ScrapingWorker._send_callback
