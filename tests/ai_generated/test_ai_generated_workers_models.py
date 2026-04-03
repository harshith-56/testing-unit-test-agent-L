from workers.models import SubmitScrapingJobPayload
import json
import pytest

# AI_TEST_AGENT_START function=SubmitScrapingJobPayload.parse_connection
def test_parse_connection_with_valid_json_string():
    input_str = '{"key": "value", "num": 123}'
    result = SubmitScrapingJobPayload.parse_connection(input_str)
    assert isinstance(result, dict)
    assert result["key"] == "value"
    assert result["num"] == 123

def test_parse_connection_with_invalid_json_string_raises_value_error():
    input_str = '{"key": "value", "num": 123'  # missing closing brace
    with pytest.raises(ValueError, match="connection must be valid JSON"):
        SubmitScrapingJobPayload.parse_connection(input_str)

def test_parse_connection_with_non_string_input_returns_input_unchanged():
    input_dict = {"foo": "bar"}
    result = SubmitScrapingJobPayload.parse_connection(input_dict)
    assert result is input_dict

def test_parse_connection_with_empty_string_raises_value_error():
    input_str = ""
    with pytest.raises(ValueError, match="connection must be valid JSON"):
        SubmitScrapingJobPayload.parse_connection(input_str)

def test_parse_connection_with_string_null_returns_none():
    input_str = "null"
    result = SubmitScrapingJobPayload.parse_connection(input_str)
    assert result is None

def test_parse_connection_with_string_number_returns_number():
    input_str = "123"
    result = SubmitScrapingJobPayload.parse_connection(input_str)
    assert result == 123

def test_parse_connection_with_string_array_returns_list():
    input_str = '["a", "b", "c"]'
    result = SubmitScrapingJobPayload.parse_connection(input_str)
    assert isinstance(result, list)
    assert result == ["a", "b", "c"]

def test_parse_connection_with_none_input_returns_none():
    input_none = None
    result = SubmitScrapingJobPayload.parse_connection(input_none)
    assert result is None

def test_parse_connection_with_boolean_input_returns_boolean():
    input_bool = True
    result = SubmitScrapingJobPayload.parse_connection(input_bool)
    assert result is True
# AI_TEST_AGENT_END function=SubmitScrapingJobPayload.parse_connection
