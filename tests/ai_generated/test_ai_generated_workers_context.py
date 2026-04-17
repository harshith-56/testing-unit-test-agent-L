from workers.context import Context
import pytest

# AI_TEST_AGENT_START function=Context.get
def test_get_existing_attribute_returns_value():
    ctx = Context()
    ctx.foo = 'bar'
    result = ctx.get('foo')
    assert result == 'bar'

def test_get_nonexistent_attribute_returns_default_none():
    ctx = Context()
    result = ctx.get('missing')
    assert result is None

def test_get_nonexistent_attribute_returns_provided_default():
    ctx = Context()
    default_value = 123
    result = ctx.get('missing', default=default_value)
    assert result == default_value

def test_get_with_empty_string_key_returns_default():
    ctx = Context()
    default_value = 'default'
    result = ctx.get('', default=default_value)
    assert result == default_value

def test_get_with_none_key_raises_type_error():
    ctx = Context()
    with pytest.raises(TypeError):
        ctx.get(None)

def test_get_with_integer_key_raises_type_error():
    ctx = Context()
    with pytest.raises(TypeError):
        ctx.get(123)

def test_get_attribute_with_value_none_returns_none_not_default():
    ctx = Context()
    ctx.foo = None
    result = ctx.get('foo', default='default')
    assert result is None

def test_get_attribute_with_boolean_false_returns_false_not_default():
    ctx = Context()
    ctx.flag = False
    result = ctx.get('flag', default=True)
    assert result is False

def test_get_attribute_with_zero_value_returns_zero_not_default():
    ctx = Context()
    ctx.count = 0
    result = ctx.get('count', default=999)
    assert result == 0
# AI_TEST_AGENT_END function=Context.get
