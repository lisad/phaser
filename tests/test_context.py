import logging
import pytest

from phaser import Context, ON_ERROR_DROP_ROW, ON_ERROR_STOP_NOW, ON_ERROR_COLLECT, ON_ERROR_WARN


def test_context_process_exception_logging(caplog):
    context = Context()
    exc = KeyError("Test KeyError")

    with caplog.at_level(logging.WARNING):
        context.process_exception(exc, phase=None, step='test_step', row=None)

    log_messages = [record.message for record in caplog.records]
    assert any("Unknown exception handled in executing steps" in message for message in log_messages)


def test_context_process_exception_with_different_policies(caplog):
    context = Context(error_policy=ON_ERROR_COLLECT)
    exc = KeyError("Test KeyError")

    context.process_exception(exc, phase=None, step='test_step', row=None)
    assert context.events['Unknown']['none'][0]['type'] == Context.ERROR

    context.error_policy = ON_ERROR_WARN
    context.process_exception(exc, phase=None, step='test_step', row=None)
    assert context.events['Unknown']['none'][1]['type'] == Context.WARNING

    context.error_policy = ON_ERROR_DROP_ROW
    context.process_exception(exc, phase=None, step='test_step', row=None)
    assert context.events['Unknown']['none'][2]['type'] == Context.DROPPED_ROW

    context.error_policy = ON_ERROR_STOP_NOW
    with caplog.at_level(logging.WARNING):
        with pytest.raises(KeyError):
            context.process_exception(exc, phase=None, step='test_step', row=None)
            log_messages = [record.message for record in caplog.records]
            assert any("Test KeyError" in message for message in log_messages)
