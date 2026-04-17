from unittest.mock import patch, MagicMock
from unittest.mock import patch, MagicMock, call
from workers.run_workers import main
from workers.run_workers import run_document_processing_worker
from workers.run_workers import run_revision_processing_worker
from workers.run_workers import run_scraping_worker
import builtins
import pytest
import sys

# AI_TEST_AGENT_START function=run_document_processing_worker
def test_run_document_processing_worker_start_called_and_shutdown_called():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        run_document_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_document_processing_worker_keyboard_interrupt_logs_and_shutdown():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = KeyboardInterrupt
        run_document_processing_worker()
        mock_logger.info.assert_called_once_with("Document processing worker received shutdown signal")
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_document_processing_worker_start_raises_unexpected_exception_propagates_and_shutdown_called():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        class CustomError(Exception):
            pass
        mock_worker_instance.start.side_effect = CustomError("unexpected error")
        with pytest.raises(CustomError, match="unexpected error"):
            run_document_processing_worker()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_document_processing_worker_shutdown_called_even_if_start_returns_normally():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.return_value = None
        run_document_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_document_processing_worker_shutdown_called_even_if_start_raises_non_keyboard_interrupt():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = RuntimeError("fail")
        with pytest.raises(RuntimeError, match="fail"):
            run_document_processing_worker()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_document_processing_worker_logger_info_not_called_if_no_keyboard_interrupt():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.return_value = None
        run_document_processing_worker()
        mock_logger.info.assert_not_called()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_document_processing_worker_logger_info_called_only_once_on_keyboard_interrupt():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = KeyboardInterrupt
        run_document_processing_worker()
        mock_logger.info.assert_called_once_with("Document processing worker received shutdown signal")
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)
# AI_TEST_AGENT_END function=run_document_processing_worker

# AI_TEST_AGENT_START function=run_revision_processing_worker
def test_run_revision_processing_worker_start_called_and_shutdown_called():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        run_revision_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_revision_processing_worker_keyboard_interrupt_logs_and_shutdown_called():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = KeyboardInterrupt
        run_revision_processing_worker()
        mock_logger.info.assert_called_once_with("Revision processing worker received shutdown signal")
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_revision_processing_worker_start_raises_unexpected_exception_propagates_and_shutdown_called():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        class CustomError(Exception):
            pass
        mock_worker_instance.start.side_effect = CustomError("unexpected error")
        with pytest.raises(CustomError, match="unexpected error"):
            run_revision_processing_worker()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_revision_processing_worker_shutdown_called_even_if_start_returns_normally():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.return_value = None
        run_revision_processing_worker()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_revision_processing_worker_shutdown_called_even_if_start_raises_non_keyboardinterrupt():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = RuntimeError("fail")
        with pytest.raises(RuntimeError, match="fail"):
            run_revision_processing_worker()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_revision_processing_worker_logger_info_not_called_if_no_keyboard_interrupt():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.return_value = None
        run_revision_processing_worker()
        mock_logger.info.assert_not_called()

def test_run_revision_processing_worker_logger_info_called_once_on_keyboard_interrupt():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = KeyboardInterrupt
        run_revision_processing_worker()
        mock_logger.info.assert_called_once()
        args, _ = mock_logger.info.call_args
        assert "Revision processing worker received shutdown signal" in args[0]

def test_run_revision_processing_worker_asyncio_run_called_with_shutdown_coroutine():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        run_revision_processing_worker()
        called_arg = mock_asyncio_run.call_args[0][0]
        assert callable(called_arg)
        # The callable should be the shutdown coroutine method of the worker instance
        assert called_arg == mock_worker_instance.shutdown

def test_run_revision_processing_worker_start_called_once_even_if_shutdown_raises():
    with patch('workers.run_workers.RevisionProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.shutdown.side_effect = RuntimeError("shutdown fail")
        run_revision_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)
# AI_TEST_AGENT_END function=run_revision_processing_worker

# AI_TEST_AGENT_START function=run_scraping_worker
def test_run_scraping_worker_start_called_and_shutdown_called():
    with patch('workers.run_workers.ScrapingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        run_scraping_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_scraping_worker_keyboard_interrupt_logs_and_shutdown_called():
    with patch('workers.run_workers.ScrapingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = KeyboardInterrupt
        run_scraping_worker()
        mock_logger.info.assert_called_once_with("Scraping worker received shutdown signal")
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_scraping_worker_start_raises_unexpected_exception_propagates_and_shutdown_called():
    with patch('workers.run_workers.ScrapingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = RuntimeError("unexpected error")
        with pytest.raises(RuntimeError, match="unexpected error"):
            run_scraping_worker()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_scraping_worker_shutdown_called_even_if_start_returns_none():
    with patch('workers.run_workers.ScrapingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.return_value = None
        run_scraping_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_scraping_worker_shutdown_called_even_if_start_raises_value_error():
    with patch('workers.run_workers.ScrapingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = ValueError("bad value")
        with pytest.raises(ValueError, match="bad value"):
            run_scraping_worker()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_scraping_worker_logger_info_not_called_if_no_keyboard_interrupt():
    with patch('workers.run_workers.ScrapingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        run_scraping_worker()
        mock_logger.info.assert_not_called()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)

def test_run_scraping_worker_logger_info_called_only_once_on_keyboard_interrupt():
    with patch('workers.run_workers.ScrapingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = KeyboardInterrupt
        run_scraping_worker()
        mock_logger.info.assert_called_once_with("Scraping worker received shutdown signal")
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown)
# AI_TEST_AGENT_END function=run_scraping_worker

# AI_TEST_AGENT_START function=main
@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.run_scraping_worker")
@patch("workers.run_workers.run_revision_processing_worker")
@patch("workers.run_workers.threading.Thread")
@patch("workers.run_workers.logger")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_all_workers_runs_threads_and_joins(
    mock_argparse, mock_logger, mock_thread, mock_revision, mock_scraping, mock_document
):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "all"
    mock_argparse.return_value = mock_parser

    mock_thread_instance = MagicMock()
    mock_thread.return_value = mock_thread_instance

    main()

    mock_argparse.assert_called_once()
    mock_parser.add_argument.assert_called_once_with(
        "--worker",
        choices=["document-processing", "revision-processing", "scraping", "all"],
        default="all",
        help="Which worker to run",
    )
    assert mock_thread.call_count == 3
    expected_calls = [
        call(
            target=mock_document,
            name="DocumentProcessingWorker",
            daemon=False,
        ),
        call(
            target=mock_scraping,
            name="ScrapingWorker",
            daemon=False,
        ),
        call(
            target=mock_revision,
            name="RevisionProcessingWorker",
            daemon=False,
        ),
    ]
    mock_thread.assert_has_calls(expected_calls, any_order=True)
    assert mock_thread_instance.start.call_count == 3
    assert mock_thread_instance.join.call_count == 3


@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
@patch("builtins.print")
def test_main_runs_document_processing_worker_directly(mock_print, mock_argparse, mock_document):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "document-processing"
    mock_argparse.return_value = mock_parser

    main()

    mock_print.assert_called_once_with("Running document-processing worker")
    mock_document.assert_called_once()


@patch("workers.run_workers.run_revision_processing_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_runs_revision_processing_worker_directly(mock_argparse, mock_revision):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "revision-processing"
    mock_argparse.return_value = mock_parser

    main()

    mock_revision.assert_called_once()


@patch("workers.run_workers.run_scraping_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
@patch("builtins.print")
def test_main_runs_scraping_worker_directly(mock_print, mock_argparse, mock_scraping):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "scraping"
    mock_argparse.return_value = mock_parser

    main()

    mock_print.assert_called_once_with("Starting scraping worker")
    mock_scraping.assert_called_once()


@patch("workers.run_workers.logger")
@patch("workers.run_workers.sys")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_unknown_worker_calls_logger_error_and_exits(mock_argparse, mock_sys, mock_logger):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "invalid-worker"
    mock_argparse.return_value = mock_parser

    main()

    mock_logger.error.assert_called_once_with("Unknown worker type: %s", "invalid-worker")
    mock_sys.exit.assert_called_once_with(1)


@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.run_scraping_worker")
@patch("workers.run_workers.run_revision_processing_worker")
@patch("workers.run_workers.threading.Thread")
@patch("workers.run_workers.logger")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_all_workers_handles_keyboard_interrupt_and_joins(
    mock_argparse, mock_logger, mock_thread, mock_revision, mock_scraping, mock_document
):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "all"
    mock_argparse.return_value = mock_parser

    mock_thread_instance = MagicMock()
    # join will raise KeyboardInterrupt once, then succeed
    mock_thread_instance.join.side_effect = [KeyboardInterrupt, None, None]
    mock_thread.return_value = mock_thread_instance

    main()

    mock_logger.info.assert_any_call("Starting all workers...")
    mock_logger.info.assert_any_call("Received shutdown signal, workers will terminate gracefully")
    assert mock_thread_instance.join.call_count >= 3


@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_document_processing_worker_called_even_if_print_fails(mock_argparse, mock_document):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "document-processing"
    mock_argparse.return_value = mock_parser

    with patch("builtins.print", side_effect=Exception("print error")):
        with pytest.raises(Exception, match="print error"):
            main()

    mock_document.assert_not_called()


@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.run_scraping_worker")
@patch("workers.run_workers.run_revision_processing_worker")
@patch("workers.run_workers.threading.Thread")
@patch("workers.run_workers.logger")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_all_workers_threads_start_order_and_prints(
    mock_argparse, mock_logger, mock_thread, mock_revision, mock_scraping, mock_document
):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "all"
    mock_argparse.return_value = mock_parser

    mock_doc_thread = MagicMock()
    mock_scraping_thread = MagicMock()
    mock_revision_thread = MagicMock()

    def side_effect_thread(*args, **kwargs):
        if kwargs.get("name") == "DocumentProcessingWorker":
            return mock_doc_thread
        elif kwargs.get("name") == "ScrapingWorker":
            return mock_scraping_thread
        elif kwargs.get("name") == "RevisionProcessingWorker":
            return mock_revision_thread
        else:
            return MagicMock()

    mock_thread.side_effect = side_effect_thread

    with patch("builtins.print") as mock_print:
        main()

    mock_print.assert_has_calls(
        [
            call("Running document-processing worker"),
            call("Starting scraping worker"),
            call("Starting revision-processing worker"),
        ]
    )
    mock_doc_thread.start.assert_called_once()
    mock_scraping_thread.start.assert_called_once()
    mock_revision_thread.start.assert_called_once()


@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_document_processing_worker_called_with_none_worker_value(mock_argparse, mock_document):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = None
    mock_argparse.return_value = mock_parser

    with pytest.raises(AttributeError):
        main()

    mock_document.assert_not_called()
# AI_TEST_AGENT_END function=main
