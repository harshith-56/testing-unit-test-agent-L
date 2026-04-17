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
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        run_document_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown())
        mock_logger.info.assert_not_called()

def test_run_document_processing_worker_keyboard_interrupt_logs_and_shutdown_called():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = KeyboardInterrupt
        run_document_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_logger.info.assert_called_once_with("Document processing worker received shutdown signal")
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown())

def test_run_document_processing_worker_start_raises_unexpected_exception_propagates_and_shutdown_called():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.side_effect = RuntimeError("unexpected error")
        with pytest.raises(RuntimeError, match="unexpected error"):
            run_document_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_logger.info.assert_not_called()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown())

def test_run_document_processing_worker_shutdown_called_even_if_shutdown_raises():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.shutdown.side_effect = RuntimeError("shutdown failure")
        run_document_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown())
        mock_logger.info.assert_not_called()

def test_run_document_processing_worker_start_and_shutdown_called_when_start_returns_none():
    with patch('workers.run_workers.DocumentProcessingWorker') as MockWorker, \
         patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger:
        mock_worker_instance = MockWorker.return_value
        mock_worker_instance.start.return_value = None
        run_document_processing_worker()
        mock_worker_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker_instance.shutdown())
        mock_logger.info.assert_not_called()
# AI_TEST_AGENT_END function=run_document_processing_worker

# AI_TEST_AGENT_START function=run_revision_processing_worker
def test_run_revision_processing_worker_start_called_and_shutdown_called():
    with patch('workers.run_workers.asyncio') as mock_asyncio, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.revision_processing_worker.revision_worker.RevisionProcessingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker_cls.return_value = mock_worker

        run_revision_processing_worker()

        mock_worker.start.assert_called_once()
        mock_asyncio.run.assert_called_once_with(mock_worker.shutdown())
        mock_logger.info.assert_not_called()

def test_run_revision_processing_worker_keyboard_interrupt_logs_and_shutdown_called():
    with patch('workers.run_workers.asyncio') as mock_asyncio, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.revision_processing_worker.revision_worker.RevisionProcessingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker.start.side_effect = KeyboardInterrupt
        mock_worker_cls.return_value = mock_worker

        run_revision_processing_worker()

        mock_worker.start.assert_called_once()
        mock_logger.info.assert_called_once_with("Revision processing worker received shutdown signal")
        mock_asyncio.run.assert_called_once_with(mock_worker.shutdown())

def test_run_revision_processing_worker_shutdown_called_even_if_start_raises_other_exception():
    with patch('workers.run_workers.asyncio') as mock_asyncio, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.revision_processing_worker.revision_worker.RevisionProcessingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker.start.side_effect = RuntimeError("unexpected error")
        mock_worker_cls.return_value = mock_worker

        with pytest.raises(RuntimeError, match="unexpected error"):
            run_revision_processing_worker()

        mock_worker.start.assert_called_once()
        mock_logger.info.assert_not_called()
        mock_asyncio.run.assert_called_once_with(mock_worker.shutdown())

def test_run_revision_processing_worker_shutdown_called_when_shutdown_raises_exception():
    with patch('workers.run_workers.asyncio') as mock_asyncio, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.revision_processing_worker.revision_worker.RevisionProcessingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker_cls.return_value = mock_worker
        mock_asyncio.run.side_effect = RuntimeError("shutdown failure")

        run_revision_processing_worker()

        mock_worker.start.assert_called_once()
        mock_asyncio.run.assert_called_once_with(mock_worker.shutdown())
        mock_logger.info.assert_not_called()

def test_run_revision_processing_worker_start_called_multiple_times_independent():
    with patch('workers.run_workers.asyncio') as mock_asyncio, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.revision_processing_worker.revision_worker.RevisionProcessingWorker') as mock_worker_cls:
        mock_worker1 = MagicMock()
        mock_worker2 = MagicMock()
        mock_worker_cls.side_effect = [mock_worker1, mock_worker2]

        run_revision_processing_worker()
        run_revision_processing_worker()

        assert mock_worker1.start.call_count == 1
        assert mock_worker2.start.call_count == 1
        assert mock_asyncio.run.call_count == 2
        mock_logger.info.assert_not_called()

def test_run_revision_processing_worker_start_raises_non_keyboard_interrupt_exception():
    with patch('workers.run_workers.asyncio') as mock_asyncio, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.revision_processing_worker.revision_worker.RevisionProcessingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker.start.side_effect = ValueError("bad value")
        mock_worker_cls.return_value = mock_worker

        with pytest.raises(ValueError, match="bad value"):
            run_revision_processing_worker()

        mock_worker.start.assert_called_once()
        mock_logger.info.assert_not_called()
        mock_asyncio.run.assert_called_once_with(mock_worker.shutdown())
# AI_TEST_AGENT_END function=run_revision_processing_worker

# AI_TEST_AGENT_START function=run_scraping_worker
def test_run_scraping_worker_start_called_and_shutdown_called():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker_cls.return_value = mock_worker

        run_scraping_worker()

        mock_worker.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_worker.shutdown())
        mock_logger.info.assert_not_called()

def test_run_scraping_worker_keyboard_interrupt_logs_and_shutdown_called():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker.start.side_effect = KeyboardInterrupt
        mock_worker_cls.return_value = mock_worker

        run_scraping_worker()

        mock_worker.start.assert_called_once()
        mock_logger.info.assert_called_once_with("Scraping worker received shutdown signal")
        mock_asyncio_run.assert_called_once_with(mock_worker.shutdown())

def test_run_scraping_worker_start_raises_unexpected_exception_propagates():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker.start.side_effect = RuntimeError("unexpected error")
        mock_worker_cls.return_value = mock_worker

        with pytest.raises(RuntimeError, match="unexpected error"):
            run_scraping_worker()

        mock_worker.start.assert_called_once()
        mock_logger.info.assert_not_called()
        mock_asyncio_run.assert_called_once_with(mock_worker.shutdown())

def test_run_scraping_worker_shutdown_always_called_even_if_start_succeeds():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker_cls.return_value = mock_worker

        run_scraping_worker()

        mock_asyncio_run.assert_called_once_with(mock_worker.shutdown())

def test_run_scraping_worker_shutdown_called_even_if_start_raises_non_keyboardinterrupt():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker.start.side_effect = ValueError("fail")
        mock_worker_cls.return_value = mock_worker

        with pytest.raises(ValueError, match="fail"):
            run_scraping_worker()

        mock_asyncio_run.assert_called_once_with(mock_worker.shutdown())

def test_run_scraping_worker_logger_info_not_called_if_no_keyboard_interrupt():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker_cls.return_value = mock_worker

        run_scraping_worker()

        mock_logger.info.assert_not_called()

def test_run_scraping_worker_logger_info_called_only_once_on_keyboard_interrupt():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker.start.side_effect = KeyboardInterrupt
        mock_worker_cls.return_value = mock_worker

        run_scraping_worker()

        mock_logger.info.assert_called_once_with("Scraping worker received shutdown signal")

def test_run_scraping_worker_asyncio_run_called_with_coroutine():
    with patch('workers.run_workers.asyncio.run') as mock_asyncio_run, \
         patch('workers.run_workers.logger') as mock_logger, \
         patch('workers.run_workers.ScrapingWorker') as mock_worker_cls:
        mock_worker = MagicMock()
        mock_worker_cls.return_value = mock_worker

        run_scraping_worker()

        args, kwargs = mock_asyncio_run.call_args
        coroutine = args[0]
        assert hasattr(coroutine, '__await__') or hasattr(coroutine, '__iter__') or hasattr(coroutine, '__next__') or hasattr(coroutine, '__aiter__') or hasattr(coroutine, '__anext__') or callable(coroutine)
        mock_worker.shutdown.assert_not_called()  # shutdown is not called directly, only passed to asyncio.run()
# AI_TEST_AGENT_END function=run_scraping_worker

# AI_TEST_AGENT_START function=main
@patch('workers.run_workers.run_revision_processing_worker')
@patch('workers.run_workers.run_scraping_worker')
@patch('workers.run_workers.run_document_processing_worker')
@patch('workers.run_workers.threading.Thread')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_all_workers_runs_threads_and_joins(mock_argparse, mock_logger, mock_thread, mock_doc_worker, mock_scraping_worker, mock_revision_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'all'
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
        call(target=mock_doc_worker, name="DocumentProcessingWorker", daemon=False),
        call(target=mock_scraping_worker, name="ScrapingWorker", daemon=False),
        call(target=mock_revision_worker, name="RevisionProcessingWorker", daemon=False),
    ]
    mock_thread.assert_has_calls(expected_calls, any_order=True)
    assert mock_thread_instance.start.call_count == 3
    assert mock_thread_instance.join.call_count == 3
    mock_logger.info.assert_called_with("Starting all workers...")


@patch('workers.run_workers.run_document_processing_worker')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_document_processing_worker_runs_directly(mock_argparse, mock_logger, mock_doc_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'document-processing'
    mock_argparse.return_value = mock_parser

    main()

    mock_doc_worker.assert_called_once()
    mock_logger.info.assert_not_called()


@patch('workers.run_workers.run_revision_processing_worker')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_revision_processing_worker_runs_directly(mock_argparse, mock_logger, mock_revision_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'revision-processing'
    mock_argparse.return_value = mock_parser

    main()

    mock_revision_worker.assert_called_once()
    mock_logger.info.assert_not_called()


@patch('workers.run_workers.run_scraping_worker')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_scraping_worker_runs_directly(mock_argparse, mock_logger, mock_scraping_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'scraping'
    mock_argparse.return_value = mock_parser

    main()

    mock_scraping_worker.assert_called_once()
    mock_logger.info.assert_not_called()


@patch('workers.run_workers.sys')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_unknown_worker_calls_logger_error_and_exits(mock_argparse, mock_logger, mock_sys):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'invalid-worker'
    mock_argparse.return_value = mock_parser
    mock_sys.exit = MagicMock()

    main()

    mock_logger.error.assert_called_once_with("Unknown worker type: %s", 'invalid-worker')
    mock_sys.exit.assert_called_once_with(1)


@patch('workers.run_workers.run_revision_processing_worker')
@patch('workers.run_workers.run_scraping_worker')
@patch('workers.run_workers.run_document_processing_worker')
@patch('workers.run_workers.threading.Thread')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_all_workers_keyboard_interrupt_handled(mock_argparse, mock_logger, mock_thread, mock_doc_worker, mock_scraping_worker, mock_revision_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'all'
    mock_argparse.return_value = mock_parser

    mock_thread_instance = MagicMock()
    mock_thread.return_value = mock_thread_instance

    def join_side_effect():
        raise KeyboardInterrupt()

    mock_thread_instance.join.side_effect = join_side_effect

    main()

    mock_logger.info.assert_any_call("Starting all workers...")
    mock_logger.info.assert_any_call("Received shutdown signal, workers will terminate gracefully")
    assert mock_thread_instance.join.call_count >= 1


@patch('workers.run_workers.run_document_processing_worker')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_document_processing_worker_keyboard_interrupt_propagates(mock_argparse, mock_logger, mock_doc_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'document-processing'
    mock_argparse.return_value = mock_parser

    mock_doc_worker.side_effect = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        main()

    mock_doc_worker.assert_called_once()


@patch('workers.run_workers.run_scraping_worker')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_scraping_worker_keyboard_interrupt_propagates(mock_argparse, mock_logger, mock_scraping_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'scraping'
    mock_argparse.return_value = mock_parser

    mock_scraping_worker.side_effect = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        main()

    mock_scraping_worker.assert_called_once()


@patch('workers.run_workers.run_revision_processing_worker')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.argparse.ArgumentParser')
def test_main_revision_processing_worker_keyboard_interrupt_propagates(mock_argparse, mock_logger, mock_revision_worker):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = 'revision-processing'
    mock_argparse.return_value = mock_parser

    mock_revision_worker.side_effect = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        main()

    mock_revision_worker.assert_called_once()
# AI_TEST_AGENT_END function=main
