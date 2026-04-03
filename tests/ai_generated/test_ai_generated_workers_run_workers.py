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
@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_start_called_and_shutdown_called(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker
    run_document_processing_worker()
    mock_worker.start.assert_called_once()
    mock_asyncio.run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_keyboard_interrupt_logs_and_shutdown_called(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = KeyboardInterrupt
    mock_worker_cls.return_value = mock_worker
    run_document_processing_worker()
    mock_logger.info.assert_called_once_with("Document processing worker received shutdown signal")
    mock_asyncio.run.assert_called_once_with(mock_worker.shutdown)

@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_logger_info_called_only_once_on_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = KeyboardInterrupt
    mock_worker_cls.return_value = mock_worker
    run_document_processing_worker()
    assert mock_logger.info.call_count == 1
    mock_logger.info.assert_called_with("Document processing worker received shutdown signal")

@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_logger_info_not_called_if_no_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker
    run_document_processing_worker()
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_shutdown_called_even_if_start_raises_non_keyboard_exception(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = RuntimeError("unexpected error")
    mock_worker_cls.return_value = mock_worker
    with pytest.raises(RuntimeError, match="unexpected error"):
        run_document_processing_worker()
    mock_asyncio.run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_shutdown_called_even_if_start_returns_normally(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker
    run_document_processing_worker()
    mock_asyncio.run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_asyncio_run_called_with_shutdown_coroutine(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker
    run_document_processing_worker()
    args, kwargs = mock_asyncio.run.call_args
    shutdown_coro = args[0]
    assert callable(shutdown_coro)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.DocumentProcessingWorker')
def test_run_document_processing_worker_start_raises_unexpected_exception_propagates(mock_worker_cls, mock_logger, mock_asyncio):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = ValueError("fail")
    mock_worker_cls.return_value = mock_worker
    with pytest.raises(ValueError, match="fail"):
        run_document_processing_worker()
    mock_asyncio.run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()
# AI_TEST_AGENT_END function=run_document_processing_worker

# AI_TEST_AGENT_START function=run_revision_processing_worker
@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_happy_path(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_revision_processing_worker()

    mock_worker_cls.assert_called_once_with()
    mock_worker.start.assert_called_once_with()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = KeyboardInterrupt
    mock_worker_cls.return_value = mock_worker

    run_revision_processing_worker()

    mock_worker_cls.assert_called_once_with()
    mock_worker.start.assert_called_once_with()
    mock_logger.info.assert_called_once_with("Revision processing worker received shutdown signal")
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_start_raises_other_exception(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = RuntimeError("unexpected error")
    mock_worker_cls.return_value = mock_worker

    with pytest.raises(RuntimeError, match="unexpected error"):
        run_revision_processing_worker()

    mock_worker_cls.assert_called_once_with()
    mock_worker.start.assert_called_once_with()
    mock_logger.info.assert_not_called()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_shutdown_raises_exception(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker
    mock_asyncio_run.side_effect = RuntimeError("shutdown failure")

    run_revision_processing_worker()

    mock_worker_cls.assert_called_once_with()
    mock_worker.start.assert_called_once_with()
    mock_logger.info.assert_not_called()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_start_and_shutdown_called_once(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_revision_processing_worker()

    assert mock_worker.start.call_count == 1
    assert mock_asyncio_run.call_count == 1
    mock_asyncio_run.assert_called_with(mock_worker.shutdown)

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_logger_not_called_without_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_revision_processing_worker()

    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_logger_called_only_on_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = KeyboardInterrupt
    mock_worker_cls.return_value = mock_worker

    run_revision_processing_worker()

    mock_logger.info.assert_called_once_with("Revision processing worker received shutdown signal")

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_asyncio_run_called_with_coroutine(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_revision_processing_worker()

    args, kwargs = mock_asyncio_run.call_args
    coro = args[0]
    assert hasattr(coro, '__await__')

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.RevisionProcessingWorker')
def test_run_revision_processing_worker_worker_instantiation_called_once(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_revision_processing_worker()

    mock_worker_cls.assert_called_once()
# AI_TEST_AGENT_END function=run_revision_processing_worker

# AI_TEST_AGENT_START function=run_scraping_worker
@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_calls_start_and_shutdown(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_scraping_worker()

    mock_worker.start.assert_called_once()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_handles_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = KeyboardInterrupt
    mock_worker_cls.return_value = mock_worker

    run_scraping_worker()

    mock_worker.start.assert_called_once()
    mock_logger.info.assert_called_once_with("Scraping worker received shutdown signal")
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_shutdown_always_called_even_if_start_raises_other_exception(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = RuntimeError("unexpected error")
    mock_worker_cls.return_value = mock_worker

    with pytest.raises(RuntimeError, match="unexpected error"):
        run_scraping_worker()

    mock_worker.start.assert_called_once()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_shutdown_called_once_when_start_completes_normally(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_scraping_worker()

    mock_worker.start.assert_called_once()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_shutdown_called_once_when_start_raises_non_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = ValueError("bad value")
    mock_worker_cls.return_value = mock_worker

    with pytest.raises(ValueError, match="bad value"):
        run_scraping_worker()

    mock_worker.start.assert_called_once()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)
    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_logger_info_not_called_if_no_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_scraping_worker()

    mock_logger.info.assert_not_called()

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_logger_info_called_only_once_on_keyboard_interrupt(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker.start.side_effect = KeyboardInterrupt
    mock_worker_cls.return_value = mock_worker

    run_scraping_worker()

    mock_logger.info.assert_called_once_with("Scraping worker received shutdown signal")

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_asyncio_run_called_with_correct_coroutine(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker

    run_scraping_worker()

    args, kwargs = mock_asyncio_run.call_args
    coroutine = args[0]
    assert callable(coroutine)
    # The coroutine should be the worker.shutdown method
    assert coroutine == mock_worker.shutdown

@patch('workers.run_workers.asyncio.run')
@patch('workers.run_workers.logger')
@patch('workers.run_workers.ScrapingWorker')
def test_run_scraping_worker_raises_if_asyncio_run_raises(mock_worker_cls, mock_logger, mock_asyncio_run):
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker
    mock_asyncio_run.side_effect = RuntimeError("asyncio run failed")

    with pytest.raises(RuntimeError, match="asyncio run failed"):
        run_scraping_worker()

    mock_worker.start.assert_called_once()
    mock_asyncio_run.assert_called_once_with(mock_worker.shutdown)
# AI_TEST_AGENT_END function=run_scraping_worker

# AI_TEST_AGENT_START function=main
@patch("workers.run_workers.threading.Thread")
@patch("workers.run_workers.logger")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_all_workers_keyboard_interrupt_handled(mock_argparse, mock_logger, mock_thread):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "all"
    mock_argparse.return_value = mock_parser

    mock_doc_thread = MagicMock()
    mock_scraping_thread = MagicMock()
    mock_revision_thread = MagicMock()
    mock_thread.side_effect = [mock_doc_thread, mock_scraping_thread, mock_revision_thread]

    # Simulate join raising KeyboardInterrupt once, then succeeding
    side_effects = [KeyboardInterrupt(), None, None]
    mock_doc_thread.join.side_effect = side_effects
    mock_scraping_thread.join.side_effect = side_effects
    mock_revision_thread.join.side_effect = side_effects

    main()

    mock_argparse.assert_called_once()
    mock_parser.add_argument.assert_called_once_with(
        "--worker",
        choices=["document-processing", "revision-processing", "scraping", "all"],
        default="all",
        help="Which worker to run",
    )
    assert mock_thread.call_count == 3
    mock_doc_thread.start.assert_called_once()
    mock_scraping_thread.start.assert_called_once()
    mock_revision_thread.start.assert_called_once()
    assert mock_doc_thread.join.call_count >= 2
    assert mock_scraping_thread.join.call_count >= 2
    assert mock_revision_thread.join.call_count >= 2
    mock_logger.info.assert_any_call("Starting all workers...")
    mock_logger.info.assert_any_call("Received shutdown signal, workers will terminate gracefully")


@patch("workers.run_workers.threading.Thread")
@patch("workers.run_workers.logger")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_all_workers_start_and_join(mock_argparse, mock_logger, mock_thread):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "all"
    mock_argparse.return_value = mock_parser

    mock_doc_thread = MagicMock()
    mock_scraping_thread = MagicMock()
    mock_revision_thread = MagicMock()
    mock_thread.side_effect = [mock_doc_thread, mock_scraping_thread, mock_revision_thread]

    main()

    mock_argparse.assert_called_once()
    mock_parser.add_argument.assert_called_once_with(
        "--worker",
        choices=["document-processing", "revision-processing", "scraping", "all"],
        default="all",
        help="Which worker to run",
    )
    assert mock_thread.call_count == 3
    mock_doc_thread.start.assert_called_once()
    mock_scraping_thread.start.assert_called_once()
    mock_revision_thread.start.assert_called_once()
    mock_doc_thread.join.assert_called_once()
    mock_scraping_thread.join.assert_called_once()
    mock_revision_thread.join.assert_called_once()
    mock_logger.info.assert_called_once_with("Starting all workers...")


@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_argument_parser_add_argument_called_with_correct_choices(mock_argparse):
    mock_parser = MagicMock()
    mock_argparse.return_value = mock_parser
    mock_parser.parse_args.return_value.worker = "all"

    main()

    mock_parser.add_argument.assert_called_once_with(
        "--worker",
        choices=["document-processing", "revision-processing", "scraping", "all"],
        default="all",
        help="Which worker to run",
    )


@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_document_processing_worker_called(mock_argparse, mock_run_doc):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "document-processing"
    mock_argparse.return_value = mock_parser

    main()

    mock_run_doc.assert_called_once()


@patch("workers.run_workers.run_document_processing_worker")
@patch("workers.run_workers.logger")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_document_processing_worker_called_with_unexpected_worker_value(mock_argparse, mock_logger, mock_run_doc):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "document-processing"
    mock_argparse.return_value = mock_parser

    main()

    mock_run_doc.assert_called_once()
    mock_logger.error.assert_not_called()


@patch("workers.run_workers.run_revision_processing_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_revision_processing_worker_called(mock_argparse, mock_run_revision):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "revision-processing"
    mock_argparse.return_value = mock_parser

    main()

    mock_run_revision.assert_called_once()


@patch("workers.run_workers.run_scraping_worker")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_scraping_worker_called(mock_argparse, mock_run_scraping):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "scraping"
    mock_argparse.return_value = mock_parser

    main()

    mock_run_scraping.assert_called_once()


@patch("workers.run_workers.sys")
@patch("workers.run_workers.logger")
@patch("workers.run_workers.argparse.ArgumentParser")
def test_main_unknown_worker_calls_logger_error_and_exits(mock_argparse, mock_logger, mock_sys):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value.worker = "invalid-worker"
    mock_argparse.return_value = mock_parser

    main()

    mock_logger.error.assert_called_once_with("Unknown worker type: %s", "invalid-worker")
    mock_sys.exit.assert_called_once_with(1)
# AI_TEST_AGENT_END function=main
