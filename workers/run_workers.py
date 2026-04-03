#!/usr/bin/env python3
"""Run workers. Use DOCUMENT_PROCESSING_SUBSCRIPTION, SCRAPING_SUBSCRIPTION and related env (see .env.example)."""

import asyncio
import sys
import threading
from dotenv import load_dotenv

from shared.logging import get_logger

logger = get_logger(__name__)

load_dotenv("../.env")


def run_document_processing_worker() -> None:
    from workers.document_processing_worker.doc_processing_worker import DocumentProcessingWorker
    worker = DocumentProcessingWorker()
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Document processing worker received shutdown signal")
    finally:
        asyncio.run(worker.shutdown())


def run_revision_processing_worker() -> None:
    from workers.revision_processing_worker.revision_worker import RevisionProcessingWorker
    worker = RevisionProcessingWorker()
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Revision processing worker received shutdown signal")
    finally:
        asyncio.run(worker.shutdown())


def run_scraping_worker() -> None:
    from workers.scraping_worker.scraping_worker import ScrapingWorker
    worker = ScrapingWorker()
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Scraping worker received shutdown signal")
    finally:
        asyncio.run(worker.shutdown())


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Clinical Ops Worker Manager")
    parser.add_argument(
        "--worker",
        choices=["document-processing", "revision-processing", "scraping", "all"],
        default="all",
        help="Which worker to run",
    )
    args = parser.parse_args()

    if args.worker == "all":
        # Run both workers concurrently using threads
        logger.info("Starting all workers...")
        doc_thread = threading.Thread(
            target=run_document_processing_worker,
            name="DocumentProcessingWorker",
            daemon=False
        )
        scraping_thread = threading.Thread(
            target=run_scraping_worker,
            name="ScrapingWorker",
            daemon=False
        )
        revision_thread = threading.Thread(
            target=run_revision_processing_worker,
            name="RevisionProcessingWorker",
            daemon=False
        )

        print("Running document-processing worker")
        doc_thread.start()
        print("Starting scraping worker")
        scraping_thread.start()
        print("Starting revision-processing worker")
        revision_thread.start()

        # Wait for both threads to complete
        try:
            doc_thread.join()
            scraping_thread.join()
            revision_thread.join()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, workers will terminate gracefully")
            doc_thread.join()
            scraping_thread.join()
            revision_thread.join()
    elif args.worker == "document-processing":
        print("Running document-processing worker")
        run_document_processing_worker()
    elif args.worker == "revision-processing":
        run_revision_processing_worker()
    elif args.worker == "scraping":
        print("Starting scraping worker")
        run_scraping_worker()
    else:
        logger.error("Unknown worker type: %s", args.worker)
        sys.exit(1)


if __name__ == "__main__":
    main()
