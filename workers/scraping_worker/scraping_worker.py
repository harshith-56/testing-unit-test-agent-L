import asyncio
import os
from typing import Any, Dict

from google.auth.exceptions import InvalidValue
from urllib.parse import urlparse

from scraping.aetna_scraping_utility import AetnaScrapingUtility
from scraping.anthem_reimbursement_scraping_utility import AnthemReimbursementScrapingUtility
from scraping.anthem_scraping_utility import AnthemScrapingUtility
from scraping.base_scraping_utility import BaseScrapingUtility
from scraping.emblemhealth_scraping_utility import EmblemHealthScrapingUtility
from scraping.hcsc_scraping_utility import HCSCScrapingUtility
from scraping.molina_scraping_utility import MolinaScrapingUtility
from workers.base_worker import BaseWorker
from workers.context import Context
from workers.models import (
    ScrapingJobCompleteRequest,
    SubmitScrapingJobMessage,
    SubmitScrapingJobPayload,
)


class ScrapingWorker(BaseWorker):
    """Worker for processing web scraping jobs from Pub/Sub."""

    _CUSTOMER_REGISTRY = {
        "anthem reimbursement": AnthemReimbursementScrapingUtility,
        "elevance": AnthemScrapingUtility,
        "hcsc": HCSCScrapingUtility,
        "aetna": AetnaScrapingUtility,
        "molina": MolinaScrapingUtility,
        "emblem health": EmblemHealthScrapingUtility,
    }

    _URL_REGISTRY = [
        (("anthem.com", "reimbursement"), AnthemReimbursementScrapingUtility),
        (("anthem.com",), AnthemScrapingUtility),
        (("medicalpolicy.hcsc.com",), HCSCScrapingUtility),
        (("aetna.com",), AetnaScrapingUtility),
        (("molinaclinicalpolicy.com",), MolinaScrapingUtility),
        (("emblemhealth.com",), EmblemHealthScrapingUtility)
    ]

    def __init__(self) -> None:
        subscription_name = os.getenv("SCRAPING_SUBSCRIPTION")
        if not subscription_name:
            raise ValueError("SCRAPING_SUBSCRIPTION environment variable is required")
        super().__init__(subscription_name=subscription_name)

    async def process_message(
        self, message_data: Dict[str, Any], correlation_id: str
    ) -> bool:
        try:
            self.logger.info("Processing message: %s", message_data)
            message = self.validate_message(message_data, SubmitScrapingJobMessage)
            context = self.generate_context(message, correlation_id)
            payload = message.payload
            self.logger.info(
                "Processing scraping job %s for source %s",
                payload.job_id,
                payload.source_id,
            )
            result = await self._perform_scraping(payload, context)
            try:
                await self._send_callback(result.model_dump())
            except Exception as callback_error:
                self.logger.error("Failed to send callback: %s", callback_error)
            return True
        except Exception as e:
            self.logger.error("Message validation/parsing failed: %s", e)
            error_message = f"Unexpected error during scraping: {e}"
            extracted_job_id = None
            if isinstance(message_data, dict):
                payload_data = message_data.get("payload", {})
                if isinstance(payload_data, dict):
                    extracted_job_id = payload_data.get("job_id")
            if extracted_job_id:
                failure_data = ScrapingJobCompleteRequest(
                    job_id=extracted_job_id,
                    skipped_count=0,
                    downloaded_count=0,
                    error_count=0,
                    error_message=error_message,
                )
                try:
                    await self._send_callback(failure_data.model_dump())
                except Exception as callback_error:
                    self.logger.error("Failed to send failure callback: %s", callback_error)
            else:
                self.logger.warning(
                    "Could not extract job_id from message_data, skipping failure callback"
                )
            return True

    def generate_context(
        self, message: SubmitScrapingJobMessage, correlation_id: str
    ) -> Context:
        payload = message.payload
        url = payload.connection.url
        if not url:
            raise InvalidValue("No URL provided")
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise InvalidValue(f"Invalid URL: {url}")
        self.logger.info("[%s] Scraping: %s", correlation_id, url)
        return Context(
            correlation_id=correlation_id,
            source_id=payload.source_id,
            job_id=payload.job_id,
        )

    def _get_scraping_utility(
        self, url: str, customer_name: str, context: Context
    ) -> BaseScrapingUtility:
        if customer_name:
            name_lower = customer_name.lower()
            for key, utility_class in self._CUSTOMER_REGISTRY.items():
                if key in name_lower:
                    self.logger.info("Using %s for customer: %s", utility_class.__name__, customer_name)
                    return utility_class(context=context)

        url_lower = url.lower()
        for patterns, utility_class in self._URL_REGISTRY:
            if all(p in url_lower for p in patterns):
                self.logger.info("Using %s based on URL: %s", utility_class.__name__, url)
                return utility_class(context=context)

        raise ValueError(
            f"No scraping utility found for customer_name '{customer_name}' and URL '{url}'"
        )

    async def _perform_scraping(
        self, payload: SubmitScrapingJobPayload, context: Context
    ) -> ScrapingJobCompleteRequest:
        utility = self._get_scraping_utility(
            url=payload.connection.url,
            customer_name=payload.customer_name,
            context=context,
        )

        downloaded_count, skipped_count, error_count, _ = await asyncio.to_thread(
            utility.scrape, payload.connection.url, payload.limit
        )
        return ScrapingJobCompleteRequest(
            job_id=payload.job_id,
            downloaded_count=downloaded_count,
            skipped_count=skipped_count,
            error_count=error_count,
        )

    async def _send_callback(self, callback_data: dict) -> None:
        callback_url = os.getenv("SCRAPING_JOB_COMPLETE_URL")
        if not callback_url:
            self.logger.warning("No scraping callback URL configured")
            return
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-secret-key": os.getenv("SECRET_KEY", ""),
        }
        await self.send_callback(callback_url, callback_data, headers=headers)
