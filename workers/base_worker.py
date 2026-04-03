import asyncio
import json
import os
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message
from pydantic import ValidationError

from shared.logging import get_logger
from tools.pubsub import GCPConfig, PubSubClient


class BaseWorker(ABC):
    """Base class for Google Pub/Sub workers."""

    def __init__(self, subscription_name: Optional[str] = None):
        self.logger = get_logger(self.__class__.__name__)
        if not subscription_name:
            subscription_name = os.getenv("DOCUMENT_PROCESSING_SUBSCRIPTION") or os.getenv(
                "SCRAPING_SUBSCRIPTION"
            )
        if subscription_name.startswith("projects/"):
            self.subscription_path = subscription_name
        else:
            project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
            if not project_id:
                raise ValueError("GOOGLE_CLOUD_PROJECT_ID required when subscription is not a full path")
            self.subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"

        self.logger.info("Using subscription path: %s", self.subscription_path)
        flow_max = int(os.getenv("FLOW_CONTROL_MAX_MESSAGES", "5"))
        self.flow_control = pubsub_v1.types.FlowControl(max_messages=flow_max)

        gcp_config = GCPConfig.load_from_env()
        pubsub = PubSubClient(gcp_config)
        self.subscriber = pubsub.subscriber
        self._project_id = gcp_config.project_id

        callback_timeout = int(os.getenv("CALLBACK_TIMEOUT", "3000"))
        self._callback_timeout = callback_timeout
        self.http_client = httpx.AsyncClient(timeout=callback_timeout)
        self._running = False
        max_workers = int(os.getenv("WORKER_MAX_WORKERS", "1"))
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    @abstractmethod
    async def process_message(self, message_data: Dict[str, Any], correlation_id: str) -> bool:
        """Process a single message. Returns True if processed successfully."""
        raise NotImplementedError()

    async def send_callback(
        self,
        callback_url: str,
        callback_data: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        auth_token: Optional[str] = None,
    ) -> bool:
        if not callback_url:
            self.logger.info("No callback URL provided, would send: %s", callback_data)
            return True
        request_headers = headers or {}
        if auth_token:
            request_headers["Authorization"] = f"Bearer {auth_token}"
        self.logger.info("Sending callback to: %s", callback_url)
        try:
            async with httpx.AsyncClient(timeout=self._callback_timeout) as client:
                response = await client.post(
                    callback_url, json=callback_data, headers=request_headers
                )
                response.raise_for_status()
            self.logger.info("Callback sent successfully: %s", response.status_code)
            return True
        except httpx.HTTPStatusError as e:
            self.logger.error("Callback HTTP error: %s - %s", e.response.status_code, e.response.text)
            return False
        except httpx.TimeoutException:
            self.logger.error("Callback timeout for URL: %s", callback_url)
            return False
        except Exception as e:
            self.logger.error("Failed to send callback: %s", e)
            return False

    def validate_message(self, message_data: Dict[str, Any], message_class) -> Any:
        try:
            return message_class(**message_data)
        except ValidationError as e:
            self.logger.error("Message validation failed: %s", e)
            raise
        except Exception as e:
            self.logger.error("Unexpected validation error: %s", e)
            raise

    def create_callback_data(
        self,
        correlation_id: str,
        job_id: str,
        source_id: str,
        status: str,
        message: str,
        result_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        data = {
            "correlation_id": correlation_id,
            "job_id": job_id,
            "source_id": source_id,
            "status": status,
            "message": message,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        if result_data:
            data["result"] = result_data
        return data

    def _callback(self, message: Message) -> None:
        try:
            message.ack()
            message_text = message.data.decode("utf-8")
            message_data = json.loads(message_text)
            correlation_id = (
                message_data.get("payload", {}).get("correlation_id")
                or message_data.get("meta", {}).get("message_id")
                or "unknown"
            )
            self.logger.info("Processing message with correlation_id: %s", correlation_id)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                success = loop.run_until_complete(self.process_message(message_data, correlation_id))
                if success:
                    self.logger.info("Successfully processed message: %s", correlation_id)
                else:
                    self.logger.error("Failed to process message: %s", correlation_id)
            finally:
                loop.close()
        except json.JSONDecodeError as e:
            self.logger.error("Failed to parse message JSON: %s", e)
        except Exception as e:
            self.logger.error("Unexpected error processing message: %s", e)

    def start(self) -> None:
        self.logger.info("Starting worker for subscription: %s", self.subscription_path)
        self._running = True
        try:
            self.logger.info("Testing subscription access...")
            self.subscriber.get_subscription(request={"subscription": self.subscription_path})
            self.logger.info("✅ Subscription access verified")
            streaming_pull_future = self.subscriber.subscribe(
                self.subscription_path,
                callback=self._callback,
                flow_control=self.flow_control,
            )
            self.logger.info("Worker started successfully. Listening for messages...")
            try:
                streaming_pull_future.result()
            except KeyboardInterrupt:
                self.logger.info("Received shutdown signal")
                streaming_pull_future.cancel()
                try:
                    streaming_pull_future.result()
                except Exception:
                    pass
        except Exception as e:
            self.logger.error("Error starting worker: %s", e)
            raise
        finally:
            self._running = False
            self._executor.shutdown(wait=True)
            self.logger.info("Worker stopped")

    def stop(self) -> None:
        self._running = False
        self.logger.info("Stopping worker...")

    async def shutdown(self) -> None:
        self.logger.info("Shutting down worker...")
        await self.http_client.aclose()
        self.stop()

    @property
    def is_running(self) -> bool:
        return self._running

    def health_check(self) -> Dict[str, Any]:
        return {
            "worker": self.__class__.__name__,
            "subscription": self.subscription_path,
            "project": self._project_id,
            "status": "running" if self._running else "stopped",
            "max_messages": int(os.getenv("WORKER_MAX_MESSAGES", "1")),
        }
