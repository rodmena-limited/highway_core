import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp

from .database import get_db_manager

logger = logging.getLogger(__name__)


class WebhookRunner:
    """
    Asynchronous webhook runner to send webhooks with rate limiting and retry logic.
    """

    def __init__(
        self,
        batch_size: int = 100,
        concurrency: int = 10,
        rate_limit_per_second: int = 10,
    ):
        self.batch_size = batch_size
        self.concurrency = concurrency
        self.rate_limit_per_second = rate_limit_per_second
        self.db_manager = get_db_manager()
        self.semaphore = asyncio.Semaphore(concurrency)
        # Track requests per base URL for rate limiting
        self.requests_per_url = defaultdict(list)

    async def run(self):
        """Main method to run the webhook runner."""
        logger.info("Starting webhook runner...")

        while True:
            try:
                # Get ready webhooks
                webhooks = self.db_manager.get_ready_webhooks(limit=self.batch_size)

                if not webhooks:
                    logger.debug("No webhooks to process, sleeping for 5 seconds...")
                    await asyncio.sleep(5)
                    continue

                logger.info(f"Processing {len(webhooks)} webhooks...")

                # Process webhooks concurrently with semaphore
                tasks = [self._process_webhook(webhook) for webhook in webhooks]
                await asyncio.gather(*tasks, return_exceptions=True)

                # Small delay to prevent overwhelming the system
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in webhook runner: {e}")
                await asyncio.sleep(5)  # Sleep before retrying

    async def _process_webhook(self, webhook_data: Dict[str, Any]):
        """Process a single webhook with rate limiting and retry logic."""
        async with self.semaphore:
            try:
                # Check rate limiting for this base URL
                base_url = self._get_base_url(webhook_data["url"])
                if not self._check_rate_limit(base_url):
                    # Schedule retry and return
                    self.db_manager.schedule_webhook_retry(webhook_data["id"])
                    return

                # Send the webhook
                success, response_status, response_headers, response_body = (
                    await self._send_webhook(webhook_data)
                )

                # Update webhook status
                if success:
                    self.db_manager.update_webhook_response(
                        webhook_data["id"],
                        response_status,
                        response_headers,
                        response_body,
                        success=True,
                    )
                    logger.info(
                        f"Successfully sent webhook {webhook_data['id']} to {webhook_data['url']}"
                    )
                else:
                    # Check if we should retry
                    if webhook_data["retry_count"] < webhook_data["max_retries"]:
                        self.db_manager.schedule_webhook_retry(webhook_data["id"])
                        logger.info(
                            f"Scheduled retry for webhook {webhook_data['id']} (attempt {webhook_data['retry_count'] + 1}/{webhook_data['max_retries']})"
                        )
                    else:
                        # Mark as failed permanently
                        self.db_manager.update_webhook_response(
                            webhook_data["id"],
                            response_status,
                            response_headers,
                            response_body,
                            success=False,
                        )
                        logger.warning(
                            f"Webhook {webhook_data['id']} failed permanently after {webhook_data['max_retries']} retries"
                        )

            except Exception as e:
                logger.error(f"Error processing webhook {webhook_data['id']}: {e}")
                # Schedule retry on exception
                if webhook_data["retry_count"] < webhook_data["max_retries"]:
                    self.db_manager.schedule_webhook_retry(webhook_data["id"])
                    logger.info(
                        f"Scheduled retry for webhook {webhook_data['id']} due to exception"
                    )
                else:
                    # Mark as failed permanently
                    self.db_manager.update_webhook_response(
                        webhook_data["id"],
                        0,  # No HTTP status, since request failed due to exception
                        {},
                        str(e),
                        success=False,
                    )
                    logger.warning(
                        f"Webhook {webhook_data['id']} failed permanently after {webhook_data['max_retries']} retries"
                    )

    async def _send_webhook(
        self, webhook_data: Dict[str, Any]
    ) -> tuple[bool, int, dict, str]:
        """Send a single webhook and return (success, status_code, headers, body)."""
        timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Prepare the request
                method = webhook_data["method"].upper()
                url = webhook_data["url"]
                headers = webhook_data["headers"]
                payload = webhook_data["payload"]

                # Add rate limiting marker to the URL to track requests
                base_url = self._get_base_url(url)
                self._record_request(base_url)

                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=payload,  # Use json parameter to automatically set Content-Type to application/json
                ) as response:
                    response_body = await response.text()
                    response_headers = dict(response.headers)

                    # Consider success if status is 2xx
                    success = 200 <= response.status < 300

                    return success, response.status, response_headers, response_body
        except asyncio.TimeoutError:
            logger.error(f"Timeout while sending webhook to {webhook_data['url']}")
            return False, 0, {}, "Request timeout"
        except aiohttp.ClientError as e:
            logger.error(
                f"Client error while sending webhook to {webhook_data['url']}: {e}"
            )
            return False, 0, {}, str(e)
        except Exception as e:
            logger.error(
                f"Unexpected error while sending webhook to {webhook_data['url']}: {e}"
            )
            return False, 0, {}, str(e)

    def _get_base_url(self, full_url: str) -> str:
        """Extract the base URL from a full URL for rate limiting."""
        from urllib.parse import urlparse

        parsed = urlparse(full_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _check_rate_limit(self, base_url: str) -> bool:
        """Check if we're within rate limits for the base URL."""
        # Get requests in the last second
        current_time = time.time()
        window_start = current_time - 1  # 1-second window

        # Remove outdated requests
        self.requests_per_url[base_url] = [
            req_time
            for req_time in self.requests_per_url[base_url]
            if req_time > window_start
        ]

        # Get rate limit from database (default to instance setting if not found)
        try:
            stats = self.db_manager.get_rate_limit_stats(base_url)
            # For now, use the instance default; in a real system you'd get this from the webhook record
            rate_limit = self.rate_limit_per_second
        except:
            rate_limit = self.rate_limit_per_second

        # Check if we're within limits
        return len(self.requests_per_url[base_url]) < rate_limit

    def _record_request(self, base_url: str):
        """Record a request being made for rate limiting."""
        self.requests_per_url[base_url].append(time.time())


async def run_webhook_runner():
    """Function to run the webhook runner."""
    runner = WebhookRunner(batch_size=100, concurrency=20, rate_limit_per_second=10)
    await runner.run()


if __name__ == "__main__":
    # For testing purposes
    asyncio.run(run_webhook_runner())
