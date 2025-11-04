import logging
from .decorators import tool

logger = logging.getLogger(__name__)

@tool("email.send_approved_notification")
def send_approved_notification(email: str):
    logger.info(f"--- EMAIL STUB ---")
    logger.info(f"To: {email}")
    logger.info(f"Subject: Your expense report was approved.")

@tool("email.send_denied_notification")
def send_denied_notification(email: str):
    logger.info(f"--- EMAIL STUB ---")
    logger.info(f"To: {email}")
    logger.info(f"Subject: Your expense report was denied.")

@tool("email.escalate_finance_queue")
def escalate_finance_queue(report_id: str, reason: str):
    logger.info(f"--- EMAIL STUB ---")
    logger.info(f"To: finance_queue@company.com")
    logger.info(f"Subject: Overdue Report {report_id}. Reason: {reason}")