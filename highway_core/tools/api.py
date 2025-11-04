import logging
from .decorators import tool

logger = logging.getLogger(__name__)

@tool("api.payment_gateway.pay")
def pay(report_id: str, amount: int):
    logger.info(f"--- API STUB ---")
    logger.info(f"Calling payment gateway for report {report_id} for ${amount}.")
    return {"status": "paid", "transaction_id": "txn_abc123"}