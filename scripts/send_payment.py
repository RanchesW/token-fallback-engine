"""шлёт подписанный вебхук оплаты, например чтоб закинуть юзеру токенов руками.

    python scripts/send_payment.py --user 1 --tokens 100
"""
import argparse
import json
import os
import sys
import uuid

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.signing import sign  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--user", type=int, required=True)
parser.add_argument("--tokens", type=int, required=True)
parser.add_argument("--payment-id", default=None)
parser.add_argument("--status", default="succeeded")
parser.add_argument("--url", default=os.environ.get("SERVICE_URL", "http://localhost:8000"))
args = parser.parse_args()

body = json.dumps({
    "payment_id": args.payment_id or f"pay-{uuid.uuid4().hex[:12]}",
    "user_id": args.user,
    "tokens": args.tokens,
    "status": args.status,
}).encode()
secret = os.environ.get("WEBHOOK_SECRET", "dev-secret")
response = httpx.post(f"{args.url}/webhooks/payments", content=body, headers={"X-Signature": sign(body, secret)})
print(response.status_code, response.text)
