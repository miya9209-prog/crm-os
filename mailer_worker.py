
import json
from pathlib import Path
from datetime import datetime
import os
import pandas as pd
from sendgrid import SendGridAPIClient

BASE_DIR = Path(__file__).resolve().parent
MAIL_QUEUE_DIR = BASE_DIR / "mail_queue"
MAIL_LOG_DIR = BASE_DIR / "mail_logs"
MAIL_QUEUE_DIR.mkdir(exist_ok=True)
MAIL_LOG_DIR.mkdir(exist_ok=True)
API_KEY = os.getenv("SENDGRID_API_KEY", "")
FROM_EMAIL = os.getenv("SENDGRID_FROM_EMAIL", "")
FROM_NAME = os.getenv("SENDGRID_FROM_NAME", "MISHARP")
REPLY_TO = os.getenv("SENDGRID_REPLY_TO", FROM_EMAIL)

def send(subject, html, recipients):
    client = SendGridAPIClient(API_KEY)
    payload = {
        "from": {"email": FROM_EMAIL, "name": FROM_NAME},
        "subject": subject,
        "personalizations": [{"to": [{"email": e}]} for e in recipients],
        "content": [{"type": "text/html", "value": html}],
        "reply_to": {"email": REPLY_TO},
    }
    return client.client.mail.send.post(request_body=payload)

def main():
    now = pd.Timestamp.now()
    for p in sorted(MAIL_QUEUE_DIR.glob("mail_job_*.json")):
        job = json.loads(p.read_text(encoding="utf-8"))
        if job.get("status") == "sent":
            continue
        due = pd.to_datetime(job.get("scheduled_at"), errors="coerce")
        if pd.isna(due) or due > now:
            continue
        try:
            res = send(job.get("subject", ""), job.get("html", ""), job.get("recipients", []))
            job["status"] = "sent"
            job["result"] = f"status {res.status_code}"
        except Exception as e:
            job["status"] = "failed"
            job["result"] = str(e)
        job["sent_at"] = datetime.now().isoformat(timespec="seconds")
        p.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
        (MAIL_LOG_DIR / f"{p.stem}_{job['status']}.json").write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
