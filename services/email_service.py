"""
Email notification service (Gmail SMTP).
Requires EMAIL_SENDER and EMAIL_APP_PASSWORD in services/config.py.

To generate a Gmail App Password:
  Google Account → Security → 2-Step Verification → App passwords
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(subject: str, body: str, recipients: list, sender: str, app_password: str) -> bool:
    """Send an HTML email via Gmail SMTP SSL. Returns True on success."""
    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, app_password)
            server.sendmail(sender, recipients, msg.as_string())
        return True
    except Exception as e:
        print(f"Email send failed: {e}")
        return False


def send_qc_report_email(qc_result: dict) -> bool:
    """Send QC report email using config values."""
    from services.config import EMAIL_RECIPIENTS, EMAIL_SENDER, EMAIL_APP_PASSWORD

    subject = (
        f"[QC] {qc_result['diff_schema']} — "
        f"{qc_result['pass_count']} PASS / {qc_result['fail_count']} NEED_TO_CHECK"
    )
    return send_email(
        subject,
        qc_result["report"],
        EMAIL_RECIPIENTS,
        EMAIL_SENDER,
        EMAIL_APP_PASSWORD,
    )