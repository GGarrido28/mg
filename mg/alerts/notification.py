import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from mg.alerts.config import _EMAIL_SENDER, _EMAIL_RECEIVER, _EMAIL_APP_PASSWORD


def send_email_alert(subject, message, attachment="None", attachment_name="None"):
    msg = MIMEMultipart()
    msg["From"] = _EMAIL_SENDER
    msg["To"] = _EMAIL_RECEIVER
    msg["Subject"] = subject

    msg.attach(MIMEText(message, "plain"))

    if attachment != "None":
        part = MIMEBase("application", "octet-stream")
        part.set_payload(open(attachment_name, "rb").read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition", 'attachment; filename="%s"' % attachment_name
        )
        msg.attach(part)

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(_EMAIL_SENDER, _EMAIL_APP_PASSWORD)
    text = msg.as_string()

    server.sendmail(_EMAIL_SENDER, _EMAIL_RECEIVER, text)
    server.quit()


if __name__ == "__main__":
    send_email_alert("Test", "This is a test email", "None", "None")
