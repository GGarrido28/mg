import os

# Email credentials from environment variables
_EMAIL_SENDER = os.getenv("MG_EMAIL_SENDER")
_EMAIL_RECEIVER = os.getenv("MG_EMAIL_RECEIVER")
_EMAIL_APP_PASSWORD = os.getenv("MG_EMAIL_APP_PASSWORD")
