from __future__ import annotations

import os
from typing import Sequence

from google.auth.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials


def _service_account_credentials(scopes: Sequence[str]) -> Credentials:
    creds_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS"
    )
    if not creds_path:
        raise RuntimeError(
            "Service account auth is required. Set GOOGLE_SERVICE_ACCOUNT_FILE "
            "(or GOOGLE_APPLICATION_CREDENTIALS) to your service account JSON path."
        )

    creds: Credentials = ServiceAccountCredentials.from_service_account_file(
        creds_path, scopes=list(scopes)
    )

    subject = os.getenv("GOOGLE_IMPERSONATE_USER")
    if subject and hasattr(creds, "with_subject"):
        creds = creds.with_subject(subject)

    return creds


def get_google_credentials(scopes: Sequence[str]) -> Credentials:
    return _service_account_credentials(scopes)
