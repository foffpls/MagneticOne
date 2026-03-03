from __future__ import annotations

from io import StringIO
from typing import Optional
from urllib.parse import urlparse, parse_qs
from urllib.request import urlopen

import pandas as pd


def _extract_gid_from_url(url: str) -> str:
    """
    Повертає значення параметра gid з URL (з query або fragment).
    Якщо gid не знайдено – повертає "0".
    """
    parsed = urlparse(url)
    for part in (parsed.query, parsed.fragment):
        if not part:
            continue
        qs = parse_qs(part)
        gid_values = qs.get("gid")
        if gid_values:
            return gid_values[0]
    return "0"


def build_csv_export_url(sheet_url: str) -> str:
    """
    Будує URL для експорту Google Sheets у CSV.

    Підтримуються посилання вигляду:
    - https://docs.google.com/spreadsheets/d/<ID>/edit#gid=0
    - https://docs.google.com/spreadsheets/d/<ID>/edit?gid=0
    - вже готовий export?format=csv&gid=...
    """
    if "export?format=csv" in sheet_url:
        return sheet_url

    # Обрізаємо все після /edit, якщо воно є
    base = sheet_url
    if "/edit" in sheet_url:
        base = sheet_url.split("/edit", 1)[0]

    gid = _extract_gid_from_url(sheet_url)
    return f"{base}/export?format=csv&gid={gid}"


def load_google_sheet(sheet_url: str, timeout: int = 15) -> pd.DataFrame:
    """
    Завантажує Google Sheet за посиланням sheet_url та повертає DataFrame.

    Важливо: таблиця має бути доступна принаймні для читання
    (наприклад, 'Anyone with the link – Viewer').
    """
    export_url = build_csv_export_url(sheet_url)

    with urlopen(export_url, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Не вдалося завантажити Google Sheet, HTTP статус: {resp.status}")
        # враховуємо можливу BOM
        csv_text = resp.read().decode("utf-8-sig")

    return pd.read_csv(StringIO(csv_text))

