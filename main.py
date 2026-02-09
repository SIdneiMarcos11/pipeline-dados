import os
import json
import time
from datetime import date, timedelta

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["SHEET_ID"]
WORKSHEET = os.getenv("WORKSHEET", "bcb_long")

DAYS_BACK_DAILY = 365
MONTHS_BACK_MONTHLY = 36

# Retry settings (SGS/BCB instável às vezes)
SGS_MAX_RETRIES = int(os.getenv("SGS_MAX_RETRIES", "10"))
SGS_TIMEOUT_SEC = int(os.getenv("SGS_TIMEOUT_SEC", "30"))

SERIES = [
    {"series_id": 11, "metric": "selic", "segment": "Total", "freq": "D"},

    {"series_id": 20539, "metric": "saldo_credito", "segment": "Total", "freq": "M"},
    {"series_id": 20541, "metric": "saldo_credito", "segment": "PF", "freq": "M"},
    {"series_id": 20540, "metric": "saldo_credito", "segment": "PJ", "freq": "M"},

    {"series_id": 21082, "metric": "inadimplencia", "segment": "Total", "freq": "M"},
    {"series_id": 21084, "metric": "inadimplencia", "segment": "PF", "freq": "M"},
    {"series_id": 21083, "metric": "inadimplencia", "segment": "PJ", "freq": "M"},

    # Meta anual da SELIC (BCB/SGS: 432)
    {"series_id": 432, "metric": "selic_meta", "segment": "Total", "freq": "M"},
]


def _br_ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _sleep_backoff(attempt: int) -> None:
    """
    Backoff exponencial com jitter simples.
    attempt começa em 1.
    """
    # 1,2,4,8,16,20,20...
    wait = min(2 ** (attempt - 1), 20)
    # jitter leve (0 a 0.5s) pra não bater igual sempre
    wait = wait + (attempt * 0.05)
    time.sleep(wait)


def fetch_sgs(series_id: int, start: date, end: date) -> pd.DataFrame:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
    params = {
        "formato": "json",
        "dataInicial": _br_ddmmyyyy(start),
        "dataFinal": _br_ddmmyyyy(end),
    }
    headers = {"Accept": "application/json"}

    last_err = None

    for attempt in range(1, SGS_MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=SGS_TIMEOUT_SEC)

            status = r.status_code
            body = (r.text or "").strip()
            ctype = (r.headers.get("content-type") or "").lower()

            # Caso OK + corpo não vazio -> tenta JSON
            if status == 200 and body:
                # Se retornou HTML ou algo estranho, trata como falha transitória
                if "text/html" in ctype or body.startswith("<"):
                    raise RuntimeError(
                        f"SGS 200 mas retornou HTML (provável instabilidade). "
                        f"series={series_id} ctype={ctype} preview={body[:120]!r}"
                    )

                try:
                    data = r.json()
                except Exception as e:
                    raise RuntimeError(
                        f"SGS retornou 200 mas não era JSON. series={series_id} "
                        f"ctype={ctype} preview={body[:120]!r}"
                    ) from e

                df = pd.DataFrame(data)
                if df.empty:
                    # vazio também pode ser transitório; tenta de novo algumas vezes
                    raise RuntimeError(f"SGS retornou JSON vazio. series={series_id}")

                # Normalização
                df["date"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce").dt.date
                df["value"] = (
                    df["valor"]
                    .astype(str)
                    .str.replace(",", ".", regex=False)
                    .replace("None", "")
                )
                df["value"] = pd.to_numeric(df["value"], errors="coerce")

                df = df.dropna(subset=["date", "value"])
                return df[["date", "value"]].sort_values("date")

            # Retentativas para instabilidade / rate limit
            if status in (429, 500, 502, 503, 504) or not body:
                last_err = RuntimeError(
                    f"SGS falha transitória. series={series_id} status={status} "
                    f"ctype={ctype} preview={body[:120]!r} (tentativa {attempt}/{SGS_MAX_RETRIES})"
                )
                print(f"[WARN] {last_err}")
                _sleep_backoff(attempt)
                continue

            # Erros definitivos (4xx comuns)
            r.raise_for_status()

        except Exception as e:
            last_err = e
            print(f"[WARN] fetch_sgs erro series={series_id} (tentativa {attempt}/{SGS_MAX_RETRIES}): {e}")
            if attempt < SGS_MAX_RETRIES:
                _sleep_backoff(attempt)
            else:
                break

    # Se estourou as tentativas, devolve DF vazio (pra não derrubar o pipeline todo)
    print(f"[ERROR] SGS indisponível para series={series_id} após {SGS_MAX_RETRIES} tentativas. Erro final: {last_err}")
    return pd.DataFrame(columns=["date", "value"])


def build_dataset() -> pd.DataFrame:
    end_d1 = date.today() - timedelta(days=1)

    frames = []
    for s in SERIES:
        if s["freq"] == "D":
            start = end_d1 - timedelta(days=DAYS_BACK_DAILY)
        else:
            start = end_d1 - timedelta(days=MONTHS_BACK_MONTHLY * 31)

        df = fetch_sgs(s["series_id"], start, end_d1)
        if df.empty:
            # não derruba: apenas segue
            continue

        out = df.copy()
        out["metric"] = s["metric"]
        out["segment"] = s["segment"]
        out["series_id"] = s["series_id"]
        out["freq"] = s["freq"]
        frames.append(out)

    final = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["date", "metric", "segment", "series_id", "value", "freq"])
    )

    # Normaliza formato da data e insere timestamp de carga
    final["date"] = pd.to_datetime(final["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    final["ingested_at"] = pd.Timestamp.utcnow().isoformat()

    final = final.dropna(subset=["date"])
    final = final.sort_values(["metric", "segment", "date"]).reset_index(drop=True)

    # Sanitização: remove NaN/inf (gspread/JSON não aceitam)
    final = final.replace([float("inf"), float("-inf")], None)
    final = final.where(pd.notnull(final), None)

    return final[["date", "metric", "segment", "series_id", "value", "freq", "ingested_at"]]


def write_to_gsheet(df: pd.DataFrame) -> None:
    creds_json = json.loads(os.environ["GSHEET_CREDS_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet(WORKSHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET, rows=6000, cols=20)

    ws.clear()

    safe_df = df.copy()
    safe_df = safe_df.replace([float("inf"), float("-inf")], None)
    safe_df = safe_df.where(pd.notnull(safe_df), None)

    # Importante: não forçar .astype(str) (isso vira "None" em texto)
    values = [safe_df.columns.tolist()] + safe_df.values.tolist()
    ws.update(values)


def main():
    df = build_dataset()
    write_to_gsheet(df)


if __name__ == "__main__":
    main()
