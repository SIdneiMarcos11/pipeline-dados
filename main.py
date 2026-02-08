import os
import json
from datetime import date, timedelta

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["SHEET_ID"]
WORKSHEET = os.getenv("WORKSHEET", "bcb_d1")

# BCB/SGS (Selic diária = 11 | Meta Selic = 432)
SGS_SELIC_DIA = 11
SGS_META_SELIC = 432

DAYS_BACK = 90  # limita o tamanho (sempre pequeno)

def _br_ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")

def fetch_sgs(series_id: int, start: date, end: date) -> pd.DataFrame:
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
        f"?formato=json&dataInicial={_br_ddmmyyyy(start)}&dataFinal={_br_ddmmyyyy(end)}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])
    df["date"] = pd.to_datetime(df["data"], dayfirst=True).dt.date
    df["value"] = df["valor"].astype(str).str.replace(",", ".", regex=False).astype(float)
    return df[["date", "value"]].sort_values("date")

def build_dataset() -> pd.DataFrame:
    end = date.today() - timedelta(days=1)        # D-1
    start = end - timedelta(days=DAYS_BACK)

    selic = fetch_sgs(SGS_SELIC_DIA, start, end).rename(columns={"value": "selic_pct_dia"})
    meta  = fetch_sgs(SGS_META_SELIC, start, end).rename(columns={"value": "selic_meta"})

    # junta por data; meta pode não existir todos os dias -> forward fill
    df = pd.merge(selic, meta, on="date", how="left").sort_values("date")
    df["selic_meta"] = df["selic_meta"].ffill()

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["ingested_at"] = pd.Timestamp.utcnow().isoformat()
    return df

def write_to_gsheet(df: pd.DataFrame) -> None:
    creds_json = json.loads(os.environ["GSHEET_CREDS_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet(WORKSHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET, rows=2000, cols=20)

    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

def main():
    df = build_dataset()
    write_to_gsheet(df)

if __name__ == "__main__":
    main()
