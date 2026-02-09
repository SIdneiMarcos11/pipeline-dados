import os
import json
from datetime import date, timedelta

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["SHEET_ID"]
WORKSHEET = os.getenv("WORKSHEET", "bcb_long")

DAYS_BACK_DAILY = 365
MONTHS_BACK_MONTHLY = 36

SERIES = [
    # Selic efetiva diária (como está hoje)
    {"series_id": 11, "metric": "selic", "segment": "Total", "freq": "D"},

    # ✅ NOVO: Meta Selic (% a.a.) — padronizada para mensal (M) para casar com as séries mensais
    {"series_id": 432, "metric": "selic_meta", "segment": "Total", "freq": "M"},

    {"series_id": 20539, "metric": "saldo_credito", "segment": "Total", "freq": "M"},
    {"series_id": 20541, "metric": "saldo_credito", "segment": "PF", "freq": "M"},
    {"series_id": 20540, "metric": "saldo_credito", "segment": "PJ", "freq": "M"},

    {"series_id": 21082, "metric": "inadimplencia", "segment": "Total", "freq": "M"},
    {"series_id": 21084, "metric": "inadimplencia", "segment": "PF", "freq": "M"},
    {"series_id": 21083, "metric": "inadimplencia", "segment": "PJ", "freq": "M"},
]

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

    df["date"] = pd.to_datetime(df["data"], dayfirst=True)
    df["value"] = df["valor"].astype(str).str.replace(",", ".", regex=False).astype(float)
    return df[["date", "value"]].sort_values("date")

def _to_monthly_ffill(df: pd.DataFrame, end_d1: date) -> pd.DataFrame:
    """
    Converte uma série com datas irregulares (ex.: meta Selic muda em datas específicas)
    em série mensal (1 linha por mês), preenchendo com o último valor conhecido.
    """
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    ts = df.copy()
    ts["date"] = pd.to_datetime(ts["date"])
    ts = ts.set_index("date").sort_index()

    # Cria índice mensal do 1º dia de cada mês
    start_month = ts.index.min().to_period("M").to_timestamp()
    end_month = pd.Timestamp(end_d1).to_period("M").to_timestamp()
    monthly_idx = pd.date_range(start=start_month, end=end_month, freq="MS")

    # Reindex + forward fill
    out = ts.reindex(ts.index.union(monthly_idx)).sort_index().ffill()
    out = out.loc[monthly_idx].reset_index().rename(columns={"index": "date"})
    out["date"] = out["date"].dt.date
    return out[["date", "value"]]

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
            continue

        # ✅ Tratamento especial: Meta Selic (432) → mensal por ffill
        if s["metric"] == "selic_meta" and s["freq"] == "M":
            df = _to_monthly_ffill(df, end_d1)

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

    final["date"] = pd.to_datetime(final["date"]).dt.strftime("%Y-%m-%d")
    final["ingested_at"] = pd.Timestamp.utcnow().isoformat()
    final = final.sort_values(["metric", "segment", "date"]).reset_index(drop=True)

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
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

def main():
    df = build_dataset()
    write_to_gsheet(df)

if __name__ == "__main__":
    main()
