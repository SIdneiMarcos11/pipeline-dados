import os
import json
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["SHEET_ID"]

def fetch_data():
    url = "https://api.publicapis.org/entries"
    response = requests.get(url)
    data = response.json()["entries"][:20]
    df = pd.DataFrame(data)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df

def write_to_gsheet(df):
    creds_json = json.loads(os.environ["GSHEET_CREDS_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet("dados")
    except:
        ws = sh.add_worksheet(title="dados", rows=1000, cols=20)

    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

def main():
    df = fetch_data()
    write_to_gsheet(df)

if __name__ == "__main__":
    main()
