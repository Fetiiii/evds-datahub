#!/usr/bin/env python3
# main.py - EVDS eski evdsAPI sürümüne uyumlu

import os
import time
import pandas as pd
from datetime import datetime
from evds import evdsAPI  # 🔹 senin sürümde doğru kullanım

# ---------- AYARLAR ----------
API_KEY = os.getenv("EVDS_API_KEY")  # Ortam değişkeni olarak ayarla
DATA_DIR = "data"
START_DATE = "01-01-2000"
END_DATE = datetime.now().strftime("%d-%m-%Y")
SLEEP_BETWEEN_SERIES = 0.6  # rate-limit
# -----------------------------

if not API_KEY:
    raise SystemExit("❌ EVDS_API_KEY bulunamadı. Ortam değişkeni olarak tanımla.")

os.makedirs(DATA_DIR, exist_ok=True)
evds = evdsAPI(API_KEY)  # 🔹 doğru sınıf kullanımı

# ---------- FONKSİYONLAR ----------

def safe_get_categories():
    try:
        return evds.get_categories()
    except Exception as e:
        print(f"⚠️ get_categories hata: {e}")
        return None

def safe_get_series_for_category(cat_id):
    try:
        return evds.get_series(cat_id)
    except Exception as e:
        print(f"  ⚠️ get_series hata (kategori {cat_id}): {e}")
        return None

def safe_get_data(code):
    try:
        return evds.get_data([code], startdate=START_DATE, enddate=END_DATE)
    except Exception as e:
        print(f"  ⚠️ get_data hata ({code}): {e}")
        return None

def normalize_df(df, code):
    if df is None or df.empty:
        return None

    if "Tarih" not in df.columns and "DATE" in df.columns:
        df = df.rename(columns={"DATE": "Tarih"})
    if "Tarih" not in df.columns:
        print(f"⚠️ {code}: Tarih sütunu yok.")
        return None

    df["Tarih"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Tarih"])

    other_cols = [c for c in df.columns if c != "Tarih"]
    if not other_cols:
        print(f"⚠️ {code}: Veri sütunu bulunamadı.")
        return None

    series_col = other_cols[0]
    df = df.rename(columns={series_col: code.replace(".", "_")})
    return df[["Tarih", code.replace(".", "_")]]

def append_or_create_csv(code, new_df):
    fname = os.path.join(DATA_DIR, f"{code}.csv")
    if new_df is None or new_df.empty:
        print(f"    ⛔ {code}: yeni veri yok.")
        return

    if not os.path.exists(fname):
        new_df.to_csv(fname, index=False, encoding="utf-8")
        print(f"    ✅ Oluşturuldu: {fname} ({len(new_df)} satır)")
        return

    old = pd.read_csv(fname, parse_dates=["Tarih"], dayfirst=True)
    combined = pd.concat([old, new_df], ignore_index=True)
    combined["Tarih"] = pd.to_datetime(combined["Tarih"], dayfirst=True, errors="coerce")
    combined = (
        combined.dropna(subset=["Tarih"])
        .sort_values("Tarih")
        .drop_duplicates(subset=["Tarih"], keep="last")
        .reset_index(drop=True)
    )
    combined.to_csv(fname, index=False, encoding="utf-8")
    print(f"    🔄 Güncellendi: {fname} (toplam {len(combined)} satır)")

def fetch_all_series():
    print("📡 Kategoriler alınıyor...")
    cats = safe_get_categories()
    if cats is None or cats.empty:
        print("⚠️ Kategori alınamadı.")
        return

    for _, cat in cats.iterrows():
        cat_id = cat.get("CATEGORYID")
        cat_name = cat.get("CATEGORYNAME")
        print(f"\n🔍 Kategori: {cat_name} (ID: {cat_id})")

        series_df = safe_get_series_for_category(cat_id)
        if series_df is None or series_df.empty:
            print("  (Bu kategoride seri yok veya hata oluştu.)")
            continue

        for _, s in series_df.iterrows():
            code = s.get("SERIECODE")
            if not code:
                continue

            print(f"  • Seri: {code}")
            df_raw = safe_get_data(code)
            df = normalize_df(df_raw, code)
            append_or_create_csv(code, df)
            time.sleep(SLEEP_BETWEEN_SERIES)

# ---------- ANA PROGRAM ----------
if __name__ == "__main__":
    start = time.time()
    try:
        fetch_all_series()
    except KeyboardInterrupt:
        print("\n🛑 Kullanıcı tarafından durduruldu.")
    except Exception as e:
        print(f"❌ Genel hata: {e}")
    finally:
        elapsed = time.time() - start
        print(f"\n✨ İşlem tamamlandı. Süre: {elapsed:.0f} s")
