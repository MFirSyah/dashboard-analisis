# Streamlit App: Analisis Omzet Multi-Toko

Berikut saya buatkan dua file dalam satu dokumen: `app.py` (kode Streamlit utama) dan `requirements.txt`.

> **Catatan penting sebelum menjalankan**
>
> * Anda harus menaruh kredensial Google Service Account (JSON) ke dalam `st.secrets["gcp_service_account"]` atau mengisi `st.secrets` sesuai instruksi di bagian *Setup* pada `app.py`.
> * Beri akses `viewer` pada Google Sheet untuk service account email (jika menggunakan GSheet). ID GSheet yang Anda berikan: `1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ`.
> * Jika tidak ingin gunakan GSheet, aplikasi otomatis mencoba membaca file lokal `DATA_REKAP.xlsx` (sudah Anda upload di repository jika deploy ke Streamlit Cloud).

---

## File: `app.py`

```python
# app.py
import streamlit as st
import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz
import plotly.express as px
from datetime import datetime, timedelta
import io
import json
import os

# ------------------------
# Helpers
# ------------------------

@st.cache_data
def load_local_excel(path="DATA_REKAP.xlsx"):
    if os.path.exists(path):
        xls = pd.read_excel(path, sheet_name=None)
        return xls
    return None

@st.cache_data
def load_gsheet(sheet_id: str, service_account_info: dict = None):
    # Attempt to load using gspread if service_account_info provided
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
    except Exception as e:
        st.warning("gspread tidak ditemukan atau gagal diimport: %s" % e)
        return None

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)
    sheets = {ws.title: pd.DataFrame(ws.get_all_records()) for ws in sh.worksheets()}
    return sheets


def try_load_data(gsheet_id: str):
    # First try GSheet if st.secrets provided
    sa = st.secrets.get("gcp_service_account") if isinstance(st.secrets, dict) else None
    if sa:
        try:
            sheets = load_gsheet(gsheet_id, sa)
            if sheets:
                return sheets
        except Exception as e:
            st.error(f"GSheet load error: {e}")

    # fallback local excel
    xls = load_local_excel()
    if xls is not None:
        return xls

    return None


# Generic normalization

def normalize_column_names(df: pd.DataFrame):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


# Flatten multi-sheet structure to a canonical product-level DataFrame
@st.cache_data
def build_master_df(sheets: dict):
    # Strategy: if there's a sheet named 'DATABASE' or 'Database', use it as base (contains product metadata)
    keys = {k.lower(): k for k in sheets.keys()}
    base_key = None
    for candidate in ["database", "DATABASE"]:
        if candidate.lower() in keys:
            base_key = keys[candidate.lower()]
            break
    if base_key is None:
        # take first sheet as base
        base_key = list(sheets.keys())[0]

    base = normalize_column_names(sheets[base_key])
    # ensure there is a product name column
    name_col = None
    for c in base.columns:
        if "nama" in c.lower() or "product" in c.lower() or "produk" in c.lower():
            name_col = c
            break
    if name_col is None:
        # set index as product
        base = base.reset_index().rename(columns={"index": "Nama Produk"})
        name_col = "Nama Produk"

    # Collect shop columns from other sheets: expect columns like "STORE - REKAP - READY" and "STORE - REKAP - HABIS"
    shop_info = []
    for sheet_name, df in sheets.items():
        if sheet_name == base_key:
            continue
        df = normalize_column_names(df)
        # try to find product name column
        prod_col = None
        for c in df.columns:
            if "nama" in c.lower() or "produk" in c.lower() or "product" in c.lower():
                prod_col = c
                break
        if prod_col is None:
            continue
        # melt the sheet into rows
        # Assume columns: Nama Produk, Harga, Terjual/Bulan, Stok/Status, Tanggal
        # We'll try to detect these column names heuristically
        col_map = {c.lower(): c for c in df.columns}
        def find_like(keywords):
            for k, orig in col_map.items():
                for kw in keywords:
                    if kw in k:
                        return orig
            return None
        price_col = find_like(["harga", "price"])
        terjual_col = find_like(["terjual", "sold"])
        status_col = find_like(["status", "ready", "habis", "stok"])
        date_col = find_like(["tanggal", "date", "tgl"])

        # rename to canonical
        df2 = df[[prod_col] + ([price_col] if price_col else []) + ([terjual_col] if terjual_col else []) + ([status_col] if status_col else []) + ([date_col] if date_col else [])]
        df2 = df2.rename(columns={prod_col: "Nama Produk", price_col: "Harga", terjual_col: "Terjual/Bulan", status_col: "Status", date_col: "Tanggal"})
        df2["Toko"] = sheet_name
        # coerce
        if "Tanggal" in df2.columns:
            df2["Tanggal"] = pd.to_datetime(df2["Tanggal"], errors="coerce")
        else:
            df2["Tanggal"] = pd.NaT

        shop_info.append(df2)

    if len(shop_info) == 0:
        # fallback: use base as single table
        df = base.rename(columns={name_col: "Nama Produk"})
        if "Tanggal" not in df.columns:
            df["Tanggal"] = pd.NaT
        df["Toko"] = "DATABASE"
        return df

    master = pd.concat(shop_info, ignore_index=True, sort=False)
    # fill missing Harga and Terjual/Bulan with numeric coercion
    if "Harga" in master.columns:
        master["Harga"] = pd.to_numeric(master["Harga"], errors="coerce")
    if "Terjual/Bulan" in master.columns:
        master["Terjual/Bulan"] = pd.to_numeric(master["Terjual/Bulan"], errors="coerce")
    return master


# Fuzzy link between DB KLIK products and DATABASE categories
@st.cache_data
def label_categories(master_df: pd.DataFrame, database_df: pd.DataFrame, threshold=80):
    # database_df expected to have Nama Produk and Kategori
    names_db = database_df[database_df.columns[0]].astype(str).tolist()
    # try to find category column
    cat_col = None
    for c in database_df.columns:
        if "kategori" in c.lower() or "category" in c.lower():
            cat_col = c
            break
    if cat_col is None:
        # create default
        database_df["Kategori"] = "Unknown"
        cat_col = "Kategori"
    mapping = {}
    for prod in master_df["Nama Produk"].astype(str).unique():
        match = process.extractOne(prod, names_db, scorer=fuzz.token_sort_ratio)
        if match and match[1] >= threshold:
            idx = names_db.index(match[0])
            try:
                mapping[prod] = database_df.iloc[idx][cat_col]
            except Exception:
                mapping[prod] = "Unknown"
        else:
            mapping[prod] = "Unknown"
    master_df["Kategori"] = master_df["Nama Produk"].map(mapping)
    return master_df


# ------------------------
# Streamlit UI
# ------------------------

st.set_page_config(page_title="Analisis Omzet Multi-Toko", layout="wide")
st.title("Analisis Omzet Multi-Toko")

# Setup secrets guidance
st.sidebar.markdown("**Setup & Kredensial**")
st.sidebar.caption("Masukkan kredensial Google Service Account (jika ingin tarik data dari GSheet) di `st.secrets['gcp_service_account']` sebagai JSON. Jika tidak, pastikan file DATA_REKAP.xlsx ada di repo.")

# Main control
if "analisis_selesai" not in st.session_state:
    st.session_state["analisis_selesai"] = False

col1, col2 = st.columns([1, 3])
with col1:
    if not st.session_state["analisis_selesai"]:
        if st.button("Tarik Data & Mulai Analisis"):
            st.session_state["load_trigger"] = True
            st.session_state["analisis_selesai"] = True
    else:
        st.success("Analisis sudah dijalankan. Sidebar tersedia untuk pengaturan.")

with col2:
    st.write("Gunakan tombol di kiri untuk mulai analisis. Setelah selesai, tombol disembunyikan dan sidebar muncul.")

# Sidebar (only show after analysis)
if st.session_state["analisis_selesai"]:
    st.sidebar.header("Kontrol Analisis")
    if st.sidebar.button("Hapus Cache & Mulai Analisis Ulang"):
        st.cache_data.clear()
        st.session_state["analisis_selesai"] = False
        st.experimental_rerun()

    date_range = st.sidebar.date_input("Rentang tanggal analisis (start, end)", value=(datetime.today() - timedelta(days=30), datetime.today()))
    fuzzy_threshold = st.sidebar.slider("Tingkat akurasi fuzzy (%)", 50, 100, 85)
    rows_info_placeholder = st.sidebar.empty()
    if st.sidebar.button("Unduh Data Hasil (CSV)"):
        st.session_state["download_csv"] = True
    if st.sidebar.button("Unduh Data Hasil (JSON)"):
        st.session_state["download_json"] = True

# Load data when triggered or when analysis state true
sheets = None
if st.session_state.get("analisis_selesai"):
    sheets = try_load_data("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
    if sheets is None:
        st.error("Gagal mengambil data dari GSheet atau file lokal tidak ditemukan. Pastikan kredensial dan file tersedia.")
    else:
        master = build_master_df(sheets)
        # Try to find a DATABASE sheet for category labeling
        db_sheet = None
        for k in sheets.keys():
            if k.lower() == "database" or k.lower().startswith("database"):
                db_sheet = sheets[k]
                break
        if db_sheet is None:
            # if no explicit, try first sheet
            db_sheet = sheets[list(sheets.keys())[0]]

        labeled = label_categories(master, db_sheet, threshold=fuzzy_threshold)
        rows_info_placeholder.info(f"Jumlah baris data yang diolah: {len(labeled)}")

        # Provide download
        if st.session_state.get("download_csv"):
            csv = labeled.to_csv(index=False)
            st.download_button("Download CSV Hasil", data=csv, file_name="analisis_omzet_hasil.csv", mime="text/csv")
            st.session_state["download_csv"] = False
        if st.session_state.get("download_json"):
            j = labeled.to_json(orient="records", force_ascii=False)
            st.download_button("Download JSON Hasil", data=j, file_name="analisis_omzet_hasil.json", mime="application/json")
            st.session_state["download_json"] = False

        # --- Build tabs ---
        tabs = st.tabs(["Ringkasan Kategori & Produk", "Bandingkan Produk", "Analisis Brand Kompetitor", "Tren Stok Mingguan", "Omzet Semua Toko", "Produk Baru Mingguan"])

        # Tab 1: Kategori Terlaris & Produk Terlaris & Distribusi Brand & Ringkasan Mingguan
        with tabs[0]:
            st.header("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
            # aggregate omzet per kategori (use Harga * Terjual/Bulan)
            df = labeled.copy()
            df["Omzet"] = df.get("Harga", 0).fillna(0) * df.get("Terjual/Bulan", 0).fillna(0)
            # filter by date range
            start_date, end_date = date_range
            if pd.notnull(start_date) and pd.notnull(end_date):
                # keep rows with Tanggal inside range OR unknown dates
                mask = (df["Tanggal"].isna()) | ((df["Tanggal"] >= pd.to_datetime(start_date)) & (df["Tanggal"] <= pd.to_datetime(end_date)))
                df = df[mask]

            cat_agg = df.groupby("Kategori")["Omzet"].sum().reset_index().sort_values("Omzet", ascending=False)
            # interactive bar: user select number of bars
            max_bars = st.number_input("Jumlah kategori yang ditampilkan", min_value=3, max_value=min(50, len(cat_agg)), value=min(10, len(cat_agg)))
            show_bars = cat_agg.head(max_bars)
            fig = px.bar(show_bars, x="Kategori", y="Omzet", text="Omzet", title="Omzet per Kategori")
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Produk paling laris dari kategori terpilih")
            selected_cat = st.selectbox("Pilih kategori", options=["Semua"] + cat_agg["Kategori"].astype(str).tolist())
            if selected_cat == "Semua":
                top_products = df.sort_values("Omzet", ascending=False).head(20)
            else:
                top_products = df[df["Kategori"] == selected_cat].sort_values("Omzet", ascending=False).head(20)
            display_tbl = top_products[["Nama Produk", "Harga", "Terjual/Bulan", "Status"]].rename(columns={"Nama Produk": "Nama Produk", "Harga": "Harga pada tanggal terakhir", "Terjual/Bulan": "Terjual/Bulan pada tanggal terakhir", "Status": "Status"})
            st.dataframe(display_tbl)

            st.markdown("---")
            st.header("2. Produk Terlaris (Global)")
            prod_agg = df.groupby(["Nama Produk"]).agg({"Harga": "last", "Terjual/Bulan": "last", "Omzet": "sum", "Tanggal": "max"}).reset_index()
            prod_agg = prod_agg.sort_values("Omzet", ascending=False)
            # compute week-over-week indicator (simple approach)
            # We'll compute omzet this week vs last week by resampling if Tanggal present
            def compute_wow_indicator(product):
                sub = df[df["Nama Produk"] == product]
                if sub["Tanggal"].notna().sum() < 2:
                    return "-"
                latest = sub["Tanggal"].max()
                week_start = latest - pd.Timedelta(days=7)
                prev_week_start = week_start - pd.Timedelta(days=7)
                this_week = sub[(sub["Tanggal"] > week_start) & (sub["Tanggal"] <= latest)]["Omzet"].sum()
                prev_week = sub[(sub["Tanggal"] > prev_week_start) & (sub["Tanggal"] <= week_start)]["Omzet"].sum()
                if prev_week == 0:
                    return "-"
                pct = (this_week - prev_week) / prev_week * 100
                return pct

            prod_agg["WOW_pct"] = prod_agg["Nama Produk"].apply(lambda x: compute_wow_indicator(x))
            prod_agg_display = prod_agg[["Nama Produk", "Harga", "Terjual/Bulan", "Omzet", "Tanggal", "WOW_pct"]].rename(columns={"Nama Produk": "Nama Produk", "Harga": "Harga pada tanggal terakhir", "Terjual/Bulan": "Terjual/Bulan pada tanggal terakhir", "Omzet": "Omzet", "Tanggal": "Tanggal"})
            st.dataframe(prod_agg_display.head(100))

            st.markdown("---")
            st.header("3. Distribusi Omzet Brand")
            # try to detect brand column
            brand_col = None
            for c in df.columns:
                if "brand" in c.lower():
                    brand_col = c
                    break
            if brand_col is None:
                st.info("Tidak ditemukan kolom Brand pada data. Jika ada di sheet DATABASE, tambahkan kolom bernama 'Brand'.")
            else:
                brand_agg = df.groupby(brand_col)["Omzet"].sum().reset_index().sort_values("Omzet", ascending=False)
                fig2 = px.pie(brand_agg, names=brand_col, values="Omzet", title="Persentase Omzet per Brand", hole=0.3)
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("---")
            st.header("4. Ringkasan Kinerja Mingguan (WoW Growth)")
            # compute weekly omzet per toko
            df_week = df.copy()
            df_week["Week"] = df_week["Tanggal"].dt.to_period('W').apply(lambda r: r.start_time) if df_week["Tanggal"].notna().any() else pd.NaT
            weekly = df_week.groupby(["Week"]) ["Omzet"].sum().reset_index().sort_values("Week")
            weekly["Penjualan Unit"] = df_week.groupby(["Week"])["Nama Produk"].nunique().values if df_week["Week"].notna().any() else 0
            weekly["Pertumbuhan Omzet_pct"] = weekly["Omzet"].pct_change() * 100
            st.dataframe(weekly.fillna("-"))

        # Tab 2: Bandingkan Produk
        with tabs[1]:
            st.header("Bandingkan Produk")
            all_products = labeled["Nama Produk"].astype(str).unique().tolist()
            selected = st.selectbox("Pilih Produk (DB KLIK)", options=all_products)
            if selected:
                # create comparison set: find similar names across toko
                choices = labeled["Nama Produk"].astype(str).unique().tolist()
                matches = process.extract(selected, choices, scorer=fuzz.token_sort_ratio, limit=10)
                match_names = [m[0] for m in matches if m[1] >= fuzzy_threshold]
                st.subheader("Detail & Perubahan Harga")
                left = labeled[labeled["Nama Produk"] == selected].sort_values("Tanggal")
                st.write("Data produk terpilih (history):")
                st.dataframe(left[["Tanggal","Toko","Harga","Status","Terjual/Bulan"]])
                # show price line
                if left["Harga"].notna().any():
                    figp = px.line(left.sort_values("Tanggal"), x="Tanggal", y="Harga", markers=True, title=f"Harga - {selected}")
                    st.plotly_chart(figp, use_container_width=True)

                st.write("Produk kemiripan di toko lain:")
                for mn in match_names:
                    if mn == selected:
                        continue
                    sub = labeled[labeled["Nama Produk"] == mn].sort_values("Tanggal")
                    st.write(f"**{mn}** - Toko: {', '.join(sub['Toko'].unique())}")
                    st.write(sub[["Tanggal","Toko","Harga","Status","Terjual/Bulan"]].tail(5))
                    # compare latest price
                    latest_sel = left.sort_values("Tanggal").dropna(subset=["Harga"]).tail(1)
                    latest_cmp = sub.sort_values("Tanggal").dropna(subset=["Harga"]).tail(1)
                    if latest_sel.empty or latest_cmp.empty:
                        st.write("Data tidak ditemukan untuk salah satu produk (harga/stok)")
                    else:
                        p_sel = latest_sel.iloc[-1]["Harga"]
                        p_cmp = latest_cmp.iloc[-1]["Harga"]
                        diff = p_cmp - p_sel
                        if diff < 0:
                            st.success(f"Lebih murah di {mn} sebesar Rp{abs(int(diff))}")
                        elif diff > 0:
                            st.error(f"Lebih mahal di {mn} sebesar Rp{int(diff)}")
                        else:
                            st.info("Produk Harga Sama")

        # Tab 3: Analisis Brand Kompetitor
        with tabs[2]:
            st.header("Analisis Brand di Toko Kompetitor")
            # find brand col
            brand_col = None
            for c in labeled.columns:
                if "brand" in c.lower():
                    brand_col = c
                    break
            if brand_col is None:
                st.info("Kolom Brand tidak ditemukan di dataset. Jika tersedia, tambahkan kolom bernama 'Brand'.")
            else:
                brand_summary = labeled.groupby(["Toko", brand_col]).agg({"Omzet": "sum", "Nama Produk": "count"}).rename(columns={"Nama Produk": "Unit Terjual"}).reset_index()
                st.dataframe(brand_summary)
                figb = px.pie(brand_summary, names=brand_col, values="Omzet", title="Omzet per Brand (gabungan semua toko)")
                st.plotly_chart(figb, use_container_width=True)

        # Tab 4: Tren Status Stok Mingguan per Toko
        with tabs[3]:
            st.header("Tren Status Stok Mingguan per Toko")
            df_stat = labeled.copy()
            df_stat["Week"] = df_stat["Tanggal"].dt.to_period('W').apply(lambda r: r.start_time) if df_stat["Tanggal"].notna().any() else pd.NaT
            # count ready/habis per week per toko
            def count_status(gdf):
                ready = gdf[gdf["Status"].str.lower().str.contains("ready", na=False)].shape[0]
                habis = gdf[gdf["Status"].str.lower().str.contains("habis", na=False)].shape[0]
                return pd.Series({"Ready": ready, "Habis": habis})
            weekly_status = df_stat.groupby(["Week", "Toko"]).apply(count_status).reset_index()
            st.dataframe(weekly_status.fillna(0))
            fig_line = px.line(weekly_status, x="Week", y=["Ready","Habis"], title="Tren Ready vs Habis per Minggu (gabungan semua toko)")
            st.plotly_chart(fig_line, use_container_width=True)

        # Tab 5: Tabel omzet dari semua toko (x=toko, y=tanggal)
        with tabs[4]:
            st.header("Tabel Omzet Semua Toko")
            pivot = df.pivot_table(index="Tanggal", columns="Toko", values="Omzet", aggfunc="sum").fillna(0)
            st.dataframe(pivot)
            # line chart with many series
            fig_all = px.line(pivot.reset_index(), x="Tanggal", y=pivot.columns, title="Omzet per Toko over Time")
            st.plotly_chart(fig_all, use_container_width=True)

        # Tab 6: Produk Baru Mingguan
        with tabs[5]:
            st.header("Analisis Produk Baru Mingguan")
            st.write("Pilih minggu pembanding dan minggu target untuk melihat produk yang muncul di minggu target tapi tidak di pembanding")
            weeks = df["Tanggal"].dropna().dt.to_period('W').unique().tolist()
            weeks_dt = [w.start_time for w in weeks]
            if len(weeks_dt) < 2:
                st.info("Data tanggal tidak cukup untuk analisis mingguan produk baru.")
            else:
                w1 = st.selectbox("Minggu pembanding", options=weeks_dt)
                w2 = st.selectbox("Minggu target", options=weeks_dt, index=min(1, len(weeks_dt)-1))
                df_w1 = df[df["Tanggal"].dt.to_period('W').apply(lambda r: r.start_time) == w1]
                df_w2 = df[df["Tanggal"].dt.to_period('W').apply(lambda r: r.start_time) == w2]
                new_products = set(df_w2["Nama Produk"]) - set(df_w1["Nama Produk"]) if (not df_w1.empty and not df_w2.empty) else set()
                new_df = df_w2[df_w2["Nama Produk"].isin(new_products)][["Nama Produk","Harga","Status","Kategori"]]
                st.dataframe(new_df)

        # end tabs

        st.success("Analisis selesai. Gunakan sidebar untuk mengubah rentang tanggal atau threshold fuzzy, atau hapus cache untuk refresh data.")

```

---

## File: `requirements.txt`

```
streamlit==1.24.0
pandas
numpy
plotly
gspread
oauth2client
rapidfuzz
openpyxl
xlrd
```

---

## Setup singkat & deployment ke share.streamlit.io

1. Simpan `app.py` dan `requirements.txt` di repository GitHub Anda (root repo).
2. Jika ingin tarik data dari Google Sheets, buat Service Account di Google Cloud, unduh JSON, lalu masukkan JSON ke `st.secrets` di Streamlit Cloud atau ke `secrets.toml` lokal saat pengembangan. Contoh struktur di `st.secrets`:

```toml
[gcp_service_account]
project_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "...@...iam.gserviceaccount.com"
# dll sesuai JSON
```

3. Pastikan sheet dibagikan (share) ke email service account.
4. Deploy repo ke share.streamlit.io.

---

Jika Anda ingin, saya bisa: menyesuaikan kode untuk struktur kolom spesifik file `DATA_REKAP.xlsx` yang Anda upload (tolong beri contoh 10 baris header & 10 baris data) atau membuat versi yang lebih ringan/lebih cepat.
