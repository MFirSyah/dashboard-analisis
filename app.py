# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI DENGAN CACHE FUZZY
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Update: Tambahan sheet khusus "hasil_fuzzy" untuk mempercepat recall di Tab 2
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import re
import gspread

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI KONEKSI GOOGLE SHEETS
# ===================================================================================
def get_gspread_client():
    creds_dict = {
        "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
        "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    return gspread.service_account_from_dict(creds_dict)

# ===================================================================================
# LOAD DATA
# ===================================================================================
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: {e}")
        return None, None

    rekap_list_df, database_df = [], pd.DataFrame()
    sheet_names = [
        "DATABASE", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
        "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
        "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS",
        "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
        "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS"
    ]
    for sheet_name in sheet_names:
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            all_values = sheet.get_all_values()
        except:
            continue
        if not all_values or len(all_values) < 2:
            continue
        header = all_values[0]
        data = all_values[1:]
        df_sheet = pd.DataFrame(data, columns=header)
        if "DATABASE" in sheet_name.upper():
            database_df = df_sheet
        elif "REKAP" in sheet_name.upper():
            store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
            df_sheet['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
            df_sheet['Status'] = 'Tersedia' if "READY" in sheet_name.upper() else 'Habis'
            rekap_list_df.append(df_sheet)

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]
    rename_map = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand'}
    rekap_df.rename(columns=rename_map, inplace=True)

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0).astype(int)
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0).astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()

    return rekap_df.sort_values("Tanggal"), database_df

# ===================================================================================
# FUZZY MATCH – BATCH SAVE
# ===================================================================================
def generate_fuzzy_matches(df, my_store_name="DB KLIK", cutoff=90):
    latest_date = df['Tanggal'].max()
    main_store_latest = df[(df['Toko'] == my_store_name) & (df['Tanggal'] == latest_date)]
    competitor_latest = df[(df['Toko'] != my_store_name) & (df['Tanggal'] == latest_date)]

    results = []
    for _, row in main_store_latest.iterrows():
        matches = process.extract(row['Nama Produk'], competitor_latest['Nama Produk'].tolist(), scorer=fuzz.token_set_ratio, limit=5)
        for match, score in matches:
            if score >= cutoff:
                comp_info = competitor_latest[competitor_latest['Nama Produk'] == match].iloc[0]
                results.append([
                    row['Nama Produk'], row['Harga'], row['Status'], row['Stok'],
                    comp_info['Toko'], comp_info['Nama Produk'], comp_info['Harga'], comp_info['Status'], comp_info['Stok'], score
                ])
    return pd.DataFrame(results, columns=[
        "Produk DB KLIK", "Harga DB KLIK", "Status DB KLIK", "Stok DB KLIK",
        "Toko Kompetitor", "Produk Kompetitor", "Harga Kompetitor", "Status Kompetitor", "Stok Kompetitor", "Skor Kemiripan"
    ])

def write_fuzzy_to_gsheet(df_fuzzy):
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
    try:
        sheet = spreadsheet.worksheet("hasil_fuzzy")
        spreadsheet.del_worksheet(sheet)
    except:
        pass
    worksheet = spreadsheet.add_worksheet("hasil_fuzzy", rows="1000", cols="20")
    worksheet.update([df_fuzzy.columns.values.tolist()] + df_fuzzy.values.tolist())

# ===================================================================================
# MAIN APP
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

# Sidebar
if st.sidebar.button("Update Data Fuzzy 🔄"):
    df, _ = load_data_from_gsheets()
    if df is not None:
        df_fuzzy = generate_fuzzy_matches(df)
        write_fuzzy_to_gsheet(df_fuzzy)
        st.sidebar.success("Data fuzzy berhasil diperbarui!")

# Load data utama
df, db_df = load_data_from_gsheets()
if df is None: st.stop()

# Load fuzzy dari sheet
gc = get_gspread_client()
spreadsheet = gc.open_by_key("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
try:
    fuzzy_ws = spreadsheet.worksheet("hasil_fuzzy")
    fuzzy_values = fuzzy_ws.get_all_values()
    fuzzy_df = pd.DataFrame(fuzzy_values[1:], columns=fuzzy_values[0])
except:
    fuzzy_df = pd.DataFrame()

# ===================================================================================
# TABS
# ===================================================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "⭐ Analisis Toko Saya", "⚖️ Perbandingan Harga", 
    "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", 
    "📈 Kinerja Penjualan", "📊 Analisis Mingguan"
])

with tab1:
    st.header("Analisis Toko Saya")
    st.dataframe(df.head())

with tab2:
    st.header("Perbandingan Produk DB KLIK dengan Kompetitor")
    if fuzzy_df.empty:
        st.warning("Data fuzzy belum tersedia. Tekan tombol 'Update Data Fuzzy 🔄' di sidebar.")
    else:
        selected_product = st.selectbox("Pilih Produk DB KLIK:", sorted(fuzzy_df["Produk DB KLIK"].unique()))
        if selected_product:
            subset = fuzzy_df[fuzzy_df["Produk DB KLIK"] == selected_product]
            st.dataframe(subset, use_container_width=True, hide_index=True)

with tab3:
    st.header("Analisis Brand Kompetitor")
    st.dataframe(df.groupby("Brand")["Omzet"].sum().reset_index().sort_values("Omzet", ascending=False).head(10))

with tab4:
    st.header("Status Stok Produk")
    stok_summary = df.groupby(["Toko", "Status"]).size().reset_index(name="Jumlah")
    st.dataframe(stok_summary)

with tab5:
    st.header("Kinerja Penjualan")
    omzet_summary = df.groupby("Toko")["Omzet"].sum().reset_index()
    st.bar_chart(omzet_summary.set_index("Toko"))

with tab6:
    st.header("Analisis Mingguan")
    weekly = df.copy()
    weekly["Minggu"] = weekly["Tanggal"].dt.to_period("W").apply(lambda r: r.start_time)
    weekly_summary = weekly.groupby("Minggu")["Omzet"].sum().reset_index()
    st.line_chart(weekly_summary.set_index("Minggu"))
