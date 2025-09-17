# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI FINAL DENGAN CACHE FUZZY KE SHEET
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets individual)
#  Peningkatan: Fuzzy similarity di-cache ke sheet 'hasil_fuzzy'.
# ===================================================================================

import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import re
import gspread

st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI UTAMA UNTUK DATA
# ===================================================================================
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    try:
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
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
    try:
        for sheet_name in sheet_names:
            sheet = spreadsheet.worksheet(sheet_name)
            all_values = sheet.get_all_values()
            if not all_values or len(all_values) < 2:
                continue
            header, data = all_values[0], all_values[1:]
            df_sheet = pd.DataFrame(data, columns=header)
            if '' in df_sheet.columns:
                df_sheet = df_sheet.drop(columns=[''])
            if "DATABASE" in sheet_name.upper():
                database_df = df_sheet
            elif "REKAP" in sheet_name.upper():
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                df_sheet['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
                df_sheet['Status'] = 'Tersedia' if "READY" in sheet_name.upper() else 'Habis'
                rekap_list_df.append(df_sheet)
    except Exception as e:
        st.error(f"Gagal memproses sheet: {e}.")
        return None, None

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang dimuat.")
        return None, None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    for df in [database_df, rekap_df]:
        if not df.empty:
            df.columns = [str(col).strip().upper() for col in df.columns]

    rename_map = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'}
    rekap_df.rename(columns=rename_map, inplace=True)
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    if 'Stok' not in rekap_df.columns:
        rekap_df['Stok'] = 'N/A'

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)

    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Terjual per Bulan'] = rekap_df['Terjual per Bulan'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True, keep='last')
    return rekap_df.sort_values('Tanggal'), database_df

# ===================================================================================
# FUNGSI UNTUK FUZZY KE SHEET
# ===================================================================================
def update_fuzzy_to_sheet(spreadsheet, main_store_latest, competitor_latest, score_cutoff=90):
    try:
        try:
            ws_fuzzy = spreadsheet.worksheet("hasil_fuzzy")
            ws_fuzzy.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws_fuzzy = spreadsheet.add_worksheet(title="hasil_fuzzy", rows="1000", cols="10")
        
        results = []
        competitor_products = tuple(competitor_latest['Nama Produk'].tolist())
        for _, row in main_store_latest.iterrows():
            product_name = row['Nama Produk']
            matches = process.extract(product_name, competitor_products, limit=20, scorer=fuzz.token_set_ratio)
            matches = [m for m in matches if m[1] >= score_cutoff][:5]
            for match, score in matches:
                comp_row = competitor_latest[competitor_latest['Nama Produk'] == match].iloc[0]
                results.append({
                    "Produk Utama": product_name,
                    "Produk Kompetitor": match,
                    "Skor": score,
                    "Toko Kompetitor": comp_row["Toko"],
                    "Harga Kompetitor": comp_row["Harga"],
                    "Status Kompetitor": comp_row["Status"],
                    "Stok Kompetitor": comp_row["Stok"]
                })
        df_fuzzy = pd.DataFrame(results)
        if df_fuzzy.empty:
            st.warning("Tidak ada hasil fuzzy.")
            return
        ws_fuzzy.update([df_fuzzy.columns.tolist()] + df_fuzzy.values.tolist())
        st.success("✅ Data fuzzy berhasil diperbarui.")
    except Exception as e:
        st.error(f"Gagal update fuzzy: {e}")

@st.cache_data
def load_fuzzy_from_sheet():
    try:
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
        ws = spreadsheet.worksheet("hasil_fuzzy")
        all_values = ws.get_all_values()
        if not all_values or len(all_values) < 2:
            return pd.DataFrame()
        return pd.DataFrame(all_values[1:], columns=all_values[0])
    except Exception as e:
        st.error(f"Gagal load hasil_fuzzy: {e}")
        return pd.DataFrame()

# ===================================================================================
# MAIN APP
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if not st.session_state.data_loaded:
    if st.button("Tarik Data & Mulai Analisis 🚀"):
        df, db_df = load_data_from_gsheets()
        if df is not None and not df.empty:
            st.session_state.df, st.session_state.db_df = df, db_df
            st.session_state.data_loaded = True
            st.rerun()
    st.stop()

df, db_df = st.session_state.df, st.session_state.db_df
my_store_name = "DB KLIK"

# Sidebar
if st.sidebar.button("Hapus Cache & Tarik Ulang 🔄"):
    st.cache_data.clear(); st.session_state.data_loaded = False; st.rerun()

accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91)

# Tombol update fuzzy
if st.sidebar.button("Update Data Fuzzy 🔄"):
    latest_date = df['Tanggal'].max()
    main_store_latest = df[(df['Toko']==my_store_name)&(df['Tanggal']==latest_date)]
    competitor_latest = df[(df['Toko']!=my_store_name)&(df['Tanggal']==latest_date)]
    if not main_store_latest.empty and not competitor_latest.empty:
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
        update_fuzzy_to_sheet(spreadsheet, main_store_latest, competitor_latest, score_cutoff=accuracy_cutoff)

# ===================================================================================
# ANALISIS
# ===================================================================================
df['Minggu'] = df['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
latest_entries_weekly = df.loc[df.groupby(['Minggu', 'Toko', 'Nama Produk'])['Tanggal'].idxmax()]
latest_entries_overall = df.loc[df.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
main_store_latest_overall = latest_entries_overall[latest_entries_overall['Toko']==my_store_name]
competitor_latest_overall = latest_entries_overall[latest_entries_overall['Toko']!=my_store_name]

# Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["⭐ Analisis Toko Saya", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

# -----------------------------------------------------------------------------------
with tab1:
    st.header(f"Analisis Kinerja Toko: {my_store_name}")
    top_products = main_store_latest_overall.sort_values('Terjual per Bulan', ascending=False).head(15)
    st.dataframe(top_products[['Nama Produk','Harga','Terjual per Bulan','Omzet']], use_container_width=True)

# -----------------------------------------------------------------------------------
with tab2:
    st.header(f"Perbandingan Produk '{my_store_name}' dengan Kompetitor")
    latest_date = df['Tanggal'].max()
    main_store_latest = df[(df['Toko']==my_store_name)&(df['Tanggal']==latest_date)]
    if not main_store_latest.empty:
        product_list = sorted(main_store_latest['Nama Produk'].unique())
        selected_product = st.selectbox("Pilih produk dari toko Anda:", product_list)
        if selected_product:
            product_info = main_store_latest[main_store_latest['Nama Produk']==selected_product].iloc[0]
            st.markdown(f"**Produk Pilihan Anda:** *{product_info['Nama Produk']}*")
            fuzzy_df = load_fuzzy_from_sheet()
            if not fuzzy_df.empty:
                matches = fuzzy_df[fuzzy_df["Produk Utama"]==selected_product]
                for _, m in matches.iterrows():
                    price_diff = int(m["Harga Kompetitor"]) - int(product_info["Harga"])
                    st.markdown(f"**Toko: {m['Toko Kompetitor']}** (Kemiripan: {m['Skor']}%)")
                    st.markdown(f"*{m['Produk Kompetitor']}*")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Harga Kompetitor", f"Rp {int(m['Harga Kompetitor']):,.0f}", delta=f"Rp {price_diff:,.0f}")
                    c2.metric("Status", m["Status Kompetitor"])
                    c3.metric("Stok", m["Stok Kompetitor"])
            else:
                st.info("Belum ada hasil fuzzy.")

# -----------------------------------------------------------------------------------
with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    if not competitor_latest_overall.empty:
        brand_analysis = competitor_latest_overall.groupby('Brand').agg(Total_Omzet=('Omzet','sum')).reset_index()
        st.dataframe(brand_analysis, use_container_width=True)

# -----------------------------------------------------------------------------------
with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    stock_trends = df.groupby(['Minggu','Toko','Status']).size().unstack(fill_value=0).reset_index()
    st.dataframe(stock_trends, use_container_width=True)

# -----------------------------------------------------------------------------------
with tab5:
    st.header("Analisis Kinerja Penjualan (Semua Toko)")
    all_stores_latest_per_week = latest_entries_weekly.groupby(['Minggu','Toko'])['Omzet'].sum().reset_index()
    fig = px.line(all_stores_latest_per_week, x='Minggu', y='Omzet', color='Toko', markers=True)
    st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------------------------------------------
with tab6:
    st.header("Analisis Produk Baru Mingguan")
    weeks = sorted(df['Minggu'].unique())
    if len(weeks)>=2:
        week_before, week_after = weeks[0], weeks[-1]
        all_stores = sorted(df['Toko'].unique())
        for store in all_stores:
            products_before = set(df[(df['Toko']==store)&(df['Minggu']==week_before)]['Nama Produk'])
            products_after = set(df[(df['Toko']==store
