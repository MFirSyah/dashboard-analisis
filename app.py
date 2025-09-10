import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from fuzzywuzzy import process, fuzz
import plotly.express as px
from datetime import datetime
import numpy as np

# --- KONFIGURASI AWAL ---
st.set_page_config(
    page_title="Analisis Produk DB KLIK vs Kompetitor",
    page_icon="🎯",
    layout="wide"
)

# --- FUNGSI-FUNGSI UTAMA (DENGAN CACHING) ---

def init_connection():
    """Menginisialisasi koneksi ke Google Sheets menggunakan st.secrets."""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Gagal menginisialisasi koneksi: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner="Menarik data dari Google Sheets...")
def tarik_data_dari_gsheet(spreadsheet_id, sheet_names):
    """Menarik data dari beberapa sheet dalam satu spreadsheet."""
    client = init_connection()
    if client is None: return None, "Koneksi Gagal"
    spreadsheet = client.open_by_key(spreadsheet_id)
    data_frames = {}
    for name in sheet_names:
        try:
            worksheet = spreadsheet.worksheet(name)
            all_values = worksheet.get_all_values()
            if not all_values or not all_values[0]:
                df = pd.DataFrame()
            else:
                headers = all_values[0]
                data = all_values[1:]
                df = pd.DataFrame(data, columns=headers)

            df.rename(columns={'NAMA': 'NAMA', 'TERJUAL/BLN': 'Terjual/Bulan', 'BRAND': 'BRAND'}, inplace=True)
            for col in ['HARGA', 'Terjual/Bulan', 'Omzet']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce')
            if 'TANGGAL' in df.columns:
                df['TANGGAL'] = pd.to_datetime(df['TANGGAL'], errors='coerce', dayfirst=True)
            data_frames[name] = df
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Sheet '{name}' tidak ditemukan, akan dilewati.")
            data_frames[name] = pd.DataFrame()
    return data_frames, None

@st.cache_data
def fuzzy_match_kategori(_df_produk, _df_database):
    """Memberi label kategori pada produk berdasarkan database master."""
    if _df_database.empty or 'NAMA' not in _df_database.columns or 'Kategori' not in _df_database.columns:
        _df_produk['Kategori'] = 'Tidak Terkategori'
        return _df_produk
    
    produk_list = _df_produk['NAMA'].tolist()
    database_list = _df_database['NAMA'].unique().tolist()
    kategori_map = pd.Series(_df_database['Kategori'].values, index=_df_database['NAMA']).to_dict()
    
    kategori_hasil = []
    for produk in produk_list:
        try:
            match, score = process.extractOne(produk, database_list, scorer=fuzz.token_sort_ratio)
            # Threshold tinggi (95) untuk memastikan akurasi matching ke database internal
            if score >= 95:
                kategori_hasil.append(kategori_map.get(match, 'Tidak Terkategori'))
            else:
                kategori_hasil.append('Tidak Terkategori')
        except:
            kategori_hasil.append('Tidak Terkategori')
            
    _df_produk['Kategori'] = kategori_hasil
    return _df_produk

@st.cache_data
def siapkan_data_analisis(data_frames, start_date, end_date):
    """Menggabungkan, membersihkan, dan mengkategorikan data untuk analisis."""
    # Proses DB KLIK
    db_klik_ready = data_frames.get("DB KLIK - REKAP - READY", pd.DataFrame())
    db_klik_habis = data_frames.get("DB KLIK - REKAP - HABIS", pd.DataFrame())
    df_db_klik_full = pd.concat([db_klik_ready, db_klik_habis])
    if 'Omzet' not in df_db_klik_full.columns and 'HARGA' in df_db_klik_full.columns and 'Terjual/Bulan' in df_db_klik_full.columns:
        df_db_klik_full['Omzet'] = df_db_klik_full['HARGA'] * df_db_klik_full['Terjual/Bulan']
    
    # Filter tanggal
    df_db_klik_full = df_db_klik_full.dropna(subset=['TANGGAL'])
    df_db_klik_full = df_db_klik_full[(df_db_klik_full['TANGGAL'] >= start_date) & (df_db_klik_full['TANGGAL'] <= end_date)]
    
    # Fuzzy match DB KLIK ke DATABASE
    df_database = data_frames.get("DATABASE", pd.DataFrame())
    df_db_klik_categorized = fuzzy_match_kategori(df_db_klik_full.copy(), df_database)
    
    # Proses Kompetitor
    store_list = sorted([name.replace(" - REKAP - READY", "") for name in data_frames if "READY" in name and "DB KLIK" not in name])
    competitor_dfs = []
    for store in store_list:
        df_ready = data_frames.get(f"{store} - REKAP - READY", pd.DataFrame())
        df_habis = data_frames.get(f"{store} - REKAP - HABIS", pd.DataFrame())
        df_full = pd.concat([df_ready, df_habis])
        if not df_full.empty:
            if 'Omzet' not in df_full.columns and 'HARGA' in df_full.columns and 'Terjual/Bulan' in df_full.columns:
                df_full['Omzet'] = df_full['HARGA'] * df_full['Terjual/Bulan']
            df_full['Toko'] = store
            # Filter tanggal
            df_full = df_full.dropna(subset=['TANGGAL'])
            df_full = df_full[(df_full['TANGGAL'] >= start_date) & (df_full['TANGGAL'] <= end_date)]
            competitor_dfs.append(df_full)
    
    df_competitors_full = pd.concat(competitor_dfs, ignore_index=True) if competitor_dfs else pd.DataFrame()
    
    return df_db_klik_categorized, df_competitors_full


# --- UI STREAMLIT ---

if 'analisis_dimulai' not in st.session_state: st.session_state.analisis_dimulai = False

if not st.session_state.analisis_dimulai:
    st.title("🎯 Dashboard Analisis Produk DB KLIK vs Kompetitor")
    st.markdown("Dashboard ini fokus untuk menganalisis setiap produk di **DB KLIK**, memberinya label kategori, dan membandingkannya dengan produk serupa di toko kompetitor.")
    if st.button("🚀 Tarik Data & Mulai Analisis"):
        GSHEET_ID = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
        SHEET_NAMES = ["DATABASE", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS", "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS", "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS", "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS", "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS", "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS", "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS", "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS", "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS"]
        st.session_state.data_cache, _ = tarik_data_dari_gsheet(GSHEET_ID, SHEET_NAMES)
        if st.session_state.data_cache:
            st.session_state.analisis_dimulai = True
            st.rerun()
else:
    all_data = st.session_state.data_cache
    st.title("🎯 Analisis Produk DB KLIK vs Kompetitor")

    with st.sidebar:
        st.title("⚙️ Pengaturan Analisis")
        if st.button("🔄 Tarik Ulang Data"):
            st.session_state.analisis_dimulai = False
            st.cache_data.clear()
            st.rerun()

        all_dates_list = [df['TANGGAL'].dropna() for df in all_data.values() if 'TANGGAL' in df.columns and not df['TANGGAL'].dropna().empty]
        if not all_dates_list:
            st.error("Gagal menemukan data TANGGAL yang valid.")
            st.stop()

        all_dates = pd.concat(all_dates_list)
        min_date, max_date = all_dates.min().date(), all_dates.max().date()
        
        selected_date_range = st.date_input("Pilih Rentang Analisis", (min_date, max_date), min_value=min_date, max_value=max_date)
        start_date = datetime.combine(selected_date_range[0], datetime.min.time()) if len(selected_date_range) > 0 else datetime.min
        end_date = datetime.combine(selected_date_range[1], datetime.max.time()) if len(selected_date_range) > 1 else datetime.max
        
        fuzzy_threshold = st.slider("Akurasi Pencocokan Kompetitor (%)", 50, 100, 85, help="Tingkat kemiripan untuk mencari produk di toko lain.")

    # --- PEMROSESAN DATA UTAMA ---
    df_db_klik, df_competitors = siapkan_data_analisis(all_data, start_date, end_date)

    if df_db_klik.empty:
        st.warning("Tidak ada data DB KLIK yang ditemukan pada rentang tanggal yang dipilih. Silakan sesuaikan filter di sidebar.")
        st.stop()

    # Buat daftar produk unik untuk dropdown
    latest_db_klik = df_db_klik.sort_values('TANGGAL').drop_duplicates('NAMA', keep='last')
    produk_list_options = latest_db_klik.sort_values('NAMA')['NAMA'].unique()

    st.header("Pilih Produk DB KLIK untuk Dianalisis")
    selected_product = st.selectbox("Cari dan pilih produk:", produk_list_options, index=0, help="Mulai ketik untuk mencari nama produk.")

    if selected_product:
        # --- TAMPILKAN INFO PRODUK UTAMA ---
        st.markdown("---")
        st.header(f"📊 Detail Produk: {selected_product}")
        
        produk_hist = df_db_klik[df_db_klik['NAMA'] == selected_product].sort_values('TANGGAL')
        produk_latest = produk_hist.iloc[-1]
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Kategori", produk_latest['Kategori'])
        col2.metric("Harga Terakhir", f"Rp{produk_latest['HARGA']:,.0f}")
        col3.metric("Terjual/Bulan", f"{produk_latest['Terjual/Bulan'] or 0:,.0f}")
        
        status = "Ready" if produk_latest['NAMA'] in all_data.get("DB KLIK - REKAP - READY", pd.DataFrame())['NAMA'].values else "Habis"
        col4.metric("Status Stok", status)

        # --- GRAFIK HISTORI HARGA ---
        fig_price = px.line(produk_hist, x='TANGGAL', y='HARGA', title=f'Histori Perubahan Harga', markers=True)
        fig_price.update_layout(yaxis_tickprefix='Rp', yaxis_tickformat=',.0f', yaxis_title="Harga", xaxis_title="Tanggal")
        st.plotly_chart(fig_price, use_container_width=True)

        # --- CARI & TAMPILKAN DATA KOMPETITOR ---
        st.header("⚔️ Perbandingan dengan Kompetitor")
        
        competitor_matches = []
        for toko in df_competitors['Toko'].unique():
            df_toko = df_competitors[df_competitors['Toko'] == toko]
            produk_toko_list = df_toko['NAMA'].unique()
            
            match_result = process.extractOne(selected_product, produk_toko_list, scorer=fuzz.token_sort_ratio)
            
            if match_result and match_result[1] >= fuzzy_threshold:
                match_name = match_result[0]
                latest_match = df_toko[df_toko['NAMA'] == match_name].sort_values('TANGGAL').iloc[-1]
                
                selisih = latest_match['HARGA'] - produk_latest['HARGA']
                
                status_kompetitor = "Ready" if latest_match['NAMA'] in all_data.get(f"{toko} - REKAP - READY", pd.DataFrame())['NAMA'].values else "Habis"

                competitor_matches.append({
                    'Toko': toko,
                    'Produk Kompetitor': match_name,
                    'Harga Kompetitor': latest_match['HARGA'],
                    'Selisih Harga': selisih,
                    'Status Kompetitor': status_kompetitor
                })

        if not competitor_matches:
            st.info("Tidak ditemukan produk serupa di toko kompetitor dengan tingkat akurasi yang dipilih.")
        else:
            df_compare = pd.DataFrame(competitor_matches).sort_values('Selisih Harga')
            
            def format_selisih(s):
                if pd.isna(s): return "N/A"
                if s > 0: return f"Lebih Mahal (Rp +{s:,.0f}) 🔺"
                elif s < 0: return f"Lebih Murah (Rp {s:,.0f}) ✅"
                return "Sama"
                
            df_compare['Perbandingan'] = df_compare['Selisih Harga'].apply(format_selisih)
            
            st.dataframe(df_compare[['Toko', 'Produk Kompetitor', 'Harga Kompetitor', 'Perbandingan', 'Status Kompetitor']],
                column_config={"Harga Kompetitor": st.column_config.NumberColumn(format="Rp %d")},
                use_container_width=True
            )

