# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import re
import gspread
from gspread_pandas import Spread, Client

# Konfigurasi halaman Streamlit
st.set_page_config(layout="wide", page_title="Dashboard Analisis Otomatis")

# --- FUNGSI-FUNGSI UTAMA ---

# Fungsi untuk mengambil dan memproses data dari Google Sheets
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...", ttl=600)
def load_data_from_gsheets():
    """
    Menghubungkan ke Google Sheets menggunakan kredensial dari st.secrets,
    mengambil semua data dari sheet, dan memprosesnya menjadi DataFrame.
    """
    try:
        creds_dict = st.secrets["gcp_service_account"]
        client = Client(creds=gspread.service_account_from_dict(creds_dict))
    except Exception as e:
        st.error(f"Gagal memuat kredensial dari Streamlit Secrets. Pastikan secrets sudah diatur dengan benar. Error: {e}")
        return pd.DataFrame(), pd.DataFrame(), None

    # !!! PENTING: Ganti dengan NAMA PERSIS file Google Sheets Anda !!!
    spreadsheet_name = "NAMA_FILE_GOOGLE_SHEETS_ANDA"
    try:
        spread = Spread(spreadsheet_name, client=client)
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Spreadsheet dengan nama '{spreadsheet_name}' tidak ditemukan. Pastikan nama sudah benar dan sudah dibagikan (shared) ke email service account.")
        return pd.DataFrame(), pd.DataFrame(), None

    rekap_list_df = []
    database_df = pd.DataFrame()
    my_store_name = None

    # Loop melalui semua sheet di dalam file Google Sheets
    for sheet in spread.sheets:
        sheet_title = sheet.title
        st.write(f"Membaca sheet: {sheet_title}...") # Indikator proses

        is_database_file = "DATABASE" in sheet_title.upper()
        is_rekap_file = "REKAP" in sheet_title.upper()

        if is_database_file:
            database_df = spread.sheet_to_df(index=None, sheet=sheet)
            database_df.columns = [str(col).strip().upper() for col in database_df.columns]
            db_store_name_match = re.match(r"^(.*?) - DATABASE", sheet_title, re.IGNORECASE)
            if db_store_name_match:
                my_store_name = db_store_name_match.group(1).strip()
        
        elif is_rekap_file:
            df_sheet = spread.sheet_to_df(index=None, sheet=sheet)
            df_sheet.columns = [str(col).strip().upper() for col in df_sheet.columns]
            
            store_name_match = re.match(r"^(.*?) - REKAP", sheet_title, re.IGNORECASE)
            store_name = store_name_match.group(1).strip() if store_name_match else "Toko Tidak Dikenal"
            df_sheet['Toko'] = store_name

            # Logika status berdasarkan nama sheet (bisa disesuaikan)
            if "READY" in sheet_title.upper():
                df_sheet['Status'] = 'Tersedia'
            elif "HABIS" in sheet_title.upper():
                df_sheet['Status'] = 'Habis'
            else:
                df_sheet['Status'] = 'Tersedia' # Default status
            
            rekap_list_df.append(df_sheet)

    if not rekap_list_df:
        return pd.DataFrame(), pd.DataFrame(), None

    # Bagian pemrosesan data (sama seperti kode lama Anda)
    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    column_mapping = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga'}
    rekap_df.rename(columns=column_mapping, inplace=True)
    
    required_cols = ['Tanggal', 'Nama Produk', 'Harga', 'Terjual per Bulan']
    for col in required_cols:
        if col not in rekap_df.columns:
            st.error(f"Kolom krusial '{col}' tidak ditemukan di salah satu sheet REKAP. Pastikan nama kolom sudah benar.")
            return pd.DataFrame(), pd.DataFrame(), None

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce')
    def clean_price(price):
        try:
            # Membersihkan format mata uang yang lebih robus
            price_str = re.sub(r'[^\d]', '', str(price))
            return pd.to_numeric(price_str, errors='coerce')
        except: 
            return None
    rekap_df['Harga'] = rekap_df['Harga'].apply(clean_price)
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0).astype(int)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)
    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True)
    
    st.success("Semua data berhasil diambil dan diproses!")
    return rekap_df.sort_values('Tanggal'), database_df, my_store_name


def get_smart_matches(query_product_info, competitor_df, limit=5, score_cutoff=90):
    query_name = query_product_info['Nama Produk']
    query_lower = query_name.lower()
    query_brand = query_lower.split()[0]
    identifiers = set(re.findall(r'\b([a-z0-9]*[a-z][0-9][a-z0-9]*|[a-z0-9]*[0-9][a-z][a-z0-9]*)\b', query_lower))
    all_caps_words = set(re.findall(r'\b[A-Z]{3,}\b', query_name))
    identifiers.update({word.lower() for word in all_caps_words})
    if not identifiers:
        identifiers = {word for word in query_lower.split() if len(word) > 4 and word.isalnum()}
    competitor_product_list = competitor_df['Nama Produk'].tolist()
    candidates = process.extract(query_name, competitor_product_list, limit=20, scorer=fuzz.token_set_ratio)
    smart_results = []
    for candidate_name, score in candidates:
        if len(smart_results) >= limit: break
        if score < score_cutoff: continue
        candidate_lower = candidate_name.lower()
        if query_brand not in candidate_lower: continue
        if not any(identifier in candidate_lower for identifier in identifiers): continue
        smart_results.append((candidate_name, score))
    return smart_results

@st.cache_data(show_spinner="Menganalisis produk yang memiliki kemiripan di toko lain...")
def find_products_with_matches(_main_store_df, _competitor_df):
    if _main_store_df.empty or _competitor_df.empty:
        return set()
    products_with_matches_set = set()
    latest_date = _main_store_df['Tanggal'].max()
    _main_store_df_latest = _main_store_df[_main_store_df['Tanggal'] == latest_date]
    for _, product_row in _main_store_df_latest.iterrows():
        matches = get_smart_matches(product_row, _competitor_df, limit=1, score_cutoff=90)
        if matches:
            products_with_matches_set.add(product_row['Nama Produk'])
    return products_with_matches_set

def style_matched_products(row, matched_set):
    if row['Nama Produk'] in matched_set:
        return ['background-color: lightblue'] * len(row)
    else:
        return [''] * len(row)

def create_consolidated_weekly_summary(weekly_stats_list):
    if not weekly_stats_list: return pd.DataFrame()
    weekly_stats_list = sorted(weekly_stats_list, key=lambda x: x['start_date'])
    keterangan_labels = ["Total Omzet per Bulan", "Rata - Rata Omzet Per Hari", "Total Produk Terjual per Bulan", "Rata - Rata Terjual Per Hari", "Rata - Rata Harga Per Produk"]
    final_df = pd.DataFrame({'Keterangan': keterangan_labels})
    for i, stats in enumerate(weekly_stats_list):
        column_name = f"MINGGU {i + 1} (s/d {stats['start_date'].strftime('%d %b %Y')})"
        column_values = [f"Rp{stats['Total Omzet per Bulan']:,.2f}", f"Rp{stats['Rata - Rata Omzet Per Hari']:,.2f}", f"{stats['Total Produk Terjual per Bulan']:,}", f"{stats['Rata - Rata Terjual Per Hari']:,}", f"Rp{stats['Rata - Rata Harga Per Produk']:,.2f}"]
        final_df[column_name] = column_values
    return final_df


# --- INTERFACE DASHBOARD ---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor Otomatis")

st.sidebar.header("Kontrol Analisis")
if st.sidebar.button("Tarik Data & Mulai Analisis 🚀"):
    
    df, db_df, my_store_name_from_db = load_data_from_gsheets()

    if df.empty:
        st.error("Gagal memuat data REKAP dari Google Sheets. Proses dihentikan.")
        st.stop()
    if db_df.empty:
        st.warning("Sheet DATABASE tidak ditemukan atau gagal dibaca. Tab 'Analisis Toko Saya' tidak akan tersedia.")
    if my_store_name_from_db is None and not db_df.empty:
        st.warning("Nama toko Anda tidak dapat diidentifikasi dari nama sheet DATABASE. Gunakan format 'NAMA TOKO - DATABASE'.")

    st.sidebar.header("Filter Data")
    all_stores_list = sorted(df['Toko'].unique())
    main_store_for_comp_options = all_stores_list
    if my_store_name_from_db in main_store_for_comp_options:
        main_store_index = main_store_for_comp_options.index(my_store_name_from_db)
    else:
        main_store_index = 0
        
    main_store_for_comp = st.sidebar.selectbox("Pilih Toko Utama untuk Perbandingan:", options=main_store_for_comp_options, index=main_store_index)
    min_date = df['Tanggal'].min().date()
    max_date = df['Tanggal'].max().date()
    start_date = st.sidebar.date_input("Tanggal Mulai", min_date, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("Tanggal Akhir", max_date, min_value=start_date, max_value=max_date)

    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date)
    df_filtered = df[(df['Tanggal'] >= start_datetime) & (df['Tanggal'] <= end_datetime)].copy()
    
    if df_filtered.empty:
        st.error("Tidak ada data pada rentang tanggal yang dipilih. Sesuaikan filter Anda.")
        st.stop()
        
    latest_date_in_range = df_filtered['Tanggal'].max()
    df_latest = df_filtered[df_filtered['Tanggal'] == latest_date_in_range].copy()
    main_store_df = df_filtered[df_filtered['Toko'] == main_store_for_comp].copy()
    competitor_df = df_filtered[df_filtered['Toko'] != main_store_for_comp].copy()

    # --- PEMBUATAN TABS ---
    TABS = []
    if not db_df.empty and my_store_name_from_db:
        TABS.append(f"⭐ Analisis Toko Saya ({my_store_name_from_db})")
    TABS.extend(["⚖️ Perbandingan Harga Kompetitor", "📦 Status Stok Produk", "💡 Rekomendasi Analisis", "📈 Kinerja Penjualan"])
    created_tabs = st.tabs(TABS)

    start_index = 0
    if not db_df.empty and my_store_name_from_db:
        my_store_tab = created_tabs[0]
        start_index = 1
        with my_store_tab:
            st.header(f"Analisis Kinerja Toko: {my_store_name_from_db}")
            my_store_rekap_df = df_filtered[df_filtered['Toko'] == my_store_name_from_db].copy()
            if my_store_rekap_df.empty:
                st.warning(f"Tidak ditemukan data rekap untuk toko '{my_store_name_from_db}' pada rentang tanggal terpilih.")
            else:
                # Letakkan semua visualisasi untuk tab "Analisis Toko Saya" di sini
                # Contoh: Kategori terlaris, produk terlaris, dll.
                st.subheader("Peta Kontribusi Omzet (Treemap)")
                omzet_per_produk = my_store_rekap_df.groupby('Nama Produk')['Omzet'].sum().reset_index()
                fig_treemap = px.treemap(omzet_per_produk, path=[px.Constant("Semua Produk"), 'Nama Produk'], values='Omzet', title='Kontribusi Omzet per Produk', hover_data={'Omzet':':,.0f'})
                fig_treemap.update_traces(textinfo="label+percent root")
                st.plotly_chart(fig_treemap, use_container_width=True)

    with created_tabs[start_index]:
        # Kode untuk tab "Perbandingan Harga Kompetitor"
        st.header(f"Perbandingan Produk '{main_store_for_comp}'")
        # ... (sisa kode tab ini)
        pass

    with created_tabs[start_index + 1]:
        # Kode untuk tab "Status Stok Produk"
        st.header("Status Stok per Toko (pada hari terakhir data)")
        # ... (sisa kode tab ini)
        pass
        
    with created_tabs[start_index + 2]:
        # Kode untuk tab "Rekomendasi Analisis"
        st.header("💡 Rekomendasi Analisis Lanjutan")
        # ... (sisa kode tab ini)
        pass

    with created_tabs[start_index + 3]:
        # Kode untuk tab "Kinerja Penjualan"
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        # ... (sisa kode tab ini)
        pass

else:
    st.info("👋 Selamat datang! Klik tombol di sidebar kiri untuk menarik data terbaru dari Google Sheets dan memulai analisis.")
