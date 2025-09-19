# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI OPTIMIZED
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets)
#  Peningkatan: Analisis SBERT sekarang menulis hasil langsung ke Google Sheets
#               untuk mengurangi penggunaan memori dan mencegah crash.
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
import plotly.express as px
import re
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from sentence_transformers import SentenceTransformer, util
import torch
from datetime import datetime
import numpy as np

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

# --- FUNGSI UNTUK KONEKSI GOOGLE SHEETS ---
def get_gspread_client():
    """Membuat dan mengembalikan client gspread yang diautentikasi."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds_dict = {
        "type": st.secrets["gcp_type"],
        "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"],
        "private_key": st.secrets["gcp_private_key"],
        "client_email": st.secrets["gcp_client_email"],
        "client_id": st.secrets["gcp_client_id"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

# --- FUNGSI UNTUK MEMBACA WORKSHEET MENJADI DATAFRAME ---
def get_df_from_ws(worksheet):
    """Membaca data dari worksheet dan mengembalikannya sebagai DataFrame."""
    return get_as_dataframe(worksheet, evaluate_formulas=True).dropna(how="all")

# --- FUNGSI UNTUK MEMUAT SEMUA DATA MENTAH ---
@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_all_data():
    """Memuat semua data dari semua worksheet yang relevan di Google Sheets."""
    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(st.secrets["gcp_spreadsheet_id"])
        
        all_dfs = []
        excluded_sheets = ['DATABASE', 'hasil_fuzzy']
        worksheets = spreadsheet.worksheets()

        for ws in worksheets:
            if ws.title not in excluded_sheets:
                df = get_df_from_ws(ws)
                toko_status = ws.title.split(' - REKAP - ')
                if len(toko_status) == 2:
                    df['TOKO'] = toko_status[0].strip()
                    df['STATUS'] = 'Tersedia' if toko_status[1].strip() in ['RE', 'READY'] else 'Habis'
                    all_dfs.append(df)

        if not all_dfs:
            st.error("Tidak ada data yang dapat dimuat. Pastikan nama worksheet sudah benar.")
            return None, None, None

        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df['TANGGAL'] = pd.to_datetime(full_df['TANGGAL'], errors='coerce')
        full_df = full_df.dropna(subset=['TANGGAL'])
        
        # Pisahkan data DB Klik dan Kompetitor
        df_dbklik = full_df[full_df['TOKO'] == 'DB KLIK'].copy()
        df_competitors = full_df[full_df['TOKO'] != 'DB KLIK'].copy()

        # Coba muat hasil fuzzy yang sudah ada
        try:
            hasil_fuzzy_ws = spreadsheet.worksheet("hasil_fuzzy")
            df_hasil_sbert = get_df_from_ws(hasil_fuzzy_ws)
        except gspread.exceptions.WorksheetNotFound:
            df_hasil_sbert = pd.DataFrame() # Buat dataframe kosong jika sheet tidak ada

        return df_dbklik, df_competitors, df_hasil_sbert, spreadsheet

    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheets: {e}")
        return None, None, None, None

# --- FUNGSI UNTUK MEMUAT MODEL SBERT (DENGAN CACHE) ---
@st.cache_resource(show_spinner="Memuat model AI untuk analisis...")
def load_sbert_model():
    """Memuat model SentenceTransformer dan menyimpannya di cache."""
    return SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    
# --- FUNGSI UTAMA UNTUK ANALISIS SBERT ---
def run_sbert_analysis(ss, df_dbklik, df_competitors):
    """
    Menjalankan analisis kemiripan produk menggunakan SBERT.
    Hasilnya ditulis langsung ke Google Sheets per baris untuk menghemat memori.
    """
    model = load_sbert_model()
    
    st.info("Memulai analisis SBERT. Proses ini mungkin memakan waktu beberapa menit...")
    
    # Dapatkan worksheet, buat jika belum ada
    try:
        worksheet = ss.worksheet("hasil_fuzzy")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = ss.add_worksheet(title="hasil_fuzzy", rows="1", cols="9")
        st.success("Worksheet 'hasil_fuzzy' baru telah dibuat.")

    # Kosongkan sheet dan tulis header baru
    worksheet.clear()
    header = [
        'TANGGAL Analisis', 'Produk DBKlik', 'SKU DBKlik', 'HARGA DBKlik',
        'Produk Kompetitor', 'HARGA Kompetitor', 'Toko Kompetitor', 
        'Skor Kemiripan (%)', 'Brand'
    ]
    worksheet.append_row(header, value_input_option='USER_ENTERED')
    
    # Persiapan data
    df_dbklik_filtered = df_dbklik.dropna(subset=['NAMA', 'BRAND', 'HARGA']).copy()
    df_competitors_filtered = df_competitors.dropna(subset=['NAMA', 'BRAND', 'HARGA']).copy()
    
    brands = df_dbklik_filtered['BRAND'].unique()
    competitor_embeddings_by_brand = {}

    # Buat embeddings untuk setiap brand kompetitor sekali saja
    with st.spinner("Membuat 'kamus' produk kompetitor..."):
        for brand in brands:
            brand_products = df_competitors_filtered[df_competitors_filtered['BRAND'] == brand]['NAMA'].tolist()
            if brand_products:
                competitor_embeddings_by_brand[brand] = model.encode(brand_products, convert_to_tensor=True)

    # Setup progress bar
    progress_bar = st.progress(0)
    total_products = len(df_dbklik_filtered)
    
    # Loop utama untuk setiap produk di DB Klik
    for index, row in df_dbklik_filtered.iterrows():
        product_name_dbklik = row['NAMA']
        product_brand = row['BRAND']
        
        # Inisialisasi list untuk menampung baris yang akan ditulis untuk produk ini
        rows_to_write = []
        
        # Lakukan perbandingan hanya jika ada produk kompetitor dengan brand yang sama
        if product_brand in competitor_embeddings_by_brand:
            competitor_products = df_competitors_filtered[df_competitors_filtered['BRAND'] == product_brand]
            
            # Encode produk DB Klik saat ini
            embedding_dbklik = model.encode(product_name_dbklik, convert_to_tensor=True)
            
            # Hitung cosine similarity
            cos_scores = util.cos_sim(embedding_dbklik, competitor_embeddings_by_brand[product_brand])[0]
            top_results = torch.topk(cos_scores, k=min(5, len(cos_scores)))
            
            # Proses hasil
            for score, idx in zip(top_results[0], top_results[1]):
                if score.item() > 0.85: # Ambil yang skornya di atas 85%
                    match_row = competitor_products.iloc[idx.item()]
                    # Siapkan baris data sebagai list
                    new_row = [
                        datetime.now().strftime('%Y-%m-%d'),
                        row['NAMA'],
                        row.get('SKU', 'N/A'),
                        row['HARGA'],
                        match_row['NAMA'],
                        match_row['HARGA'],
                        match_row['TOKO'],
                        f"{score.item() * 100:.2f}",
                        row['BRAND']
                    ]
                    rows_to_write.append(new_row)

        # Tulis hasil untuk produk ini ke spreadsheet jika ada yang cocok
        if rows_to_write:
            worksheet.append_rows(rows_to_write, value_input_option='USER_ENTERED')
            
        # Update progress bar
        progress_bar.progress((index + 1) / total_products)

    progress_bar.empty()
    st.success("Analisis SBERT selesai dan semua hasil telah ditulis ke Google Sheets.")
    
    # Muat ulang data hasil dari sheet untuk ditampilkan di UI
    df_hasil_sbert_updated = get_df_from_ws(worksheet)
    return df_hasil_sbert_updated

# ===================================================================================
# INISIALISASI APLIKASI
# ===================================================================================

# Muat semua data saat aplikasi pertama kali dijalankan
if 'data_loaded' not in st.session_state:
    df_dbklik, df_competitors, df_hasil_sbert, ss = load_all_data()
    if df_dbklik is not None:
        st.session_state.df_dbklik = df_dbklik
        st.session_state.df_competitors = df_competitors
        st.session_state.df_hasil_sbert = df_hasil_sbert
        st.session_state.spreadsheet = ss
        st.session_state.data_loaded = True
    else:
        st.error("Tidak dapat memuat data. Periksa koneksi atau konfigurasi Google Sheets Anda.")
        st.stop()

# Cek apakah analisis SBERT perlu dijalankan secara otomatis
if 'sbert_check_done' not in st.session_state:
    st.session_state.sbert_check_done = True # Tandai agar tidak dicek berulang kali
    
    # Ambil TANGGAL terbaru dari data kompetitor
    latest_competitor_date = st.session_state.df_competitors['TANGGAL'].max().strftime('%Y-%m-%d')
    
    # Ambil TANGGAL analisis terakhir dari hasil sbert
    latest_analysis_date = "1970-01-01" # TANGGAL default jika hasil kosong
    if not st.session_state.df_hasil_sbert.empty and 'TANGGAL Analisis' in st.session_state.df_hasil_sbert.columns:
        # Konversi ke datetime untuk memastikan perbandingan benar
        sbert_dates = pd.to_datetime(st.session_state.df_hasil_sbert['TANGGAL Analisis'], errors='coerce')
        if not sbert_dates.isnull().all():
            latest_analysis_date = sbert_dates.max().strftime('%Y-%m-%d')
            
    # Bandingkan TANGGAL
    if latest_competitor_date > latest_analysis_date:
        st.warning("Data kompetitor lebih baru dari analisis terakhir. Menjalankan analisis SBERT secara otomatis...")
        with st.spinner("Mohon tunggu, proses otomatis sedang berjalan..."):
            st.session_state.df_hasil_sbert = run_sbert_analysis(
                st.session_state.spreadsheet,
                st.session_state.df_dbklik,
                st.session_state.df_competitors
            )
        st.rerun()

# ===================================================================================
# TAMPILAN UI STREAMLIT
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

# --- SIDEBAR ---
st.sidebar.header("Opsi Analisis")
if st.sidebar.button("Jalankan Ulang Analisis SBERT Manual", key="manual_sbert"):
    with st.spinner("Analisis SBERT manual sedang berjalan..."):
        st.session_state.df_hasil_sbert = run_sbert_analysis(
            st.session_state.spreadsheet,
            st.session_state.df_dbklik,
            st.session_state.df_competitors
        )
    st.success("Analisis SBERT manual selesai!")
    st.rerun()


# --- TABS ---
tab1, tab2, tab3 = st.tabs(["📈 Ringkasan Penjualan", "⚖️ Perbandingan HARGA (SBERT)", "🆕 Analisis Produk Baru"])

with tab1:
    st.header("Ringkasan Penjualan DB Klik")
    
    col1, col2 = st.columns(2)
    with col1:
        brand_filter = st.selectbox("Pilih Brand:", options=['Semua Brand'] + sorted(st.session_state.df_dbklik['BRAND'].unique().tolist()))
    
    filtered_dbklik = st.session_state.df_dbklik
    if brand_filter != 'Semua Brand':
        filtered_dbklik = filtered_dbklik[filtered_dbklik['BRAND'] == brand_filter]

    # Grafik penjualan per brand
    sales_by_brand = filtered_dbklik.groupby('BRAND')['TERJUAL/BLN'].sum().sort_values(ascending=False).head(15)
    fig_brand = px.bar(sales_by_brand, x=sales_by_brand.index, y='TERJUAL/BLN', title="Top 15 Brand Terlaris", labels={'index': 'Brand', 'TERJUAL/BLN': 'Total Terjual per Bulan'})
    st.plotly_chart(fig_brand, use_container_width=True)

    # Tabel produk terlaris
    st.subheader("Produk Terlaris")
    top_products = filtered_dbklik.sort_values(by="TERJUAL/BLN", ascending=False).head(20)
    st.dataframe(top_products[['NAMA', 'BRAND', 'KATEGORI', 'HARGA', 'TERJUAL/BLN']])


with tab2:
    st.header("Perbandingan HARGA Produk dengan Kompetitor (Metode SBERT)")
    
    if st.session_state.df_hasil_sbert.empty:
        st.warning("Data hasil perbandingan belum tersedia. Silakan jalankan analisis SBERT melalui tombol di sidebar.")
    else:
        # Dropdown untuk memilih produk DB Klik
        product_list = sorted(st.session_state.df_hasil_sbert['Produk DBKlik'].unique().tolist())
        selected_product = st.selectbox("Pilih Produk DB Klik untuk Dilihat Perbandingannya:", options=product_list)
        
        if selected_product:
            comparison_df = st.session_state.df_hasil_sbert[st.session_state.df_hasil_sbert['Produk DBKlik'] == selected_product].copy()
            
            # Konversi kolom ke numerik untuk perhitungan
            comparison_df['HARGA DBKlik'] = pd.to_numeric(comparison_df['HARGA DBKlik'], errors='coerce')
            comparison_df['HARGA Kompetitor'] = pd.to_numeric(comparison_df['HARGA Kompetitor'], errors='coerce')
            
            # Hitung selisih
            comparison_df['Selisih'] = comparison_df['HARGA Kompetitor'] - comparison_df['HARGA DBKlik']
            
            # Format tampilan
            comparison_df['Skor Kemiripan (%)'] = pd.to_numeric(comparison_df['Skor Kemiripan (%)'], errors='coerce').map('{:,.2f}%'.format)
            comparison_df['HARGA DBKlik'] = comparison_df['HARGA DBKlik'].map('Rp {:,.0f}'.format)
            comparison_df['HARGA Kompetitor'] = comparison_df['HARGA Kompetitor'].map('Rp {:,.0f}'.format)
            
            def format_selisih(x):
                if pd.isna(x):
                    return "N/A"
                if x > 0:
                    return f"Lebih Mahal Rp {x:,.0f}"
                elif x < 0:
                    return f"Lebih Murah Rp {abs(x):,.0f}"
                else:
                    return "HARGA Sama"
            
            comparison_df['Keterangan'] = comparison_df['Selisih'].apply(format_selisih)

            st.dataframe(comparison_df[['Toko Kompetitor', 'Produk Kompetitor', 'HARGA Kompetitor', 'Keterangan', 'Skor Kemiripan (%)']], use_container_width=True)


with tab3:
    st.header("Analisis Produk Baru di Toko Kompetitor")
    
    # Gabungkan semua data untuk analisis ini
    df_combined = pd.concat([st.session_state.df_dbklik, st.session_state.df_competitors], ignore_index=True)
    df_combined['Minggu'] = df_combined['TANGGAL'].dt.to_period('W').astype(str)
    
    # Filter data
    all_brands_tab3 = ['Semua Brand'] + sorted(df_combined['BRAND'].unique().tolist())
    brand_filter_tab3 = st.selectbox("Filter berdasarkan Brand:", all_brands_tab3, key="brand_tab3")

    df_filtered = df_combined
    if brand_filter_tab3 != 'Semua Brand':
        df_filtered = df_filtered[df_filtered['BRAND'] == brand_filter_tab3]
        
    weeks = sorted(df_filtered['Minggu'].unique())
    if len(weeks) >= 2:
        col1, col2 = st.columns(2)
        with col1:
            week_before = st.selectbox("Pilih Minggu Pembanding:", weeks)
        with col2:
            week_after = st.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            all_stores = sorted(df_filtered['TOKO'].unique())
            for store in all_stores:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_filtered[(df_filtered['TOKO'] == store) & (df_filtered['Minggu'] == week_before) & (df_filtered['STATUS'] == 'Tersedia')]['NAMA'])
                    products_after = set(df_filtered[(df_filtered['TOKO'] == store) & (df_filtered['Minggu'] == week_after) & (df_filtered['STATUS'] == 'Tersedia')]['NAMA'])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_filtered[df_filtered['NAMA'].isin(new_products) & (df_filtered['TOKO'] == store) & (df_filtered['Minggu'] == week_after)].copy()
                        new_products_df['HARGA_fmt'] = new_products_df['HARGA'].apply(lambda x: f"Rp {x:,.0f}")
                        st.dataframe(new_products_df[['NAMA', 'HARGA_fmt']], use_container_width=True)
    else:
        st.info("Tidak cukup data mingguan untuk melakukan perbandingan.")

