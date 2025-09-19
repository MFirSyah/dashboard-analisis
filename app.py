# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI STABIL
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Koreksi:
#  1. Pembacaan header dibuat robust (otomatis ke huruf besar).
#  2. Metode otentikasi gspread diperbarui untuk memperbaiki error '_auth_request'.
#  3. Tampilan status proses dibuat lebih detail dan informatif.
#  4. Peringatan format tanggal (UserWarning) telah diperbaiki.
#  5. Proses penulisan SBERT diubah menjadi sistem batch untuk efisiensi.
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
from datetime import datetime
import numpy as np
# Library ML/AI
from sentence_transformers import SentenceTransformer, util
import torch


# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

def standardize_columns(df):
    """Mengubah semua nama kolom menjadi huruf besar dan menghapus spasi."""
    df.columns = df.columns.str.strip().str.upper()
    return df

# --- FUNGSI UNTUK KONEKSI GOOGLE SHEETS (DIPERBAIKI) ---
def get_gspread_client():
    """Membuat dan mengembalikan client gspread yang diautentikasi menggunakan metode terbaru."""
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
        "auth_uri": st.secrets["gcp_auth_uri"],
        "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    # Menggunakan metode otentikasi yang direkomendasikan gspread v5+
    return gspread.service_account_from_dict(creds_dict, scopes=scopes)

# --- FUNGSI UNTUK MEMBACA WORKSHEET MENJADI DATAFRAME ---
def get_df_from_ws(worksheet):
    """Membaca data dari worksheet, menstandardisasi kolom, dan mengembalikannya."""
    df = get_as_dataframe(worksheet, evaluate_formulas=True).dropna(how="all")
    return standardize_columns(df)

# --- FUNGSI UNTUK MEMUAT SEMUA DATA MENTAH (DENGAN STATUS DETAIL) ---
@st.cache_data(ttl=600)
def load_all_data():
    """Memuat semua data dengan tampilan status yang lebih detail."""
    status_placeholder = st.empty()
    try:
        status_placeholder.info("🔗 Menghubungkan ke Google Sheets...")
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(st.secrets["gcp_spreadsheet_id"])
        
        all_dfs = []
        excluded_sheets = ['DATABASE', 'hasil_fuzzy']
        worksheets = spreadsheet.worksheets()
        
        status_placeholder.info("📥 Membaca semua data dari worksheet...")
        for i, ws in enumerate(worksheets):
            if ws.title not in excluded_sheets:
                st.text(f"   - Membaca '{ws.title}'...")
                df = get_df_from_ws(ws)
                toko_status = ws.title.split(' - REKAP - ')
                if len(toko_status) == 2:
                    df['TOKO'] = toko_status[0].strip()
                    df['STATUS'] = 'Tersedia' if toko_status[1].strip() in ['RE', 'READY'] else 'Habis'
                    all_dfs.append(df)
        
        st.text("✅ Selesai membaca worksheet.")

        if not all_dfs:
            status_placeholder.error("Tidak ada data yang dapat dimuat. Pastikan nama worksheet sudah benar.")
            return None, None, None

        status_placeholder.info("⚙️ Menggabungkan dan memproses data...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        
        # Standardisasi nama kolom utama setelah digabung
        full_df = standardize_columns(full_df)

        # PERBAIKAN: Menambahkan dayfirst=True untuk memastikan format tanggal DD/MM/YYYY dibaca dengan benar
        full_df['TANGGAL'] = pd.to_datetime(full_df['TANGGAL'], errors='coerce', dayfirst=True)
        full_df = full_df.dropna(subset=['TANGGAL'])
        
        # Pisahkan data DB Klik dan Kompetitor
        df_dbklik = full_df[full_df['TOKO'] == 'DB KLIK'].copy()
        df_competitors = full_df[full_df['TOKO'] != 'DB KLIK'].copy()

        # Coba muat hasil fuzzy yang sudah ada
        status_placeholder.info("🔍 Mengecek hasil analisis SBERT sebelumnya...")
        try:
            hasil_fuzzy_ws = spreadsheet.worksheet("hasil_fuzzy")
            df_hasil_sbert = get_df_from_ws(hasil_fuzzy_ws)
        except gspread.exceptions.WorksheetNotFound:
            df_hasil_sbert = pd.DataFrame() 

        status_placeholder.success("✔️ Semua data berhasil dimuat!")
        return df_dbklik, df_competitors, df_hasil_sbert, spreadsheet

    except Exception as e:
        status_placeholder.error(f"Gagal memuat data dari Google Sheets: {e}")
        return None, None, None, None

# --- FUNGSI UNTUK MEMUAT MODEL SBERT (DENGAN CACHE) ---
@st.cache_resource(show_spinner="Memuat model AI untuk analisis...")
def load_sbert_model():
    """Memuat model SentenceTransformer dan menyimpannya di cache."""
    return SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    
# --- FUNGSI UTAMA UNTUK ANALISIS SBERT (DENGAN PENULISAN BATCH) ---
def run_sbert_analysis(ss, df_dbklik, df_competitors):
    """
    Menjalankan analisis SBERT dan menulis hasilnya ke GSheets dalam bentuk batch.
    """
    status_placeholder = st.empty()
    
    status_placeholder.info("🧠 Memuat model AI SBERT...")
    model = load_sbert_model()
    
    status_placeholder.info("📝 Menyiapkan worksheet 'hasil_fuzzy' di Google Sheets...")
    try:
        worksheet = ss.worksheet("hasil_fuzzy")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = ss.add_worksheet(title="hasil_fuzzy", rows="1", cols="9")
        st.success("Worksheet 'hasil_fuzzy' baru telah dibuat.")

    worksheet.clear()
    header = [
        'Tanggal Analisis', 'Produk DBKlik', 'SKU DBKlik', 'Harga DBKlik',
        'Produk Kompetitor', 'Harga Kompetitor', 'Toko Kompetitor', 
        'Skor Kemiripan (%)', 'Brand'
    ]
    worksheet.append_row(header, value_input_option='USER_ENTERED')
    
    status_placeholder.info("🧹 Membersihkan dan mempersiapkan data...")
    df_dbklik_filtered = df_dbklik.dropna(subset=['NAMA', 'BRAND', 'HARGA']).copy()
    df_competitors_filtered = df_competitors.dropna(subset=['NAMA', 'BRAND', 'HARGA']).copy()
    
    brands = df_dbklik_filtered['BRAND'].unique()
    competitor_embeddings_by_brand = {}
    
    status_placeholder.info("📚 Membuat 'kamus' vektor untuk produk kompetitor...")
    with st.spinner("Proses ini akan memakan waktu sejenak..."):
        for brand in brands:
            brand_products = df_competitors_filtered[df_competitors_filtered['BRAND'] == brand]['NAMA'].tolist()
            if brand_products:
                competitor_embeddings_by_brand[brand] = model.encode(brand_products, convert_to_tensor=True)
    
    status_placeholder.info("🚀 Memulai perbandingan produk DB Klik dengan kompetitor...")
    progress_bar = st.progress(0, text="Menganalisis produk...")
    total_products = len(df_dbklik_filtered)
    
    # PERUBAHAN: Logika Batch
    batch_size = 50  # Tulis ke Gsheet setiap 50 produk DB Klik diproses
    results_batch = [] # List untuk menampung hasil sementara

    for i, (index, row) in enumerate(df_dbklik_filtered.iterrows()):
        product_name_dbklik = row['NAMA']
        product_brand = row['BRAND']
        
        if product_brand in competitor_embeddings_by_brand:
            competitor_products = df_competitors_filtered[df_competitors_filtered['BRAND'] == product_brand]
            embedding_dbklik = model.encode(product_name_dbklik, convert_to_tensor=True)
            cos_scores = util.cos_sim(embedding_dbklik, competitor_embeddings_by_brand[product_brand])[0]
            top_results = torch.topk(cos_scores, k=min(5, len(cos_scores)))
            
            for score, idx in zip(top_results[0], top_results[1]):
                if score.item() > 0.85:
                    match_row = competitor_products.iloc[idx.item()]
                    new_row = [
                        datetime.now().strftime('%Y-m-%d'),
                        row['NAMA'], row.get('SKU', 'N/A'), row['HARGA'],
                        match_row['NAMA'], match_row['HARGA'], match_row['TOKO'],
                        f"{score.item() * 100:.2f}", row['BRAND']
                    ]
                    results_batch.append(new_row)

        progress_bar.progress((i + 1) / total_products, text=f"Menganalisis produk: {i+1}/{total_products}")

        # Tulis batch ke Gsheet jika sudah mencapai ukuran batch ATAU jika ini adalah item terakhir
        if len(results_batch) >= batch_size or (i + 1) == total_products:
            if results_batch: # Pastikan batch tidak kosong
                st.text(f"   - Menulis {len(results_batch)} hasil ke Google Sheets...")
                worksheet.append_rows(results_batch, value_input_option='USER_ENTERED')
                results_batch = [] # Kosongkan batch setelah ditulis

    progress_bar.empty()
    status_placeholder.success("✅ Analisis SBERT selesai! Semua hasil telah ditulis ke Google Sheets.")
    
    df_hasil_sbert_updated = get_df_from_ws(worksheet)
    return df_hasil_sbert_updated

# ===================================================================================
# INISIALISASI APLIKASI
# ===================================================================================

if 'data_loaded' not in st.session_state:
    df_dbklik, df_competitors, df_hasil_sbert, ss = load_all_data()
    if df_dbklik is not None:
        st.session_state.df_dbklik = standardize_columns(df_dbklik)
        st.session_state.df_competitors = standardize_columns(df_competitors)
        st.session_state.df_hasil_sbert = standardize_columns(df_hasil_sbert)
        st.session_state.spreadsheet = ss
        st.session_state.data_loaded = True
    else:
        st.stop()

if 'sbert_check_done' not in st.session_state:
    st.session_state.sbert_check_done = True
    latest_competitor_date = st.session_state.df_competitors['TANGGAL'].max().strftime('%Y-%m-%d')
    latest_analysis_date = "1970-01-01"
    if not st.session_state.df_hasil_sbert.empty and 'TANGGAL ANALISIS' in st.session_state.df_hasil_sbert.columns:
        sbert_dates = pd.to_datetime(st.session_state.df_hasil_sbert['TANGGAL ANALISIS'], errors='coerce')
        if not sbert_dates.isnull().all():
            latest_analysis_date = sbert_dates.max().strftime('%Y-%m-%d')
            
    if latest_competitor_date > latest_analysis_date:
        st.warning("Data kompetitor lebih baru dari analisis terakhir. Menjalankan analisis SBERT secara otomatis...")
        st.session_state.df_hasil_sbert = run_sbert_analysis(
            st.session_state.spreadsheet,
            st.session_state.df_dbklik,
            st.session_state.df_competitors
        )
        st.rerun()

# ===================================================================================
# TAMPILAN UI STREAMLIT (NAMA KOLOM MENGGUNAKAN HURUF BESAR)
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

st.sidebar.header("Opsi Analisis")
if st.sidebar.button("Jalankan Ulang Analisis SBERT Manual", key="manual_sbert"):
    st.session_state.df_hasil_sbert = run_sbert_analysis(
        st.session_state.spreadsheet,
        st.session_state.df_dbklik,
        st.session_state.df_competitors
    )
    st.success("Analisis SBERT manual selesai!")
    st.rerun()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Ringkasan Penjualan", "⚖️ Perbandingan Harga (SBERT)", "🆕 Analisis Produk Baru",
    "🚀 Analisis Kompetitor", "💰 Distribusi Harga", "📦 Ketersediaan Stok"
])

# Menggunakan variabel agar tidak perlu menulis ulang
df_dbklik_main = st.session_state.df_dbklik
df_competitors_main = st.session_state.df_competitors
df_hasil_sbert_main = st.session_state.df_hasil_sbert

with tab1:
    st.header("Ringkasan Penjualan DB Klik")
    brand_filter = st.selectbox("Pilih Brand:", options=['Semua Brand'] + sorted(df_dbklik_main['BRAND'].unique().tolist()))
    
    filtered_dbklik = df_dbklik_main
    if brand_filter != 'Semua Brand':
        filtered_dbklik = filtered_dbklik[filtered_dbklik['BRAND'] == brand_filter]

    sales_by_brand = filtered_dbklik.groupby('BRAND')['TERJUAL/BLN'].sum().sort_values(ascending=False).head(15)
    fig_brand = px.bar(sales_by_brand, x=sales_by_brand.index, y='TERJUAL/BLN', title="Top 15 Brand Terlaris", labels={'index': 'Brand', 'TERJUAL/BLN': 'Total Terjual per Bulan'})
    st.plotly_chart(fig_brand, use_container_width=True)

    st.subheader("Produk Terlaris")
    top_products = filtered_dbklik.sort_values(by="TERJUAL/BLN", ascending=False).head(20)
    st.dataframe(top_products[['NAMA', 'BRAND', 'KATEGORI', 'HARGA', 'TERJUAL/BLN']])

with tab2:
    st.header("Perbandingan Harga Produk dengan Kompetitor (Metode SBERT)")
    if df_hasil_sbert_main.empty:
        st.warning("Data hasil perbandingan belum tersedia. Jalankan analisis SBERT melalui tombol di sidebar.")
    else:
        product_list = sorted(df_hasil_sbert_main['PRODUK DBKLIK'].unique().tolist())
        selected_product = st.selectbox("Pilih Produk DB Klik untuk Dilihat Perbandingannya:", options=product_list)
        
        if selected_product:
            comparison_df = df_hasil_sbert_main[df_hasil_sbert_main['PRODUK DBKLIK'] == selected_product].copy()
            comparison_df['HARGA DBKLIK'] = pd.to_numeric(comparison_df['HARGA DBKLIK'], errors='coerce')
            comparison_df['HARGA KOMPETITOR'] = pd.to_numeric(comparison_df['HARGA KOMPETITOR'], errors='coerce')
            comparison_df['Selisih'] = comparison_df['HARGA KOMPETITOR'] - comparison_df['HARGA DBKLIK']
            
            comparison_df['SKOR KEMIRIPAN (%)'] = pd.to_numeric(comparison_df['SKOR KEMIRIPAN (%)'], errors='coerce').map('{:,.2f}%'.format)
            comparison_df['HARGA DBKLIK'] = comparison_df['HARGA DBKLIK'].map('Rp {:,.0f}'.format)
            comparison_df['HARGA KOMPETITOR'] = comparison_df['HARGA KOMPETITOR'].map('Rp {:,.0f}'.format)
            
            def format_selisih(x):
                if pd.isna(x): return "N/A"
                if x > 0: return f"Lebih Mahal Rp {x:,.0f}"
                elif x < 0: return f"Lebih Murah Rp {abs(x):,.0f}"
                else: return "Harga Sama"
            
            comparison_df['Keterangan'] = comparison_df['Selisih'].apply(format_selisih)
            st.dataframe(comparison_df[['TOKO KOMPETITOR', 'PRODUK KOMPETITOR', 'HARGA KOMPETITOR', 'Keterangan', 'SKOR KEMIRIPAN (%)']], use_container_width=True)

with tab3:
    st.header("Analisis Produk Baru di Toko Kompetitor")
    df_combined_tab3 = pd.concat([df_dbklik_main, df_competitors_main], ignore_index=True)
    df_combined_tab3['MINGGU'] = df_combined_tab3['TANGGAL'].dt.to_period('W').astype(str)
    
    all_brands_tab3 = ['Semua Brand'] + sorted(df_combined_tab3['BRAND'].dropna().unique().tolist())
    brand_filter_tab3 = st.selectbox("Filter berdasarkan Brand:", all_brands_tab3, key="brand_tab3")

    df_filtered = df_combined_tab3
    if brand_filter_tab3 != 'Semua Brand':
        df_filtered = df_filtered[df_filtered['BRAND'] == brand_filter_tab3]
        
    weeks = sorted(df_filtered['MINGGU'].unique())
    if len(weeks) >= 2:
        col1, col2 = st.columns(2)
        with col1: week_before = st.selectbox("Pilih Minggu Pembanding:", weeks)
        with col2: week_after = st.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            all_stores = sorted(df_filtered['TOKO'].unique())
            for store in all_stores:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_filtered[(df_filtered['TOKO'] == store) & (df_filtered['MINGGU'] == week_before) & (df_filtered['STATUS'] == 'Tersedia')]['NAMA'])
                    products_after = set(df_filtered[(df_filtered['TOKO'] == store) & (df_filtered['MINGGU'] == week_after) & (df_filtered['STATUS'] == 'Tersedia')]['NAMA'])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_filtered[df_filtered['NAMA'].isin(new_products) & (df_filtered['TOKO'] == store) & (df_filtered['MINGGU'] == week_after)].copy()
                        new_products_df['HARGA_FMT'] = new_products_df['HARGA'].apply(lambda x: f"Rp {x:,.0f}")
                        st.dataframe(new_products_df[['NAMA', 'HARGA_FMT']], use_container_width=True)
    else:
        st.info("Tidak cukup data mingguan untuk melakukan perbandingan.")

with tab4:
    st.header("🚀 Analisis Kompetitor")
    products_per_store = df_competitors_main['TOKO'].value_counts()
    fig_products_store = px.bar(products_per_store, x=products_per_store.index, y=products_per_store.values, title="Jumlah Produk yang Dijual per Toko", labels={'index': 'Toko', 'y': 'Jumlah Produk'})
    st.plotly_chart(fig_products_store, use_container_width=True)

    st.subheader("Top 20 Produk Terlaris di Semua Kompetitor")
    top_competitor_products = df_competitors_main.sort_values(by="TERJUAL/BLN", ascending=False).head(20)
    st.dataframe(top_competitor_products[['NAMA', 'TOKO', 'BRAND', 'HARGA', 'TERJUAL/BLN']], use_container_width=True)

with tab5:
    st.header("💰 Distribusi Harga")
    df_combined_tab5 = pd.concat([df_dbklik_main, df_competitors_main], ignore_index=True)
    df_combined_tab5['HARGA'] = pd.to_numeric(df_combined_tab5['HARGA'], errors='coerce')
    df_combined_tab5.dropna(subset=['HARGA'], inplace=True)
    
    all_brands_tab5 = ['Semua Brand'] + sorted(df_combined_tab5['BRAND'].dropna().unique().tolist())
    brand_filter_tab5 = st.selectbox("Pilih Brand untuk Perbandingan Harga:", all_brands_tab5, key="brand_tab5")

    df_filtered_harga = df_combined_tab5
    if brand_filter_tab5 != 'Semua Brand':
        df_filtered_harga = df_filtered_harga[df_filtered_harga['BRAND'] == brand_filter_tab5]
        
    fig_price_dist = px.box(df_filtered_harga, x='TOKO', y='HARGA', title=f"Distribusi Harga untuk Brand: {brand_filter_tab5}", labels={'TOKO': 'Toko', 'HARGA': 'Harga Produk (Rp)'}, points="outliers")
    fig_price_dist.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(fig_price_dist, use_container_width=True)

with tab6:
    st.header("📦 Ketersediaan Stok")
    df_combined_tab6 = pd.concat([df_dbklik_main, df_competitors_main], ignore_index=True)
    stock_status = df_combined_tab6.groupby(['TOKO', 'STATUS']).size().reset_index(name='JUMLAH')
    
    fig_stock = px.bar(stock_status, x='TOKO', y='JUMLAH', color='STATUS', title="Jumlah Produk Berdasarkan Status Ketersediaan", labels={'TOKO': 'Toko', 'JUMLAH': 'Jumlah Produk'}, barmode='group', color_discrete_map={'Tersedia': 'green', 'Habis': 'red'})
    fig_stock.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(fig_stock, use_container_width=True)

