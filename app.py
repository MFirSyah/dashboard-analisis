# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI STRUKTUR ASLI + CACHING
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Deskripsi: Kode ini mempertahankan struktur asli sambil mengimplementasikan 
#             caching fuzzy similarity untuk mempercepat performa Tab 2.
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
from thefuzz import process
import plotly.express as px
import re
import gspread
from google.oauth2.service_account import Credentials

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis Penjualan")

# ===================================================================================
# FUNGSI KONEKSI DAN PEMUATAN DATA (STRUKTUR ASLI)
# ===================================================================================

# Fungsi untuk otentikasi dan koneksi ke Google Sheets
def get_gspread_client():
    creds_dict = {
        "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key"],
        "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"],
    }
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

# Fungsi-fungsi pemuatan data per toko (dipertahankan sesuai permintaan)
@st.cache_data(show_spinner=False)
def load_data_per_sheet(_gs_client, sheet_name, store_name):
    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["gcp_spreadsheet_key"])
        worksheet = spreadsheet.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        df['Toko'] = store_name
        return df
    except Exception as e:
        st.error(f"Gagal memuat sheet '{sheet_name}': {e}")
        return pd.DataFrame()

# Fungsi untuk memuat data kamus dan database brand
@st.cache_data(show_spinner=False)
def load_kamus_brand(_gs_client):
    try:
        spreadsheet = _gs_client.open_by_key(st.secrets["gcp_spreadsheet_key"])
        kamus_ws = spreadsheet.worksheet("kamus_brand")
        db_brand_ws = spreadsheet.worksheet("DATABASE_BRAND")
        df_kamus = pd.DataFrame(kamus_ws.get_all_records())
        df_db_brand = pd.DataFrame(db_brand_ws.get_all_records())
        return df_kamus, df_db_brand
    except Exception as e:
        st.error(f"Gagal memuat kamus brand: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- FUNGSI BARU UNTUK FUZZY CACHING ---
@st.cache_data(show_spinner="Memuat data kemiripan produk...")
def load_fuzzy_data(_spreadsheet):
    """
    Fungsi cepat untuk memuat hasil kalkulasi fuzzy dari worksheet 'hasil_fuzzy'.
    """
    try:
        worksheet = _spreadsheet.worksheet("hasil_fuzzy")
        df_fuzzy = pd.DataFrame(worksheet.get_all_records())
        df_fuzzy['Skor_Kemiripan'] = pd.to_numeric(df_fuzzy['Skor_Kemiripan'])
        return df_fuzzy
    except gspread.WorksheetNotFound:
        st.warning("Worksheet 'hasil_fuzzy' tidak ditemukan. Silakan buat cache terlebih dahulu melalui Opsi Admin di sidebar.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Gagal memuat data fuzzy: {e}")
        return pd.DataFrame()

def update_fuzzy_cache(_spreadsheet, _df_master):
    """
    Fungsi 'berat' untuk menghitung ulang semua skor kemiripan dan menyimpannya ke worksheet.
    """
    if _df_master.empty:
        st.error("Data master kosong, tidak dapat memperbarui cache.")
        return

    with st.spinner("Memperbarui cache fuzzy similarity... Ini mungkin memakan waktu beberapa menit."):
        try:
            unique_products = _df_master['Nama Produk'].dropna().unique()
            fuzzy_results = []
            progress_bar = st.progress(0)
            total_products = len(unique_products)

            for i, product in enumerate(unique_products):
                similar_products = process.extract(product, unique_products, limit=6)
                for similar, score in similar_products:
                    if product != similar:
                        fuzzy_results.append({
                            'Produk_Asal': product,
                            'Produk_Serupa': similar,
                            'Skor_Kemiripan': score
                        })
                progress_bar.progress((i + 1) / total_products)

            fuzzy_df = pd.DataFrame(fuzzy_results)
            
            try:
                worksheet_to_delete = _spreadsheet.worksheet('hasil_fuzzy')
                _spreadsheet.del_worksheet(worksheet_to_delete)
            except gspread.WorksheetNotFound:
                pass
            
            worksheet = _spreadsheet.add_worksheet(title='hasil_fuzzy', rows="1", cols="1")
            worksheet.update([fuzzy_df.columns.values.tolist()] + fuzzy_df.values.tolist())
            
            st.success("Cache fuzzy similarity berhasil diperbarui!")
            st.info("Harap muat ulang halaman (refresh) untuk menggunakan data cache yang baru.")

        except Exception as e:
            st.error(f"Terjadi kesalahan saat memperbarui cache: {e}")

# ===================================================================================
# PROSES UTAMA
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

# Inisialisasi koneksi
gs_client = get_gspread_client()
spreadsheet_main = gs_client.open_by_key(st.secrets["gcp_spreadsheet_key"])

# Memuat semua data dengan struktur asli
with st.spinner("Memuat semua data dari Google Sheets..."):
    # Daftar semua sheet rekap
    sheet_configs = [
        {"sheet_name": "SURYA MITRA ONLINE - REKAP - RE", "store_name": "SURYA MITRA ONLINE"},
        {"sheet_name": "SURYA MITRA ONLINE - REKAP - HA", "store_name": "SURYA MITRA ONLINE"},
        {"sheet_name": "GG STORE - REKAP - READY", "store_name": "GG STORE"},
        {"sheet_name": "GG STORE - REKAP - HABIS", "store_name": "GG STORE"},
        {"sheet_name": "TECH ISLAND - REKAP - READY", "store_name": "TECH ISLAND"},
        {"sheet_name": "TECH ISLAND - REKAP - HABIS", "store_name": "TECH ISLAND"},
        {"sheet_name": "MULTIFUNGSI - REKAP - READY", "store_name": "MULTIFUNGSI"},
        {"sheet_name": "MULTIFUNGSI - REKAP - HABIS", "store_name": "MULTIFUNGSI"},
        {"sheet_name": "JAYA PC - REKAP - READY", "store_name": "JAYA PC"},
        {"sheet_name": "JAYA PC - REKAP - HABIS", "store_name": "JAYA PC"},
        {"sheet_name": "IT SHOP - REKAP - READY", "store_name": "IT SHOP"},
        {"sheet_name": "IT SHOP - REKAP - HABIS", "store_name": "IT SHOP"},
        {"sheet_name": "LEVEL99 - REKAP - READY", "store_name": "LEVEL99"},
        {"sheet_name": "LEVEL99 - REKAP - HABIS", "store_name": "LEVEL99"},
        {"sheet_name": "ABDITAMA - REKAP - READY", "store_name": "ABDITAMA"},
        {"sheet_name": "ABDITAMA - REKAP - HABIS", "store_name": "ABDITAMA"},
        {"sheet_name": "DB KLIK - REKAP - READY", "store_name": "DB KLIK"},
        {"sheet_name": "DB KLIK - REKAP - HABIS", "store_name": "DB KLIK"},
        {"sheet_name": "LOGITECH - REKAP - READY", "store_name": "LOGITECH"},
        {"sheet_name": "LOGITECH - REKAP - HABIS", "store_name": "LOGITECH"},
    ]
    
    all_dfs = [load_data_per_sheet(gs_client, config["sheet_name"], config["store_name"]) for config in sheet_configs]
    df_gabungan = pd.concat(all_dfs, ignore_index=True)

    # Memuat kamus brand
    df_kamus, df_db_brand = load_kamus_brand(gs_client)

# Hentikan jika data gagal dimuat
if df_gabungan.empty:
    st.error("Data utama gagal dimuat. Proses tidak dapat dilanjutkan.")
    st.stop()

# --- Pemrosesan Data (Struktur Asli) ---
df_gabungan.rename(columns={'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan'}, inplace=True)
required_cols = ['TANGGAL', 'Toko', 'Nama Produk', 'HARGA', 'Terjual per Bulan', 'BRAND']
for col in required_cols:
    if col not in df_gabungan.columns:
        df_gabungan[col] = None

df_gabungan = df_gabungan[required_cols]
df_gabungan['TANGGAL'] = pd.to_datetime(df_gabungan['TANGGAL'], errors='coerce')
df_gabungan['HARGA'] = pd.to_numeric(df_gabungan['HARGA'], errors='coerce')
df_gabungan['Terjual per Bulan'] = pd.to_numeric(df_gabungan['Terjual per Bulan'], errors='coerce')
df_gabungan.dropna(subset=['TANGGAL', 'Nama Produk', 'HARGA'], inplace=True)
df_gabungan['Minggu'] = df_gabungan['TANGGAL'].dt.strftime('Minggu %U, %Y')
df_gabungan['Status'] = df_gabungan['Terjual per Bulan'].apply(lambda x: 'Tersedia' if pd.notnull(x) and x > 0 else 'Stok Kosong')

# Standardisasi Brand (Struktur Asli)
kamus_dict = df_kamus.set_index('Alias')['Brand_Utama'].to_dict()
df_gabungan['BRAND'] = df_gabungan['BRAND'].replace(kamus_dict)

# --- MEMUAT DATA FUZZY YANG SUDAH DI-CACHE ---
df_fuzzy = load_fuzzy_data(spreadsheet_main)

# ===================================================================================
# SIDEBAR
# ===================================================================================
st.sidebar.header("Filter Data")
unique_weeks = sorted(df_gabungan['Minggu'].unique())
selected_week = st.sidebar.selectbox("Pilih Minggu Analisis", options=unique_weeks, index=len(unique_weeks)-1)

df_filtered = df_gabungan[df_gabungan['Minggu'] == selected_week].copy()

st.sidebar.header("Opsi Admin")
if st.sidebar.button("Perbarui Cache Fuzzy Similarity"):
    # Menggunakan df_gabungan (data master lengkap) untuk membuat cache
    update_fuzzy_cache(spreadsheet_main, df_gabungan)

# ===================================================================================
# LAYOUT UTAMA APLIKASI
# ===================================================================================
st.markdown(f"Menampilkan data untuk: **{selected_week}**")

tab1, tab2, tab3 = st.tabs(["📈 Ringkasan Umum", "🎯 Analisis Produk Serupa", "🆕 Deteksi Produk Baru"])

# --- TAB 1: RINGKASAN UMUM ---
with tab1:
    st.header("Ringkasan Umum Performa Toko")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Produk Terpantau", f"{df_filtered['Nama Produk'].nunique():,}")
    col2.metric("Total Toko Terpantau", f"{df_filtered['Toko'].nunique():,}")
    col3.metric("Total Brand", f"{df_filtered['BRAND'].nunique():,}")
    
    st.markdown("---")
    st.subheader("Distribusi Jumlah Produk per Toko")
    products_per_store = df_filtered.groupby('Toko')['Nama Produk'].nunique().sort_values(ascending=False)
    fig_bar = px.bar(products_per_store, x=products_per_store.index, y='Nama Produk', labels={'y': 'Jumlah Produk Unik', 'Toko': 'Nama Toko'}, text_auto=True)
    st.plotly_chart(fig_bar, use_container_width=True)

# --- TAB 2: ANALISIS PRODUK SERUPA (MENGGUNAKAN CACHE) ---
with tab2:
    st.header("Analisis Kemiripan Produk")

    if df_fuzzy.empty:
        st.info("Data kemiripan produk belum tersedia. Silakan buat cache melalui Opsi Admin di sidebar.")
    else:
        all_products_in_cache = sorted(df_fuzzy['Produk_Asal'].unique())
        product_choice = st.selectbox(
            "Pilih Produk untuk Dibandingkan",
            options=all_products_in_cache,
            index=0
        )
        min_score = st.slider("Tingkat Kemiripan Minimum (%)", 50, 100, 85)

        if product_choice:
            results_df = df_fuzzy[
                (df_fuzzy['Produk_Asal'] == product_choice) & 
                (df_fuzzy['Skor_Kemiripan'] >= min_score)
            ].sort_values(by='Skor_Kemiripan', ascending=False)
            
            st.write(f"Menampilkan produk yang mirip dengan **{product_choice}**:")
            
            if results_df.empty:
                st.write("Tidak ditemukan produk serupa dengan tingkat kemiripan tersebut.")
            else:
                similar_products_names = results_df['Produk_Serupa'].tolist()
                detailed_info = df_filtered[df_filtered['Nama Produk'].isin(similar_products_names)].copy()
                detailed_info = detailed_info[['Toko', 'Nama Produk', 'HARGA', 'Status']]
                
                final_display_df = pd.merge(
                    detailed_info,
                    results_df[['Produk_Serupa', 'Skor_Kemiripan']],
                    left_on='Nama Produk',
                    right_on='Produk_Serupa'
                ).drop(columns=['Produk_Serupa']).sort_values(by='Skor_Kemiripan', ascending=False)
                
                final_display_df['HARGA'] = final_display_df['HARGA'].apply(lambda x: f"Rp {x:,.0f}")
                
                st.dataframe(
                    final_display_df[['Toko', 'Nama Produk', 'HARGA', 'Status', 'Skor_Kemiripan']],
                    use_container_width=True
                )

# --- TAB 3: DETEKSI PRODUK BARU ---
with tab3:
    st.header("Deteksi Produk Baru Antar Minggu")
    weeks = sorted(df_gabungan['Minggu'].unique())
    
    if len(weeks) < 2:
        st.info("Membutuhkan data dari minimal 2 minggu untuk melakukan perbandingan.")
    else:
        col_before, col_after = st.columns(2)
        week_before = col_before.selectbox("Pilih Minggu Pembanding", options=weeks, index=len(weeks)-2)
        week_after = col_after.selectbox("Pilih Minggu Penentu", options=weeks, index=len(weeks)-1)

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            all_stores = sorted(df_filtered['Toko'].unique())
            for store in all_stores:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_gabungan[(df_gabungan['Toko'] == store) & (df_gabungan['Minggu'] == week_before) & (df_gabungan['Status'] == 'Tersedia')]['Nama Produk'])
                    products_after = set(df_gabungan[(df_gabungan['Toko'] == store) & (df_gabungan['Minggu'] == week_after) & (df_gabungan['Status'] == 'Tersedia')]['Nama Produk'])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_filtered[df_filtered['Nama Produk'].isin(new_products) & (df_filtered['Toko'] == store)].copy()
                        new_products_df['Harga_fmt'] = new_products_df['HARGA'].apply(lambda x: f"Rp {x:,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'Harga_fmt', 'BRAND']], use_container_width=True)
