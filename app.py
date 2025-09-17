# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI OPTIMIZED
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets individual)
#  Peningkatan:
#  - Menggunakan pre-calculated fuzzy matching untuk mempercepat analisis.
#  - Data fuzzy similarity disimpan di sheet 'hasil_fuzzy'.
#  - Terdapat tombol untuk memicu kalkulasi ulang data fuzzy secara periodik.
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
from google.oauth2.service_account import Credentials
import warnings

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis Penjualan")
warnings.filterwarnings('ignore', category=FutureWarning)

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

def get_gspread_client():
    """Menginisialisasi dan mengembalikan klien gspread."""
    creds_dict = {
        "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key"].replace('\\n', '\n'),
        "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    """
    Fungsi untuk memuat dan memproses data utama dan data fuzzy dari Google Sheets.
    """
    try:
        gc = get_gspread_client()
        spreadsheet_url = st.secrets["gcp_spreadsheet_url"]
        sh = gc.open_by_url(spreadsheet_url)
    except Exception as e:
        st.error(f"Gagal terhubung ke Google Sheets. Cek kembali konfigurasi `st.secrets`. Error: {e}")
        return pd.DataFrame(), [], pd.DataFrame(), pd.DataFrame()

    # Daftar semua sheet rekap
    sheet_names = [
        "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS", "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS", "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS", "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS", "SURYA MITRA ONLINE - REKAP - RE", "SURYA MITRA ONLINE - REKAP - HA"
    ]
    
    all_data = []
    store_map = {
        "LOGITECH": "Logitech Official", "DB KLIK": "DB Klik", "ABDITAMA": "Abditama", "LEVEL99": "Level99",
        "IT SHOP": "IT Shop", "JAYA PC": "Jaya PC", "MULTIFUNGSI": "Multifungsi", "TECH ISLAND": "Tech Island",
        "GG STORE": "GG Store", "SURYA MITRA ONLINE": "Surya Mitra Online"
    }

    for sheet_name in sheet_names:
        try:
            worksheet = sh.worksheet(sheet_name)
            df = pd.DataFrame(worksheet.get_all_records())
            
            store_key = sheet_name.split(' - ')[0]
            status_key = sheet_name.split(' - ')[-1]

            df['Toko'] = store_map.get(store_key, "Unknown")
            df['Status'] = 'Tersedia' if 'READY' in status_key or 'RE' in status_key else 'Habis'
            
            df.rename(columns={'NAMA': 'Nama Produk', 'HARGA': 'Harga', 'TERJUAL/BLN': 'Terjual/Bln', 'BRAND': 'Brand'}, inplace=True)
            all_data.append(df)
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Sheet '{sheet_name}' tidak ditemukan, akan dilewati.")
        except Exception as e:
            st.error(f"Gagal memuat data dari sheet '{sheet_name}': {e}")

    if not all_data:
        st.error("Tidak ada data yang berhasil dimuat dari sheet rekap.")
        return pd.DataFrame(), [], pd.DataFrame(), pd.DataFrame()

    df_gabungan = pd.concat(all_data, ignore_index=True)
    df_gabungan = df_gabungan[['Toko', 'Nama Produk', 'Harga', 'Terjual/Bln', 'Brand', 'Status']]

    # Pemrosesan data
    df_gabungan['Harga'] = pd.to_numeric(df_gabungan['Harga'], errors='coerce').fillna(0).astype(int)
    df_gabungan['Terjual/Bln'] = pd.to_numeric(df_gabungan['Terjual/Bln'], errors='coerce').fillna(0).astype(int)
    df_gabungan.dropna(subset=['Nama Produk'], inplace=True)
    df_gabungan = df_gabungan[df_gabungan['Nama Produk'] != '']

    # Load Database Produk dan Brand
    try:
        database_worksheet = sh.worksheet("DATABASE")
        database_df = pd.DataFrame(database_worksheet.get_all_records())
        db_brand_worksheet = sh.worksheet("DATABASE_BRAND")
        brand_list = db_brand_worksheet.col_values(1)[1:]
    except Exception as e:
        st.warning(f"Gagal memuat data 'DATABASE' atau 'DATABASE_BRAND': {e}")
        database_df = pd.DataFrame()
        brand_list = []

    # Load pre-calculated fuzzy data
    try:
        fuzzy_sheet = sh.worksheet("hasil_fuzzy")
        fuzzy_records = fuzzy_sheet.get_all_records()
        df_fuzzy = pd.DataFrame(fuzzy_records)
        if not df_fuzzy.empty:
            # PERBAIKAN: Pastikan kolom 'Skor' bertipe numerik untuk pengurutan yang benar.
            df_fuzzy['Skor'] = pd.to_numeric(df_fuzzy['Skor'], errors='coerce')
            df_fuzzy.dropna(subset=['Skor'], inplace=True) # Hapus baris jika konversi gagal
            df_fuzzy['Skor'] = df_fuzzy['Skor'].astype(int)
        else:
            # Inisialisasi DataFrame kosong jika sheet tidak memiliki data
            df_fuzzy = pd.DataFrame(columns=['Produk_Utama', 'Produk_Serupa', 'Skor'])
    except gspread.exceptions.WorksheetNotFound:
        st.error("Worksheet 'hasil_fuzzy' tidak ditemukan! Fitur Analisis Fuzzy tidak akan bekerja. Silakan buat sheet tersebut.")
        df_fuzzy = pd.DataFrame(columns=['Produk_Utama', 'Produk_Serupa', 'Skor'])
    except Exception as e:
        st.warning(f"Gagal memuat data fuzzy. Mungkin sheet kosong. Error: {e}")
        df_fuzzy = pd.DataFrame(columns=['Produk_Utama', 'Produk_Serupa', 'Skor'])

    return df_gabungan, brand_list, database_df, df_fuzzy

def calculate_and_update_fuzzy_data(df_main):
    """
    Menghitung fuzzy similarity untuk semua produk dan menyimpannya ke sheet 'hasil_fuzzy'.
    """
    st.write("Memulai proses kalkulasi fuzzy...")
    products = df_main['Nama Produk'].unique().tolist()
    all_matches = []
    
    progress_bar = st.progress(0, text="Mempersiapkan produk...")
    total_products = len(products)

    for i, product in enumerate(products):
        # Update progress bar
        progress_percentage = (i + 1) / total_products
        progress_bar.progress(progress_percentage, text=f"Memproses produk {i+1}/{total_products}: {product[:30]}...")

        # Mencari kecocokan dengan produk sisa di daftar untuk menghindari duplikasi
        matches = process.extractBests(
            product,
            products[i+1:],
            scorer=fuzz.token_sort_ratio,
            score_cutoff=75,
            limit=None 
        )
        if matches:
            for match, score, _ in matches:
                all_matches.append({'Produk_Utama': product, 'Produk_Serupa': match, 'Skor': score})

    progress_bar.progress(1.0, text="Kalkulasi selesai. Menyimpan ke Google Sheets...")
    
    if not all_matches:
        st.warning("Tidak ditemukan kecocokan produk di atas ambang batas. Tidak ada data untuk ditulis.")
        return

    df_to_upload = pd.DataFrame(all_matches)
    
    try:
        gc = get_gspread_client()
        spreadsheet_url = st.secrets["gcp_spreadsheet_url"]
        sh = gc.open_by_url(spreadsheet_url)
        
        try:
            worksheet = sh.worksheet("hasil_fuzzy")
        except gspread.exceptions.WorksheetNotFound:
            st.error("Worksheet 'hasil_fuzzy' tidak ditemukan. Membuat sheet baru...")
            worksheet = sh.add_worksheet(title="hasil_fuzzy", rows="1", cols="3")
        
        # Hapus data lama dan tulis data baru
        worksheet.clear()
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist(), value_input_option='USER_ENTERED')
        st.success(f"Berhasil! {len(df_to_upload)} data kemiripan produk telah disimpan.")
    except Exception as e:
        st.error(f"Gagal menyimpan data ke Google Sheets: {e}")

# ===================================================================================
# MEMUAT DATA DAN MEMBUAT UI
# ===================================================================================
df_gabungan, brand_list, database_df, df_fuzzy = load_data_from_gsheets()

if df_gabungan.empty:
    st.error("Gagal memuat data utama. Dashboard tidak dapat ditampilkan.")
else:
    # UI Streamlit
    st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
    
    tab1, tab2 = st.tabs(["📈 Dashboard Utama", "🔍 Analisis Kompetitor"])

    with tab1:
        st.header("Ringkasan Data Penjualan")
        
        # KPI Cards
        total_produk_unik = df_gabungan['Nama Produk'].nunique()
        total_toko = df_gabungan['Toko'].nunique()
        rata_harga = df_gabungan['Harga'].mean()
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Produk Unik", f"{total_produk_unik:,}")
        col2.metric("Jumlah Toko Terdata", f"{total_toko}")
        col3.metric("Rata-rata Harga Produk", f"Rp {rata_harga:,.0f}")
        
        st.divider()
        
        # Opsi Filter
        st.sidebar.header("Filter Data")
        selected_brands = st.sidebar.multiselect("Pilih Brand", sorted(df_gabungan['Brand'].unique()), default=None)
        price_range = st.sidebar.slider("Rentang Harga (Rp)", 0, int(df_gabungan['Harga'].max()), (0, int(df_gabungan['Harga'].max())))
        
        # Terapkan filter
        df_filtered = df_gabungan[
            (df_gabungan['Harga'] >= price_range[0]) & (df_gabungan['Harga'] <= price_range[1])
        ]
        if selected_brands:
            df_filtered = df_filtered[df_filtered['Brand'].isin(selected_brands)]

        # Visualisasi
        st.subheader("Distribusi Produk per Toko")
        produk_per_toko = df_filtered['Toko'].value_counts().reset_index()
        produk_per_toko.columns = ['Toko', 'Jumlah Produk']
        fig_pie = px.pie(produk_per_toko, names='Toko', values='Jumlah Produk', hole=0.3)
        st.plotly_chart(fig_pie, use_container_width=True)

        st.subheader("Top 10 Produk Terlaris (Berdasarkan 'Terjual/Bln')")
        top_10_laris = df_filtered.sort_values('Terjual/Bln', ascending=False).head(10)
        st.dataframe(top_10_laris[['Nama Produk', 'Brand', 'Toko', 'Harga', 'Terjual/Bln']].style.format({'Harga': 'Rp {:,.0f}'}), use_container_width=True)

        st.divider()
        # --- Bagian Baru untuk Manajemen Data Fuzzy ---
        st.subheader("Manajemen Data Fuzzy Similarity")
        st.info("Tombol ini akan menghitung ulang semua kemungkinan kemiripan produk dari data terbaru. Proses ini bisa memakan waktu beberapa menit, tergantung jumlah produk. Lakukan ini seminggu sekali atau saat ada banyak produk baru.")
        
        if st.button("🔄 Perbarui dan Hitung Ulang Data Fuzzy"):
            calculate_and_update_fuzzy_data(df_gabungan)
            # Membersihkan cache agar data fuzzy yang baru bisa langsung dimuat
            st.cache_data.clear()
            st.success("Proses selesai! Dashboard akan dimuat ulang untuk menampilkan data terbaru.")
            # PERBAIKAN: Langsung muat ulang halaman untuk refresh data secara instan
            st.rerun()


    with tab2:
        st.header("Analisis Fuzzy Similarity (Cepat & Efisien)")
        st.write("Pilih satu atau beberapa produk untuk melihat produk serupa dari toko lain berdasarkan kemiripan nama. Data diambil dari hasil kalkulasi yang sudah disimpan.")

        all_product_names = sorted(df_gabungan['Nama Produk'].unique())
        selected_products = st.multiselect("Pilih Produk untuk Dibandingkan", all_product_names)

        if selected_products:
            if df_fuzzy.empty:
                st.warning("Data fuzzy belum tersedia atau kosong. Silakan hitung data di tab 'Dashboard Utama' terlebih dahulu.")
            else:
                for product in selected_products:
                    st.markdown(f"--- \n ### Produk Serupa untuk: **'{product}'**")

                    # Mencari kemiripan dua arah
                    results1 = df_fuzzy[df_fuzzy['Produk_Utama'] == product]
                    results2 = df_fuzzy[df_fuzzy['Produk_Serupa'] == product].copy()
                    results2.rename(columns={'Produk_Serupa': 'Produk_Utama', 'Produk_Utama': 'Produk_Serupa'}, inplace=True)
                    
                    similar_products_df = pd.concat([results1, results2]).drop_duplicates(subset=['Produk_Serupa'])
                    similar_products_df = similar_products_df.sort_values(by='Skor', ascending=False)

                    if similar_products_df.empty:
                        st.write("Tidak ditemukan produk serupa di atas ambang batas (75%).")
                        continue

                    # Menampilkan informasi produk asli
                    st.write("**Info Produk Asli:**")
                    info_asli = df_gabungan[df_gabungan['Nama Produk'] == product][['Toko', 'Harga', 'Status', 'Terjual/Bln']].drop_duplicates()
                    st.dataframe(info_asli.style.format({'Harga': 'Rp {:,.0f}'}), use_container_width=True)
                    st.write("---")
                    
                    # Menampilkan produk serupa
                    for _, row in similar_products_df.iterrows():
                        similar_product_name = row['Produk_Serupa']
                        score = row['Skor']

                        st.markdown(f"**Produk Serupa:** `{similar_product_name}` (Skor: **{score}%**)")
                        info_serupa = df_gabungan[df_gabungan['Nama Produk'] == similar_product_name][['Toko', 'Harga', 'Status', 'Terjual/Bln']].drop_duplicates()
                        st.dataframe(info_serupa.style.format({'Harga': 'Rp {:,.0f}'}), use_container_width=True)
                        st.markdown("<br>", unsafe_allow_html=True)

