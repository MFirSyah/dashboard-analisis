# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI LENGKAP (6 TAB)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Peningkatan:
#  - Menggunakan pre-calculated fuzzy matching untuk mempercepat tab analisis.
#  - Data fuzzy similarity disimpan di sheet 'hasil_fuzzy' dan di-trigger oleh tombol.
#  - Mengembalikan struktur 6 Tab fungsional.
#  - Penanganan error secrets yang lebih baik.
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
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
    """Menginisialisasi dan mengembalikan klien gspread dengan penanganan secrets yang aman."""
    # PERBAIKAN: Memeriksa keberadaan semua kunci secrets sebelum digunakan
    # Diubah gcp_private_key menjadi gcp_private_key_raw agar cocok dengan TOML
    required_secrets = [
        "gcp_type", "gcp_project_id", "gcp_private_key_id", "gcp_private_key_raw",
        "gcp_client_email", "gcp_client_id", "gcp_auth_uri", "gcp_token_uri",
        "gcp_auth_provider_x509_cert_url", "gcp_client_x509_cert_url", "gcp_spreadsheet_url"
    ]
    
    missing_secrets = [secret for secret in required_secrets if not st.secrets.has_key(secret)]
    
    if missing_secrets:
        st.error(f"Beberapa kunci secrets tidak ditemukan: {', '.join(missing_secrets)}. Harap periksa pengaturan secrets di Streamlit Cloud Anda.")
        return None

    try:
        creds_dict = {
            "type": st.secrets["gcp_type"],
            "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"],
            # PERBAIKAN: Menggunakan gcp_private_key_raw agar cocok dengan nama di TOML Anda
            "private_key": st.secrets["gcp_private_key_raw"], 
            "client_email": st.secrets["gcp_client_email"],
            "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"],
            "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Gagal mengotorisasi dengan Google. Cek kembali format kredensial Anda. Error: {e}")
        return None


@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    """
    Fungsi untuk memuat dan memproses semua data dari Google Sheets.
    """
    gc = get_gspread_client()
    if gc is None:
        return pd.DataFrame(), pd.DataFrame(), [] # Return empty dataframes if connection fails

    try:
        spreadsheet_url = st.secrets["gcp_spreadsheet_url"]
        sh = gc.open_by_url(spreadsheet_url)
    except Exception as e:
        st.error(f"Gagal membuka spreadsheet. Cek kembali URL di secrets Anda. Error: {e}")
        return pd.DataFrame(), pd.DataFrame(), []

    sheet_names = [
        "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS", "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS", "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS", "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS", "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS"
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
            
            all_data.append(df)
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Sheet '{sheet_name}' tidak ditemukan.")
        except Exception as e:
            st.error(f"Gagal memuat sheet '{sheet_name}': {e}")

    if not all_data:
        st.error("Tidak ada data rekap yang berhasil dimuat.")
        return pd.DataFrame(), pd.DataFrame(), []

    df_gabungan = pd.concat(all_data, ignore_index=True)
    df_gabungan.rename(columns={'NAMA': 'Nama Produk', 'HARGA': 'Harga', 'TERJUAL/BLN': 'Terjual/Bln', 'BRAND': 'Brand'}, inplace=True)
    
    # Pemrosesan data
    df_gabungan['Harga'] = pd.to_numeric(df_gabungan['Harga'], errors='coerce').fillna(0).astype(int)
    df_gabungan['Terjual/Bln'] = pd.to_numeric(df_gabungan['Terjual/Bln'], errors='coerce').fillna(0).astype(int)
    df_gabungan['TANGGAL'] = pd.to_datetime(df_gabungan['TANGGAL'], errors='coerce')
    df_gabungan.dropna(subset=['Nama Produk', 'TANGGAL'], inplace=True)
    df_gabungan = df_gabungan[df_gabungan['Nama Produk'] != '']
    df_gabungan['Minggu'] = df_gabungan['TANGGAL'].dt.strftime('%Y-%U')

    all_brands = sorted(df_gabungan['Brand'].dropna().unique().tolist())

    # Load pre-calculated fuzzy data
    try:
        fuzzy_sheet = sh.worksheet("hasil_fuzzy")
        fuzzy_records = fuzzy_sheet.get_all_records()
        df_fuzzy = pd.DataFrame(fuzzy_records)
        if not df_fuzzy.empty:
            df_fuzzy['Skor'] = pd.to_numeric(df_fuzzy['Skor'], errors='coerce').fillna(0).astype(int)
        else:
            df_fuzzy = pd.DataFrame(columns=['Produk_Utama', 'Produk_Serupa', 'Skor'])
    except gspread.exceptions.WorksheetNotFound:
        st.error("Worksheet 'hasil_fuzzy' tidak ditemukan! Fitur Analisis Fuzzy tidak akan bekerja.")
        df_fuzzy = pd.DataFrame(columns=['Produk_Utama', 'Produk_Serupa', 'Skor'])
    except Exception as e:
        st.warning(f"Gagal memuat data fuzzy: {e}")
        df_fuzzy = pd.DataFrame(columns=['Produk_Utama', 'Produk_Serupa', 'Skor'])

    return df_gabungan, df_fuzzy, all_brands

def calculate_and_update_fuzzy_data(df_main):
    """
    Menghitung fuzzy similarity untuk semua produk dan menyimpannya ke sheet 'hasil_fuzzy'.
    """
    st.info("Memulai proses kalkulasi fuzzy... Ini bisa memakan waktu beberapa menit.")
    products = df_main['Nama Produk'].unique().tolist()
    all_matches = []
    
    progress_bar = st.progress(0, text="Mempersiapkan produk...")
    total_products = len(products)

    for i, product in enumerate(products):
        progress_percentage = (i + 1) / total_products
        progress_bar.progress(progress_percentage, text=f"Memproses produk {i+1}/{total_products}: {product[:40]}...")

        matches = process.extractBests(product, products[i+1:], scorer=fuzz.token_sort_ratio, score_cutoff=75, limit=None)
        if matches:
            for match, score, _ in matches:
                all_matches.append({'Produk_Utama': product, 'Produk_Serupa': match, 'Skor': score})

    progress_bar.progress(1.0, text="Kalkulasi selesai. Menyimpan ke Google Sheets...")
    
    if not all_matches:
        st.warning("Tidak ditemukan kecocokan produk.")
        return

    df_to_upload = pd.DataFrame(all_matches)
    
    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(st.secrets["gcp_spreadsheet_url"])
        worksheet = sh.worksheet("hasil_fuzzy")
        worksheet.clear()
        worksheet.update([df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist(), value_input_option='USER_ENTERED')
        st.success(f"Berhasil! {len(df_to_upload)} data kemiripan produk telah disimpan.")
    except Exception as e:
        st.error(f"Gagal menyimpan data ke Google Sheets: {e}")

# ===================================================================================
# MEMUAT DATA DAN MEMBUAT UI
# ===================================================================================
df_gabungan, df_fuzzy, all_brands = load_data_from_gsheets()

if df_gabungan.empty:
    st.error("Gagal memuat data utama. Dashboard tidak dapat ditampilkan.")
else:
    st.title("📊 Dashboard Analisis Penjualan & Kompetitor Lengkap")

    # Sidebar Filters
    st.sidebar.header("Filter Global")
    selected_brands = st.sidebar.multiselect("Pilih Brand", all_brands, default=None)
    
    df_filtered = df_gabungan.copy()
    if selected_brands:
        df_filtered = df_filtered[df_filtered['Brand'].isin(selected_brands)]

    # Mendefinisikan 6 Tab
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Ringkasan Umum",
        "🔍 Analisis Fuzzy Similarity",
        "📊 Analisis Produk Terlaris",
        "🆕 Deteksi Produk Baru",
        "📉 Deteksi Produk Turun Harga",
        "⚠️ Deteksi Stok Habis"
    ])

    with tab1:
        st.header("Ringkasan Umum")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Produk Unik", f"{df_filtered['Nama Produk'].nunique():,}")
        col2.metric("Jumlah Toko Terdata", f"{df_filtered['Toko'].nunique()}")
        col3.metric("Rata-rata Harga", f"Rp {df_filtered['Harga'].mean():,.0f}")
        
        st.subheader("Distribusi Produk per Toko")
        produk_per_toko = df_filtered['Toko'].value_counts().reset_index()
        fig_pie = px.pie(produk_per_toko, names='Toko', values='count', hole=0.3)
        st.plotly_chart(fig_pie, use_container_width=True)
        
        st.divider()
        st.subheader("Manajemen Data Fuzzy Similarity")
        st.info("Tombol ini akan menghitung ulang semua kemiripan produk. Proses ini bisa memakan waktu lama. Lakukan ini seminggu sekali atau saat ada banyak produk baru.")
        if st.button("🔄 Perbarui dan Hitung Ulang Data Fuzzy"):
            calculate_and_update_fuzzy_data(df_gabungan)
            st.cache_data.clear()
            st.success("Proses selesai! Dashboard akan dimuat ulang.")
            st.rerun()

    with tab2:
        st.header("Analisis Fuzzy Similarity (Cepat & Efisien)")
        st.write("Pilih produk untuk melihat produk serupa dari toko lain. Data diambil dari hasil kalkulasi yang sudah disimpan.")

        all_product_names = sorted(df_gabungan['Nama Produk'].unique())
        selected_product = st.selectbox("Pilih Produk untuk Dibandingkan", all_product_names)

        if selected_product:
            if df_fuzzy.empty:
                st.warning("Data fuzzy belum tersedia. Silakan hitung di tab 'Ringkasan Umum'.")
            else:
                st.markdown(f"### Produk Serupa untuk: **'{selected_product}'**")
                
                results1 = df_fuzzy[df_fuzzy['Produk_Utama'] == selected_product]
                results2 = df_fuzzy[df_fuzzy['Produk_Serupa'] == selected_product].copy()
                results2.rename(columns={'Produk_Serupa': 'Produk_Utama', 'Produk_Utama': 'Produk_Serupa'}, inplace=True)
                similar_df = pd.concat([results1, results2]).drop_duplicates(subset=['Produk_Serupa']).sort_values(by='Skor', ascending=False)

                st.write("**Info Produk Asli:**")
                st.dataframe(df_gabungan[df_gabungan['Nama Produk'] == selected_product][['Toko', 'Harga', 'Status']].style.format({'Harga': 'Rp {:,.0f}'}))

                if similar_df.empty:
                    st.info("Tidak ditemukan produk serupa di atas ambang batas (75%).")
                else:
                    st.write("**Produk Serupa yang Ditemukan:**")
                    for _, row in similar_df.iterrows():
                        info_serupa = df_gabungan[df_gabungan['Nama Produk'] == row['Produk_Serupa']]
                        st.markdown(f"**`{row['Produk_Serupa']}`** (Skor Kemiripan: **{row['Skor']}%**)")
                        st.dataframe(info_serupa[['Toko', 'Harga', 'Status', 'Terjual/Bln']].style.format({'Harga': 'Rp {:,.0f}'}))

    with tab3:
        st.header("Analisis Produk Terlaris")
        top_n = st.slider("Jumlah produk terlaris yang ingin ditampilkan:", 5, 50, 10)
        
        top_products = df_filtered.sort_values(by='Terjual/Bln', ascending=False).head(top_n)
        st.dataframe(top_products[['Nama Produk', 'Brand', 'Toko', 'Harga', 'Terjual/Bln']].style.format({'Harga': 'Rp {:,.0f}'}), use_container_width=True)

    # Logika untuk Tab 4, 5, dan 6
    weeks = sorted(df_filtered['Minggu'].unique(), reverse=True)
    if len(weeks) > 1:
        col_minggu1, col_minggu2 = st.columns(2)
        with col_minggu1:
            week_after = st.selectbox("Pilih Minggu Penentu:", weeks, index=0, key='minggu_penentu')
        with col_minggu2:
            week_before = st.selectbox("Pilih Minggu Pembanding:", weeks, index=1, key='minggu_pembanding')

        is_valid_week = week_before < week_after

        with tab4:
            st.header("🆕 Deteksi Produk Baru")
            if not is_valid_week: st.error("Minggu Penentu harus setelah Minggu Pembanding.")
            else:
                for store in sorted(df_filtered['Toko'].unique()):
                    with st.expander(f"Lihat Produk Baru di: **{store}**"):
                        products_before = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_before)]['Nama Produk'])
                        products_after = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)]['Nama Produk'])
                        new_products = products_after - products_before
                        
                        if not new_products: st.write("Tidak ada produk baru.")
                        else:
                            st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                            new_df = df_filtered[(df_filtered['Nama Produk'].isin(new_products)) & (df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)]
                            st.dataframe(new_df[['Nama Produk', 'Harga']].style.format({'Harga': 'Rp {:,.0f}'}))

        with tab5:
            st.header("📉 Deteksi Produk Turun Harga")
            if not is_valid_week: st.error("Minggu Penentu harus setelah Minggu Pembanding.")
            else:
                df_b = df_filtered[df_filtered['Minggu'] == week_before][['Nama Produk', 'Toko', 'Harga']]
                df_a = df_filtered[df_filtered['Minggu'] == week_after][['Nama Produk', 'Toko', 'Harga']]
                merged = pd.merge(df_b, df_a, on=['Nama Produk', 'Toko'], suffixes=('_lama', '_baru'))
                drops = merged[merged['Harga_baru'] < merged['Harga_lama']]
                
                if drops.empty: st.info("Tidak ada produk turun harga.")
                else:
                    st.write(f"Ditemukan **{len(drops)}** produk turun harga:")
                    drops['Perubahan'] = drops['Harga_baru'] - drops['Harga_lama']
                    st.dataframe(drops[['Toko', 'Nama Produk', 'Harga_lama', 'Harga_baru', 'Perubahan']].style.format({'Harga_lama': 'Rp {:,.0f}', 'Harga_baru': 'Rp {:,.0f}', 'Perubahan': 'Rp {:,.0f}'}))

        with tab6:
            st.header("⚠️ Deteksi Stok Habis")
            if not is_valid_week: st.error("Minggu Penentu harus setelah Minggu Pembanding.")
            else:
                available_before = df_filtered[(df_filtered['Minggu'] == week_before) & (df_filtered['Status'] == 'Tersedia')]
                oos_after = df_filtered[(df_filtered['Minggu'] == week_after) & (df_filtered['Status'] == 'Habis')]
                newly_oos = pd.merge(available_before, oos_after, on=['Nama Produk', 'Toko'], how='inner', suffixes=('_lama', '_baru'))
                
                if newly_oos.empty: st.info("Tidak ada produk yang baru habis stok.")
                else:
                    st.write(f"Ditemukan **{len(newly_oos)}** produk yang baru habis stok:")
                    st.dataframe(newly_oos[['Toko', 'Nama Produk', 'Harga_baru']].rename(columns={'Harga_baru': 'Harga Terakhir'}).style.format({'Harga Terakhir': 'Rp {:,.0f}'}))

    else:
        st.warning("Data tidak cukup untuk perbandingan mingguan. Silakan tunggu hingga ada data untuk minimal 2 minggu.")


