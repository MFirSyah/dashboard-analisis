# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI OPTIMAL
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets)
#  Peningkatan: Hasil fuzzy matching disimpan di worksheet terpisah untuk performa.
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import gspread
from google.oauth2.service_account import Credentials
import datetime

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================
@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    """
    Fungsi untuk memuat dan memproses data utama dan data fuzzy dari Google Sheets.
    """
    try:
        # Autentikasi ke Google Sheets menggunakan st.secrets
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        client = gspread.authorize(creds)
        
        # Buka Spreadsheet berdasarkan URL (lebih stabil daripada nama)
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/1GRC_20J_3_yN_i-b8p0sX5g_zA_sXWxj-pB-p5pE/edit?usp=sharing"
        sh = client.open_by_url(spreadsheet_url)

        # Daftar semua sheet yang akan digabungkan
        sheet_names = [
            worksheet.title for worksheet in sh.worksheets() 
            if 'REKAP' in worksheet.title.upper() and 
               'DATABASE' not in worksheet.title.upper() and 
               'KAMUS' not in worksheet.title.upper() and 
               'HASIL_FUZZY' not in worksheet.title.upper()
        ]
        
        all_data = []
        for sheet_name in sheet_names:
            worksheet = sh.worksheet(sheet_name)
            data = worksheet.get_all_records()
            if not data: continue # Lewati sheet kosong
            df_sheet = pd.DataFrame(data)
            
            # Ekstrak nama toko dan status dari nama sheet
            parts = sheet_name.split(' - ')
            df_sheet['Toko'] = parts[0].strip()
            df_sheet['Status'] = 'Tersedia' if any(s in parts[-1].upper() for s in ['READY', 'RE']) else 'Habis'
            all_data.append(df_sheet)

        if not all_data:
            st.error("Tidak ada data REKAP yang ditemukan di Google Sheet.")
            return pd.DataFrame(), pd.DataFrame(), None

        df = pd.concat(all_data, ignore_index=True)

        # Pembersihan dan pra-pemrosesan data
        df.rename(columns={'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'HARGA': 'Harga'}, inplace=True)
        
        # Pastikan kolom-kolom esensial ada
        required_cols = ['Toko', 'Nama Produk', 'Harga', 'Terjual per Bulan', 'Status', 'BRAND']
        for col in required_cols:
            if col not in df.columns:
                df[col] = '' # Tambah kolom kosong jika tidak ada

        df = df[required_cols]
        
        for col in ['Harga', 'Terjual per Bulan']:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0)
        
        df['TANGGAL'] = pd.to_datetime('today')
        df['Minggu'] = df['TANGGAL'].dt.strftime('%Y-%U') # Format Tahun-Minggu ke

        # Muat data fuzzy yang sudah diproses
        try:
            fuzzy_worksheet = sh.worksheet("hasil_fuzzy")
            fuzzy_data = fuzzy_worksheet.get_all_records()
            df_fuzzy = pd.DataFrame(fuzzy_data)
        except gspread.exceptions.WorksheetNotFound:
            st.warning("Worksheet 'hasil_fuzzy' tidak ditemukan. Silakan jalankan pembaruan data produk serupa.")
            df_fuzzy = pd.DataFrame() # Kembalikan DataFrame kosong jika sheet tidak ada

        return df, df_fuzzy, client # Kembalikan juga client untuk operasi tulis

    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheets: {e}")
        st.info("Pastikan format st.secrets Anda benar dan service account memiliki akses Editor ke Google Sheet.")
        return pd.DataFrame(), pd.DataFrame(), None

def update_fuzzy_sheet(df, client):
    """
    Menghitung ulang data fuzzy similarity dan menyimpannya kembali ke Google Sheets.
    """
    try:
        if df.empty:
            st.error("Data utama kosong, tidak dapat memproses fuzzy matching.")
            return False

        # Ambil daftar produk unik yang tersedia sebagai basis perbandingan
        choices = df[df['Status'] == 'Tersedia']['Nama Produk'].dropna().unique()
        
        if len(choices) == 0:
            st.warning("Tidak ada produk tersedia untuk diproses.")
            return False
        
        fuzzy_results = []
        
        # Buat progress bar
        progress_text = "Memproses perbandingan produk. Harap tunggu..."
        progress_bar = st.progress(0, text=progress_text)
        total_products = len(choices)

        # Lakukan perbandingan fuzzy untuk setiap produk
        for i, product in enumerate(choices):
            # Cari 10 produk paling mirip (limit=11 untuk mengabaikan diri sendiri)
            similar_products = process.extract(product, choices, limit=11, scorer=fuzz.token_sort_ratio)
            for similar_product, score in similar_products:
                if product != similar_product: # Abaikan jika produknya sama persis
                    fuzzy_results.append({
                        'Produk_Utama': product,
                        'Produk_Serupa': similar_product,
                        'Skor_Kecocokan': score
                    })
            
            # Update progress bar
            progress_bar.progress((i + 1) / total_products, text=f"{progress_text} ({i+1}/{total_products})")

        progress_bar.empty()
        
        if not fuzzy_results:
            st.warning("Tidak ada hasil perbandingan produk yang ditemukan.")
            return True # Selesai tanpa error

        df_new_fuzzy = pd.DataFrame(fuzzy_results)
        
        # Buka Spreadsheet dan worksheet tujuan
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/1GRC_20J_3_yN_i-b8p0sX5g_zA_sXWxj-pB-p5pE/edit?usp=sharing"
        sh = client.open_by_url(spreadsheet_url)
        
        try:
            worksheet = sh.worksheet("hasil_fuzzy")
        except gspread.exceptions.WorksheetNotFound:
            st.info("Worksheet 'hasil_fuzzy' tidak ditemukan, membuatnya...")
            worksheet = sh.add_worksheet(title="hasil_fuzzy", rows="1", cols="3")

        # Kosongkan sheet sebelum menulis data baru
        worksheet.clear()
        
        # Tulis data baru ke sheet
        worksheet.update([df_new_fuzzy.columns.values.tolist()] + df_new_fuzzy.values.tolist(), value_input_option='USER_ENTERED')
        
        return True
    except Exception as e:
        st.error(f"Terjadi kesalahan saat memperbarui data fuzzy: {e}")
        return False


# ===================================================================================
# APLIKASI STREAMLIT
# ===================================================================================
if __name__ == "__main__":
    st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
    st.markdown("Versi Optimal dengan Pemrosesan Fuzzy Terpisah.")

    df, df_fuzzy, gspread_client = load_data_from_gsheets()

    if not df.empty:
        # === MEMBUAT TAB ===
        tab1, tab2, tab3 = st.tabs(["🔝 Top Produk Terlaris", "🔍 Analisis Produk Serupa", "✨ Deteksi Produk Baru"])

        # === ISI TAB 1: TOP PRODUK TERLARIS ===
        with tab1:
            st.header("🏆 Peringkat Produk Terlaris di Semua Toko")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                toko_options = ['Semua Toko'] + sorted(df['Toko'].unique())
                selected_store = st.selectbox("Pilih Toko:", options=toko_options, key="tab1_store")
            with col2:
                top_n = st.number_input("Jumlah Produk Ditampilkan:", min_value=5, max_value=100, value=10, step=5)
            with col3:
                min_sales = st.number_input("Minimal Penjualan/Bulan:", min_value=0, value=10, step=1)

            # Filter data berdasarkan input pengguna
            df_filtered_tab1 = df[df['Terjual per Bulan'] >= min_sales]
            if selected_store != 'Semua Toko':
                df_filtered_tab1 = df_filtered_tab1[df_filtered_tab1['Toko'] == selected_store]

            # Tampilkan data terlaris
            top_products = df_filtered_tab1.sort_values(by='Terjual per Bulan', ascending=False).head(top_n)
            top_products_display = top_products.copy()
            top_products_display['Harga'] = top_products_display['Harga'].apply(lambda x: f"Rp {x:,.0f}".replace(',', '.'))
            
            st.dataframe(top_products_display[['Toko', 'Nama Produk', 'Harga', 'Terjual per Bulan', 'BRAND']], use_container_width=True)

        # === ISI TAB 2: ANALISIS PRODUK SERUPA (FUZZY) ===
        with tab2:
            st.header("🤝 Perbandingan Produk Serupa")
            st.info("Fitur ini menggunakan data yang telah diproses untuk kecepatan. Klik tombol di bawah jika Anda ingin memperbarui datanya (misal: seminggu sekali).")

            # Tombol untuk memperbarui data fuzzy
            if st.button("🔄 Perbarui Data Produk Serupa (Fuzzy)", key="update_fuzzy_btn"):
                if gspread_client:
                    with st.spinner("Sedang memproses dan menyimpan hasil fuzzy... Ini mungkin memakan waktu beberapa menit."):
                        success = update_fuzzy_sheet(df, gspread_client)
                    if success:
                        st.success("Data produk serupa berhasil diperbarui! Halaman akan dimuat ulang untuk menampilkan data baru.")
                        st.cache_data.clear() # Hapus cache agar data baru termuat
                        st.rerun()
                    else:
                        st.error("Gagal memperbarui data.")
                else:
                    st.error("Koneksi ke Google Sheets tidak tersedia untuk memperbarui data.")


            st.markdown("---")
            
            if df_fuzzy.empty:
                st.warning("Data produk serupa belum tersedia. Silakan klik tombol 'Perbarui Data' di atas untuk memprosesnya.")
            else:
                col1_tab2, col2_tab2 = st.columns([1, 2])
                with col1_tab2:
                    # Ambil daftar produk dari kolom 'Produk_Utama' di df_fuzzy
                    product_list = sorted(df_fuzzy['Produk_Utama'].unique())
                    selected_product = st.selectbox("Pilih Produk untuk Dibandingkan:", product_list, key="tab2_product")
                    
                    min_score = st.slider("Tingkat Kemiripan Minimum (%):", min_value=70, max_value=100, value=85, step=1)

                with col2_tab2:
                    if selected_product:
                        # Ambil hasil dari df_fuzzy (lebih cepat)
                        similar_products_df = df_fuzzy[
                            (df_fuzzy['Produk_Utama'] == selected_product) &
                            (df_fuzzy['Skor_Kecocokan'] >= min_score)
                        ].sort_values(by='Skor_Kecocokan', ascending=False)

                        if similar_products_df.empty:
                            st.write("Tidak ditemukan produk serupa dengan tingkat kemiripan tersebut.")
                        else:
                            # Tampilkan produk referensi
                            st.write(f"**Produk Referensi:**")
                            ref_details = df[df['Nama Produk'] == selected_product]
                            ref_details_display = ref_details.copy()
                            ref_details_display['Harga'] = ref_details_display['Harga'].apply(lambda x: f"Rp {x:,.0f}".replace(',', '.'))
                            st.dataframe(ref_details_display[['Toko', 'Nama Produk', 'Harga', 'Terjual per Bulan', 'Status']], use_container_width=True)

                            # Tampilkan produk serupa
                            st.write(f"**Produk Serupa yang Ditemukan:**")
                            product_names_to_lookup = similar_products_df['Produk_Serupa'].tolist()
                            details_df = df[df['Nama Produk'].isin(product_names_to_lookup)].copy()
                            details_df['Harga'] = details_df['Harga'].apply(lambda x: f"Rp {x:,.0f}".replace(',', '.'))
                            
                            # Gabungkan dengan skor untuk ditampilkan
                            merged_df = pd.merge(details_df, similar_products_df, left_on='Nama Produk', right_on='Produk_Serupa')
                            merged_df_display = merged_df.sort_values(by='Skor_Kecocokan', ascending=False)

                            st.dataframe(merged_df_display[['Toko', 'Nama Produk', 'Harga', 'Terjual per Bulan', 'Status', 'Skor_Kecocokan']], use_container_width=True)

        # === ISI TAB 3: DETEKSI PRODUK BARU ===
        with tab3:
            st.header("🆕 Deteksi Produk Baru")
            st.info("Bandingkan daftar produk antara dua periode (Tahun-Minggu) yang berbeda untuk menemukan produk yang baru muncul.")
            
            weeks = sorted(df['Minggu'].unique(), reverse=True)
            if len(weeks) >= 2:
                col1_tab3, col2_tab3 = st.columns(2)
                with col1_tab3:
                    week_after = st.selectbox("Pilih Periode Baru:", options=weeks, index=0, key="tab3_after")
                with col2_tab3:
                    week_before = st.selectbox("Bandingkan dengan Periode Lama:", options=weeks, index=1, key="tab3_before")

                if week_before >= week_after:
                    st.error("Periode Baru harus setelah Periode Lama.")
                else:
                    all_stores = sorted(df['Toko'].unique())
                    for store in all_stores:
                        with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                            products_before = set(df[(df['Toko'] == store) & (df['Minggu'] == week_before) & (df['Status'] == 'Tersedia')]['Nama Produk'])
                            products_after = set(df[(df['Toko'] == store) & (df['Minggu'] == week_after) & (df['Status'] == 'Tersedia')]['Nama Produk'])
                            new_products = products_after - products_before
                            
                            if not new_products:
                                st.write("Tidak ada produk baru yang terdeteksi.")
                            else:
                                st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                                new_products_df = df[(df['Nama Produk'].isin(new_products)) & (df['Toko'] == store) & (df['Minggu'] == week_after)].copy()
                                new_products_df['Harga'] = new_products_df['Harga'].apply(lambda x: f"Rp {x:,.0f}".replace(',', '.'))
                                st.dataframe(new_products_df[['Nama Produk', 'Harga', 'Terjual per Bulan', 'BRAND']], use_container_width=True)
            else:
                st.warning("Data tidak cukup untuk perbandingan antar minggu. Minimal harus ada data dari 2 minggu yang berbeda.")

    else:
        st.error("Tidak dapat memuat data. Silakan periksa kembali koneksi atau konfigurasi Google Sheets Anda.")
