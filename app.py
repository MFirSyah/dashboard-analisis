# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI OPTIMASI FUZZY
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets individual)
#  Peningkatan:
#  - Proses Fuzzy Similarity dipisahkan untuk meningkatkan performa.
#  - Hasil Fuzzy di-cache di Google Sheet 'hasil_fuzzy'.
#  - Ditambahkan tombol untuk memperbarui hasil fuzzy secara manual.
#  - Ditambahkan Tab 3-6 untuk analisis lebih mendalam.
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import gspread
from datetime import datetime

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

def get_gspread_client():
    """Menginisialisasi dan mengembalikan koneksi ke Google Sheets."""
    creds_dict = {
        "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"],
        "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    gc = gspread.service_account_from_dict(creds_dict)
    return gc

@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    """
    Fungsi untuk memuat dan memproses data dari semua sheet yang relevan di Google Sheets.
    Juga memuat hasil fuzzy yang sudah dihitung sebelumnya.
    """
    try:
        gc = get_gspread_client()
        spreadsheet = gc.open("DATA_REKAP")

        sheet_names = [
            "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS",
            "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
            "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
            "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
            "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
            "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
            "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
            "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
            "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS",
            "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS"
        ]

        all_data = []
        for sheet_name in sheet_names:
            worksheet = spreadsheet.worksheet(sheet_name)
            df = pd.DataFrame(worksheet.get_all_records())
            df['Toko'] = sheet_name.split(' - ')[0]
            df['Status'] = 'Tersedia' if 'READY' in sheet_name else 'Habis'
            all_data.append(df)

        df_combined = pd.concat(all_data, ignore_index=True)
        
        df_combined.rename(columns={'NAMA': 'Nama Produk', 'HARGA': 'Harga', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal'}, inplace=True)
        df_combined = df_combined[['Nama Produk', 'Harga', 'Terjual per Bulan', 'BRAND', 'Toko', 'Status', 'Tanggal']]
        
        df_combined['Harga'] = pd.to_numeric(df_combined['Harga'], errors='coerce').fillna(0).astype(int)
        df_combined['Terjual per Bulan'] = pd.to_numeric(df_combined['Terjual per Bulan'], errors='coerce').fillna(0).astype(int)
        
        # PERBAIKAN: Menangani nilai kosong di kolom BRAND yang menyebabkan TypeError
        df_combined['BRAND'].fillna('TIDAK DIKETAHUI', inplace=True)
        df_combined['BRAND'] = df_combined['BRAND'].astype(str) # Memastikan tipe data konsisten

        df_combined = df_combined[df_combined['Nama Produk'] != '']
        df_combined['Tanggal'] = pd.to_datetime(df_combined['Tanggal'], errors='coerce')
        df_combined.dropna(subset=['Tanggal'], inplace=True) # Hapus baris dengan tanggal tidak valid

        try:
            worksheet_fuzzy = spreadsheet.worksheet("hasil_fuzzy")
            df_fuzzy = pd.DataFrame(worksheet_fuzzy.get_all_records())
            if not df_fuzzy.empty:
                df_fuzzy['Harga Referensi'] = pd.to_numeric(df_fuzzy['Harga Referensi'], errors='coerce').fillna(0).astype(int)
                df_fuzzy['Harga Pembanding'] = pd.to_numeric(df_fuzzy['Harga Pembanding'], errors='coerce').fillna(0).astype(int)
                df_fuzzy['Skor Similaritas'] = pd.to_numeric(df_fuzzy['Skor Similaritas'], errors='coerce').fillna(0).astype(int)
        except gspread.WorksheetNotFound:
            st.warning("Worksheet 'hasil_fuzzy' tidak ditemukan.")
            df_fuzzy = pd.DataFrame()
            
        return df_combined, df_fuzzy

    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheets: {e}")
        return pd.DataFrame(), pd.DataFrame()

def update_fuzzy_sheet(df_combined):
    """
    Menghitung ulang fuzzy similarity untuk semua produk dan menulis hasilnya ke sheet 'hasil_fuzzy'.
    """
    try:
        with st.spinner("Memulai proses kalkulasi fuzzy similarity. Ini bisa memakan waktu beberapa menit..."):
            products_to_analyze = df_combined[['Nama Produk', 'Toko', 'Harga']].drop_duplicates().reset_index(drop=True)
            fuzzy_results = []
            
            for index, row in products_to_analyze.iterrows():
                ref_product, ref_store, ref_price = row['Nama Produk'], row['Toko'], row['Harga']
                choices_df = products_to_analyze[products_to_analyze['Toko'] != ref_store]
                if choices_df.empty: continue

                similar_products = process.extract(ref_product, choices_df['Nama Produk'], limit=5, scorer=fuzz.token_set_ratio)
                
                for product, score in similar_products:
                    comp_row = choices_df[choices_df['Nama Produk'] == product].iloc[0]
                    fuzzy_results.append({
                        'Produk Referensi': ref_product, 'Toko Referensi': ref_store, 'Harga Referensi': ref_price,
                        'Produk Pembanding': product, 'Toko Pembanding': comp_row['Toko'],
                        'Harga Pembanding': comp_row['Harga'], 'Skor Similaritas': score
                    })
            df_fuzzy_new = pd.DataFrame(fuzzy_results)

        with st.spinner("Menyimpan hasil ke Google Sheets..."):
            gc = get_gspread_client()
            spreadsheet = gc.open("DATA_REKAP")
            try:
                worksheet = spreadsheet.worksheet("hasil_fuzzy")
                spreadsheet.del_worksheet(worksheet)
            except gspread.WorksheetNotFound: pass
            
            worksheet = spreadsheet.add_worksheet(title="hasil_fuzzy", rows="1", cols="1")
            worksheet.update([df_fuzzy_new.columns.values.tolist()] + df_fuzzy_new.values.tolist())
        
        return True, None
    except Exception as e:
        return False, str(e)

# ===================================================================================
# MEMUAT DATA & PRE-PROCESSING
# ===================================================================================
df, df_fuzzy = load_data_from_gsheets()

if not df.empty:
    df['Total Nilai Terjual'] = df['Harga'] * df['Terjual per Bulan']
    df['Minggu'] = df['Tanggal'].dt.isocalendar().week

# ===================================================================================
# TAMPILAN UI STREAMLIT
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
st.markdown("---")

if df.empty:
    st.error("Data tidak dapat dimuat. Mohon periksa koneksi atau konfigurasi Google Sheets.")
else:
    tab_titles = [
        "📈 Analisis Umum", 
        "⚔️ Analisis Kompetitor", 
        "🔬 Analisis Brand", 
        "📦 Ketersediaan Stok", 
        "✨ Deteksi Produk Baru",
        "🏪 Performa Toko"
    ]
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_titles)

    # ================================== TAB 1: ANALISIS UMUM ==================================
    with tab1:
        st.header("Analisis Umum Tren Penjualan (Semua Toko & Brand)")

        st.subheader("🏆 Produk Terlaris Keseluruhan")
        top_products = df.sort_values(by='Terjual per Bulan', ascending=False).head(10)
        st.dataframe(top_products[['Nama Produk', 'BRAND', 'Toko', 'Harga', 'Terjual per Bulan']], use_container_width=True)

        st.subheader("💰 Distribusi Harga Produk Keseluruhan")
        st.info("Grafik di bawah menampilkan distribusi harga untuk 15 brand dengan jumlah produk terbanyak agar tetap mudah dibaca.")
        # Mengambil 15 brand dengan produk terbanyak untuk visualisasi
        top_brands_for_hist = df['BRAND'].value_counts().nlargest(15).index
        df_for_hist = df[df['BRAND'].isin(top_brands_for_hist)]
        fig_hist = px.histogram(df_for_hist, x="Harga", color="BRAND", title="Distribusi Harga Berdasarkan Top 15 Brand")
        st.plotly_chart(fig_hist, use_container_width=True)

        st.subheader("🛒 Penjualan per Toko")
        sales_by_store = df.groupby('Toko')['Terjual per Bulan'].sum().reset_index()
        fig_bar = px.bar(sales_by_store, x='Toko', y='Terjual per Bulan', title="Total Penjualan per Bulan di Setiap Toko")
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # ================================== TAB 2: ANALISIS KOMPETITOR ==================================
    with tab2:
        st.header("Analisis Perbandingan Produk dengan Kompetitor")
        st.info("Fitur ini membandingkan nama produk Anda dengan produk kompetitor menggunakan kemiripan teks. Hasilnya sudah dihitung sebelumnya untuk mempercepat tampilan.")
        st.markdown("---")
        st.subheader("Perbarui Data Perbandingan Produk")
        st.warning("Proses ini akan memakan waktu beberapa menit. Lakukan hanya jika ada pembaruan data produk yang signifikan.")
        if st.button("🚀 Mulai Perbarui Data Fuzzy Similarity"):
            success, error_message = update_fuzzy_sheet(df)
            if success:
                st.success("Data fuzzy similarity berhasil diperbarui! Halaman akan dimuat ulang.")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"Terjadi kesalahan: {error_message}")
        st.markdown("---")

        if df_fuzzy.empty:
            st.error("Data perbandingan (hasil_fuzzy) tidak tersedia. Silakan klik tombol perbarui di atas.")
        else:
            st.subheader("Pilih Produk untuk Dibandingkan")
            product_list = sorted(df_fuzzy['Produk Referensi'].unique())
            product_to_compare = st.selectbox("Pilih produk:", product_list, index=None, placeholder="Ketik untuk mencari nama produk...")
            if product_to_compare:
                ref_info = df_fuzzy[df_fuzzy['Produk Referensi'] == product_to_compare].iloc[0]
                st.write(f"Produk Referensi: **{ref_info['Produk Referensi']}**")
                st.write(f"Toko: **{ref_info['Toko Referensi']}** | Harga: **Rp {ref_info['Harga Referensi']:,.0f}**")
                similar_products_df = df_fuzzy[df_fuzzy['Produk Referensi'] == product_to_compare]
                similarity_threshold = st.slider("Skor similaritas minimum:", 0, 100, 75)
                filtered_results = similar_products_df[similar_products_df['Skor Similaritas'] >= similarity_threshold]
                if not filtered_results.empty:
                    st.dataframe(filtered_results[['Produk Pembanding', 'Toko Pembanding', 'Harga Pembanding', 'Skor Similaritas']].sort_values('Skor Similaritas', ascending=False), use_container_width=True)
                else:
                    st.info("Tidak ditemukan produk pembanding dengan skor di atas ambang batas.")

    # ================================== TAB 3: ANALISIS BRAND ==================================
    with tab3:
        st.header("Analisis Mendalam Berdasarkan Brand")
        stores_for_brand_analysis = st.multiselect("Pilih Toko untuk Analisis Brand:", sorted(df['Toko'].unique()), default=sorted(df['Toko'].unique()), key="tab3_stores")
        
        if stores_for_brand_analysis:
            brand_df = df[df['Toko'].isin(stores_for_brand_analysis)]
            
            # Kalkulasi metrik brand
            brand_metrics = brand_df.groupby('BRAND').agg(
                Jumlah_Produk=('Nama Produk', 'nunique'),
                Total_Penjualan_Unit=('Terjual per Bulan', 'sum'),
                Total_Penjualan_Nilai=('Total Nilai Terjual', 'sum'),
                Harga_Rata_Rata=('Harga', 'mean')
            ).reset_index().sort_values('Total_Penjualan_Nilai', ascending=False)

            st.subheader("📊 Performa Keseluruhan Brand")
            st.dataframe(brand_metrics, use_container_width=True)

            col3a, col3b = st.columns(2)
            with col3a:
                st.subheader("Top 10 Brand Berdasarkan Nilai Penjualan")
                fig_brand_sales = px.bar(brand_metrics.head(10), x='BRAND', y='Total_Penjualan_Nilai', title="Nilai Penjualan (Rp) per Brand")
                st.plotly_chart(fig_brand_sales, use_container_width=True)
            with col3b:
                st.subheader("Top 10 Brand Berdasarkan Jumlah Produk")
                fig_brand_products = px.bar(brand_metrics.sort_values('Jumlah_Produk', ascending=False).head(10), x='BRAND', y='Jumlah_Produk', title="Jumlah Produk Unik per Brand")
                st.plotly_chart(fig_brand_products, use_container_width=True)
        else:
            st.warning("Pilih minimal satu toko untuk menampilkan analisis brand.")

    # ================================== TAB 4: KETERSEDIAAN STOK ==================================
    with tab4:
        st.header("Analisis Ketersediaan Stok Produk")
        col4a, col4b = st.columns(2)
        with col4a:
            store_for_stock = st.selectbox("Pilih Toko:", sorted(df['Toko'].unique()), key="tab4_store")
        
        stock_df = df[df['Toko'] == store_for_stock]
        
        if not stock_df.empty:
            status_counts = stock_df['Status'].value_counts()
            
            with col4b:
                st.subheader(f"Status Stok di {store_for_stock}")
                fig_pie = px.pie(values=status_counts.values, names=status_counts.index, title="Proporsi Produk Tersedia vs Habis", hole=.3)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            st.subheader(f"Daftar Produk yang Habis di Toko {store_for_stock}")
            out_of_stock_df = stock_df[stock_df['Status'] == 'Habis']
            if not out_of_stock_df.empty:
                st.dataframe(out_of_stock_df[['Nama Produk', 'BRAND', 'Terjual per Bulan']], use_container_width=True)
            else:
                st.success(f"Semua produk di toko {store_for_stock} tersedia!")
        else:
            st.info("Pilih toko untuk melihat status stok.")

    # ================================== TAB 5: DETEKSI PRODUK BARU ==================================
    with tab5:
        st.header("Deteksi Kemunculan Produk Baru")
        st.info("Fitur ini membandingkan daftar produk yang tersedia antara dua periode waktu (minggu) untuk menemukan item baru.")
        
        weeks = sorted(df['Minggu'].unique())
        
        col5a, col5b = st.columns(2)
        with col5a:
            week_before = st.selectbox("Pilih Minggu Pembanding:", weeks, index=0)
        with col5b:
            week_after = st.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
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
                        new_products_df = df[df['Nama Produk'].isin(new_products) & (df['Toko'] == store) & (df['Minggu'] == week_after)].copy()
                        new_products_df['Harga_fmt'] = new_products_df['Harga'].apply(lambda x: f"Rp {x:,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'BRAND', 'Harga_fmt']], use_container_width=True)
    
    # ================================== TAB 6: PERFORMA TOKO ==================================
    with tab6:
        st.header("Analisis Performa Antar Toko")

        store_performance = df.groupby('Toko').agg(
            Total_Penjualan_Unit=('Terjual per Bulan', 'sum'),
            Total_Penjualan_Nilai=('Total Nilai Terjual', 'sum'),
            Jumlah_Produk_Unik=('Nama Produk', 'nunique'),
            Harga_Rata_Rata=('Harga', 'mean')
        ).reset_index().sort_values('Total_Penjualan_Nilai', ascending=False)
        
        store_performance['Harga_Rata_Rata'] = store_performance['Harga_Rata_Rata'].astype(int)

        st.subheader("📈 Tabel Perbandingan Kinerja Toko")
        st.dataframe(store_performance, use_container_width=True)

        st.subheader("💰 Visualisasi Perbandingan Nilai Penjualan")
        fig_store_sales = px.bar(store_performance, x='Toko', y='Total_Penjualan_Nilai',
                                 title="Total Nilai Penjualan (Rp) per Toko",
                                 labels={'Toko': 'Toko', 'Total_Penjualan_Nilai': 'Total Nilai Penjualan (Rp)'})
        st.plotly_chart(fig_store_sales, use_container_width=True)

