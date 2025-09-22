# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 4.1 (REVISI)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi ini menerapkan 10 poin perbaikan yang diminta.
# ===================================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import re
from datetime import datetime
import numpy as np

# ===============================
# KONFIGURASI HALAMAN
# ===============================
st.set_page_config(layout="wide", page_title="Dashboard Analisis Penjualan")

# ===============================
# CSS Kustom (Opsional, untuk estetika)
# ===============================
st.markdown("""
<style>
    .stDataFrame {
        width: 100%;
    }
    .stExpander {
        border: 1px solid #e6e6e6;
        border-radius: 0.5rem;
        padding: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)


# ===============================
# FUNGSI-FUNGSI BANTUAN
# ===============================
def format_rupiah(nilai):
    """Format angka menjadi string mata uang Rupiah."""
    if pd.isna(nilai) or not isinstance(nilai, (int, float)):
        return "Rp 0"
    return f"Rp {int(nilai):,}".replace(",", ".")

@st.cache_data(show_spinner="Memuat dan memproses data...")
def load_and_process_data():
    """
    Memuat semua file CSV, membersihkan, dan menggabungkannya menjadi DataFrame.
    """
    # === Daftar Toko dan file-filenya ===
    stores = {
        "DB KLIK": {"READY": "DATA_REKAP.xlsx - DB KLIK - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - DB KLIK - REKAP - HABIS.csv"},
        "ABDITAMA": {"READY": "DATA_REKAP.xlsx - ABDITAMA - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - ABDITAMA - REKAP - HABIS.csv"},
        "GG STORE": {"READY": "DATA_REKAP.xlsx - GG STORE - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - GG STORE - REKAP - HABIS.csv"},
        "IT SHOP": {"READY": "DATA_REKAP.xlsx - IT SHOP - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - IT SHOP - REKAP - HABIS.csv"},
        "JAYA PC": {"READY": "DATA_REKAP.xlsx - JAYA PC - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - JAYA PC - REKAP - HABIS.csv"},
        "LEVEL99": {"READY": "DATA_REKAP.xlsx - LEVEL99 - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - LEVEL99 - REKAP - HABIS.csv"},
        "LOGITECH": {"READY": "DATA_REKAP.xlsx - LOGITECH - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - LOGITECH - REKAP - HABIS.csv"},
        "MULTIFUNGSI": {"READY": "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - HABIS.csv"},
        "SURYA MITRA ONLINE": {"READY": "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - RE.csv", "HABIS": "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - HA.csv"},
        "TECH ISLAND": {"READY": "DATA_REKAP.xlsx - TECH ISLAND - REKAP - READY.csv", "HABIS": "DATA_REKAP.xlsx - TECH ISLAND - REKAP - HABIS.csv"}
    }

    all_data = []
    # Memuat data untuk setiap toko
    for store_name, files in stores.items():
        for status, filename in files.items():
            try:
                df = pd.read_csv(filename)
                df['Toko'] = store_name
                df['Status'] = 'Tersedia' if status == 'READY' else 'Habis'
                all_data.append(df)
            except FileNotFoundError:
                st.warning(f"File tidak ditemukan: {filename}")
                continue
            except Exception as e:
                st.error(f"Gagal memuat {filename}: {e}")
                continue

    if not all_data:
        st.error("Tidak ada data yang berhasil dimuat. Harap periksa file CSV Anda.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_combined = pd.concat(all_data, ignore_index=True)

    # Pra-pemrosesan data
    df_combined['TANGGAL'] = pd.to_datetime(df_combined['TANGGAL'], errors='coerce')
    df_combined.dropna(subset=['TANGGAL'], inplace=True)
    df_combined['HARGA'] = pd.to_numeric(df_combined['HARGA'], errors='coerce').fillna(0)
    df_combined['TERJUAL/BLN'] = pd.to_numeric(df_combined['TERJUAL/BLN'], errors='coerce').fillna(0)
    
    # Ganti nama kolom agar konsisten
    df_combined.rename(columns={'NAMA': 'Nama Produk'}, inplace=True)

    # Hitung Omzet
    df_combined['Omzet'] = df_combined['HARGA'] * df_combined['TERJUAL/BLN']

    # Pisahkan data DB KLIK dan kompetitor
    df_db_klik = df_combined[df_combined['Toko'] == 'DB KLIK'].copy()
    df_all_stores = df_combined[df_combined['Toko'] != 'DB KLIK'].copy()

    # Memuat data matching
    try:
        df_matching = pd.read_csv("DATA_REKAP.xlsx - HASIL_MATCHING.csv")
    except FileNotFoundError:
        st.error("File 'HASIL_MATCHING.csv' tidak ditemukan. Beberapa fitur perbandingan tidak akan berfungsi.")
        df_matching = pd.DataFrame()
    except Exception as e:
        st.error(f"Gagal memuat HASIL_MATCHING.csv: {e}")
        df_matching = pd.DataFrame()

    return df_db_klik, df_all_stores, df_combined, df_matching

# ===============================
# MEMUAT DATA
# ===============================
df_db_klik, df_all_stores, df_combined, df_matching = load_and_process_data()

# ===============================
# JUDUL UTAMA
# ===============================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
st.markdown("---")

# ===============================
# APLIKASI UTAMA
# ===============================
if df_db_klik.empty or df_all_stores.empty:
    st.error("Data tidak cukup untuk menampilkan dashboard. Pastikan file CSV ada dan tidak kosong.")
else:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Kinerja Toko (DB KLIK)",
        "⚔️ Analisis Kompetitor",
        "🏢 Analisis Brand",
        "🆕 Deteksi Produk Baru",
        "💰 Analisis Margin (Simulasi)",
        "🏆 Produk Unggulan"
    ])

    # ==============================================================================
    # TAB 1: KINERJA TOKO (DB KLIK)
    # ==============================================================================
    with tab1:
        st.header("Analisis Kinerja Penjualan Toko DB KLIK")

        # Filter tanggal untuk seluruh tab
        min_date = df_db_klik['TANGGAL'].min().date()
        max_date = df_db_klik['TANGGAL'].max().date()
        
        # Menggunakan date_input dengan rentang tanggal
        selected_date_range = st.date_input(
            "Pilih Rentang Tanggal Analisis",
            (min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key="db_klik_date_range"
        )
        
        if len(selected_date_range) == 2:
            start_date, end_date = selected_date_range
            # Filter DataFrame berdasarkan rentang tanggal yang dipilih
            mask = (df_db_klik['TANGGAL'].dt.date >= start_date) & (df_db_klik['TANGGAL'].dt.date <= end_date)
            df_db_filtered = df_db_klik.loc[mask].copy()

            if df_db_filtered.empty:
                st.warning("Tidak ada data untuk rentang tanggal yang dipilih.")
            else:
                # Metrik Utama
                total_omzet = df_db_filtered['Omzet'].sum()
                total_produk_terjual = df_db_filtered['TERJUAL/BLN'].sum()
                jumlah_produk_unik = df_db_filtered['Nama Produk'].nunique()
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Omzet", format_rupiah(total_omzet))
                col2.metric("Total Unit Terjual/Bulan", f"{int(total_produk_terjual):,}".replace(",", "."))
                col3.metric("Jumlah Produk Unik", f"{jumlah_produk_unik:,}".replace(",", "."))
                st.markdown("---")

                # --- POIN 1: Analisis Kategori Terlaris (Bar Chart + Tabel) ---
                st.subheader("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
                df_kategori_omzet = df_db_filtered.groupby('KATEGORI')['Omzet'].sum().sort_values(ascending=False).reset_index()
                
                fig_kategori = px.bar(
                    df_kategori_omzet,
                    x='KATEGORI', y='Omzet',
                    title="Omzet per Kategori Produk",
                    labels={'KATEGORI': 'Kategori', 'Omzet': 'Total Omzet'},
                    color='Omzet',
                    color_continuous_scale=px.colors.sequential.Viridis,
                    text_auto=True
                )
                fig_kategori.update_layout(xaxis_title="Kategori Produk", yaxis_title="Total Omzet (Rp)")
                fig_kategori.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
                st.plotly_chart(fig_kategori, use_container_width=True)

                with st.expander("Lihat Tabel Data Omzet per Kategori"):
                    df_kategori_omzet_display = df_kategori_omzet.copy()
                    df_kategori_omzet_display['Omzet'] = df_kategori_omzet_display['Omzet'].apply(format_rupiah)
                    st.dataframe(df_kategori_omzet_display, use_container_width=True)
                
                # --- POIN 2: Produk Terlaris per Kategori dengan kolom SKU ---
                with st.expander("2. Lihat Produk Terlaris per Kategori"):
                    # Ambil daftar kategori unik dari data yang difilter
                    kategori_list = sorted(df_db_filtered['KATEGORI'].dropna().unique())
                    selected_kategori = st.selectbox("Pilih Kategori:", kategori_list)
                    
                    if selected_kategori:
                        produk_per_kategori = df_db_filtered[df_db_filtered['KATEGORI'] == selected_kategori]
                        produk_per_kategori = produk_per_kategori.sort_values(by="Omzet", ascending=False)
                        
                        # Buat kolom format Rupiah
                        produk_per_kategori['Harga'] = produk_per_kategori['HARGA'].apply(format_rupiah)
                        produk_per_kategori['Omzet'] = produk_per_kategori['Omzet'].apply(format_rupiah)

                        # Tampilkan dengan urutan kolom baru
                        st.dataframe(produk_per_kategori[[
                            'Nama Produk',
                            'SKU',
                            'Harga',
                            'TERJUAL/BLN',
                            'Omzet'
                        ]], use_container_width=True)

                # --- POIN 3: Distribusi Omzet Brand (Pie Chart) ---
                st.subheader("3. Distribusi Omzet Brand")
                df_brand_omzet = df_db_filtered.groupby('BRAND')['Omzet'].sum().sort_values(ascending=False).reset_index()
                df_brand_omzet = df_brand_omzet.nlargest(15, 'Omzet') # Ambil top 15 brand

                fig_brand = px.pie(
                    df_brand_omzet,
                    names='BRAND',
                    values='Omzet',
                    title="Distribusi Omzet 15 Brand Teratas",
                    hole=0.3
                )
                # Persentase di dalam, hover detail
                fig_brand.update_traces(
                    textposition='inside', 
                    textinfo='percent',
                    hovertemplate='<b>%{label}</b><br>Omzet: %{customdata[0]}<br>Persentase: %{percent:.1%}<extra></extra>'
                )
                # Menambahkan data kustom (omzet yang diformat) untuk hover
                fig_brand.data[0].customdata = np.array([format_rupiah(x) for x in df_brand_omzet['Omzet']]).reshape(-1, 1)

                st.plotly_chart(fig_brand, use_container_width=True)
                
                # Menambahkan tabel nilai asli di luar chart, sesuai permintaan
                with st.expander("Lihat Tabel Data Omzet per Brand"):
                    df_brand_omzet_display = df_brand_omzet.copy()
                    df_brand_omzet_display['Omzet'] = df_brand_omzet_display['Omzet'].apply(format_rupiah)
                    st.dataframe(df_brand_omzet_display, use_container_width=True)

    # ==============================================================================
    # TAB 2: ANALISIS KOMPETITOR
    # ==============================================================================
    with tab2:
        st.header("Analisis Perbandingan dengan Kompetitor")

        # --- POIN 4, 5, 6, 7, 8: Perbandingan Produk DB KLIK dengan Kompetitor ---
        st.subheader("1. Perbandingan Produk 'DB KLIK' dengan Kompetitor")

        # Poin 4: Gunakan data tanggal paling baru dari DB KLIK
        latest_date_db = df_db_klik['TANGGAL'].max()
        df_db_latest = df_db_klik[df_db_klik['TANGGAL'] == latest_date_db].copy()
        st.info(f"Analisis perbandingan menggunakan data DB KLIK terbaru per tanggal: **{latest_date_db.strftime('%d %B %Y')}**")

        col1_filter, col2_filter = st.columns(2)

        # Poin 5: Tambahkan filter berdasarkan BRAND
        with col1_filter:
            brands_on_latest_date = sorted(df_db_latest['BRAND'].dropna().unique())
            selected_brand_comp = st.selectbox('Filter berdasarkan Brand:', ['Semua Brand'] + brands_on_latest_date, key="brand_comp_filter")
        
        # Filter daftar produk berdasarkan brand yang dipilih
        if selected_brand_comp != 'Semua Brand':
            products_to_show = sorted(df_db_latest[df_db_latest['BRAND'] == selected_brand_comp]['Nama Produk'].unique())
        else:
            products_to_show = sorted(df_db_latest['Nama Produk'].unique())

        with col2_filter:
            selected_product = st.selectbox("Pilih produk DB KLIK untuk dibandingkan:", products_to_show)

        if selected_product and not df_matching.empty:
            product_info_db = df_db_latest[df_db_latest['Nama Produk'] == selected_product].iloc[0]

            st.markdown("---")
            st.write(f"#### Informasi Produk: **{selected_product}**")
            
            # --- POIN 6: Informasi baru yang ditampilkan ---
            col1_info, col2_info, col3_info = st.columns(3)

            # 6a: Harga Rata-Rata
            with col1_info:
                all_entries_product = df_db_klik[df_db_klik['Nama Produk'] == selected_product]
                avg_price = all_entries_product['HARGA'].mean()
                st.metric("Harga Rata-Rata (DB KLIK)", format_rupiah(avg_price))

            # 6b: Perbandingan Status
            with col2_info:
                status_terbaru = product_info_db['Status']
                stok_terbaru = product_info_db.get('STOK', 'N/A')
                st.metric("Status Produk Terbaru", status_terbaru, help=f"Stok: {stok_terbaru}")

            # 6c: Toko Dengan Omzet Tertinggi
            with col3_info:
                # Cari produk yang cocok di df_matching
                matches = df_matching[df_matching['Produk Toko Saya'] == selected_product]
                if not matches.empty:
                    competitor_product_names = matches['Produk Kompetitor'].unique()
                    # Filter data semua toko berdasarkan produk kompetitor yang cocok
                    df_competitor_products = df_all_stores[df_all_stores['Nama Produk'].isin(competitor_product_names)]
                    
                    if not df_competitor_products.empty:
                        # Cari toko dengan omzet tertinggi untuk produk-produk tersebut
                        toko_omzet_tertinggi = df_competitor_products.loc[df_competitor_products['Omzet'].idxmax()]
                        nama_toko = toko_omzet_tertinggi['Toko']
                        omzet_toko = toko_omzet_tertinggi['Omzet']
                        st.metric("Kompetitor Omzet Tertinggi", nama_toko, help=f"Omzet: {format_rupiah(omzet_toko)}")
                    else:
                        st.metric("Kompetitor Omzet Tertinggi", "N/A", help="Produk tidak ditemukan di data kompetitor.")
                else:
                    st.metric("Kompetitor Omzet Tertinggi", "N/A", help="Tidak ada data matching untuk produk ini.")
            
            st.markdown("---")

            # --- POIN 8: Tabel perbandingan di Toko Kompetitor ---
            st.write("#### Perbandingan di Toko Kompetitor (Hasil Matching Terakhir)")
            matches_for_table = df_matching[df_matching['Produk Toko Saya'] == selected_product]
            if not matches_for_table.empty:
                comparison_data = []
                # Ambil data harga terakhir dari DB KLIK
                db_price_latest = product_info_db['HARGA']
                
                # Tanggal data kompetitor terakhir
                latest_date_competitor = df_all_stores['TANGGAL'].max()

                for _, row in matches_for_table.iterrows():
                    toko_kompetitor = row['Toko Kompetitor']
                    produk_kompetitor = row['Produk Kompetitor']
                    
                    # Cari harga produk kompetitor pada tanggal terakhir
                    competitor_product_data = df_all_stores[
                        (df_all_stores['Toko'] == toko_kompetitor) & 
                        (df_all_stores['Nama Produk'] == produk_kompetitor) &
                        (df_all_stores['TANGGAL'] == latest_date_competitor)
                    ]
                    
                    harga_kompetitor = competitor_product_data['HARGA'].iloc[0] if not competitor_product_data.empty else 'N/A'
                    
                    comparison_data.append({
                        "Produk Saya": selected_product,
                        "Harga Saya": format_rupiah(db_price_latest),
                        "Produk Kompetitor": produk_kompetitor,
                        "Harga Kompetitor": format_rupiah(harga_kompetitor) if isinstance(harga_kompetitor, (int,float)) else 'N/A',
                        "Toko Kompetitor": toko_kompetitor,
                        "Skor Kemiripan": f"{row['Skor Kemiripan']}%"
                    })
                
                df_comparison_table = pd.DataFrame(comparison_data)
                st.dataframe(df_comparison_table, use_container_width=True)

            else:
                st.info("Tidak ditemukan produk yang cocok di toko kompetitor.")
            
            # --- POIN 7: Visualisasi Tren Harga Historis dihapus ---
            # (Kode untuk chart line historis telah dihapus dari bagian ini)

        st.markdown("---")
        # --- POIN 9: Produk Terlaris Kompetitor dengan kolom SKU ---
        st.subheader("2. Produk Terlaris Kompetitor")
        toko_kompetitor_list = sorted(df_all_stores['Toko'].unique())
        selected_toko = st.selectbox("Pilih Toko Kompetitor:", toko_kompetitor_list)
        
        if selected_toko:
            df_toko_terpilih = df_all_stores[df_all_stores['Toko'] == selected_toko]
            df_toko_terlaris = df_toko_terpilih.sort_values('Omzet', ascending=False).head(20)

            # Tambahkan kolom SKU kosong
            df_toko_terlaris['SKU'] = 'N/A'
            
            # Format kolom
            df_toko_terlaris['Harga'] = df_toko_terlaris['HARGA'].apply(format_rupiah)
            df_toko_terlaris['Omzet'] = df_toko_terlaris['Omzet'].apply(format_rupiah)

            # Tampilkan dengan urutan kolom yang diminta
            st.dataframe(df_toko_terlaris[[
                'Nama Produk',
                'SKU',
                'Harga',
                'TERJUAL/BLN',
                'Omzet'
            ]], use_container_width=True)


    # ==============================================================================
    # TAB 3: ANALISIS BRAND
    # ==============================================================================
    with tab3:
        st.header("Analisis Kekuatan Brand")

        # --- POIN 10: Analisis Brand di Toko Kompetitor dengan format Rupiah ---
        st.subheader("Analisis Brand di Toko Kompetitor")
        
        # Filter tanggal
        min_date_comp = df_all_stores['TANGGAL'].min().date()
        max_date_comp = df_all_stores['TANGGAL'].max().date()

        selected_date_range_comp = st.date_input(
            "Pilih Rentang Tanggal Analisis",
            (min_date_comp, max_date_comp),
            min_value=min_date_comp,
            max_value=max_date_comp,
            key="competitor_date_range"
        )

        if len(selected_date_range_comp) == 2:
            start_date_c, end_date_c = selected_date_range_comp
            mask_c = (df_all_stores['TANGGAL'].dt.date >= start_date_c) & (df_all_stores['TANGGAL'].dt.date <= end_date_c)
            df_all_stores_filtered = df_all_stores.loc[mask_c]

            # Agregasi data
            brand_competitor_analysis = df_all_stores_filtered.groupby(['BRAND', 'Toko']).agg(
                Total_Omzet=('Omzet', 'sum'),
                Jumlah_Produk=('Nama Produk', 'nunique'),
                Rata_Rata_Terjual=('TERJUAL/BLN', 'mean')
            ).reset_index().sort_values(by="Total_Omzet", ascending=False)
            
            # Pilihan Brand
            brand_list = sorted(brand_competitor_analysis['BRAND'].unique())
            selected_brand_analysis = st.multiselect("Pilih Brand untuk dianalisis:", brand_list, default=brand_list[:5])
            
            if selected_brand_analysis:
                display_df = brand_competitor_analysis[brand_competitor_analysis['BRAND'].isin(selected_brand_analysis)]
                
                # Terapkan format Rupiah pada kolom Total_Omzet
                display_df['Total_Omzet'] = display_df['Total_Omzet'].apply(format_rupiah)
                display_df['Rata_Rata_Terjual'] = display_df['Rata_Rata_Terjual'].apply(lambda x: f"{x:,.2f}".replace(",", "."))

                st.dataframe(display_df, use_container_width=True)

                # Visualisasi
                st.write("#### Visualisasi Omzet Brand per Toko Kompetitor")
                fig_brand_comp = px.bar(
                    display_df,
                    x='Toko',
                    y=display_df['Total_Omzet'].replace({'Rp ': '', '\.': ''}, regex=True).astype(int), # Konversi kembali ke angka untuk plot
                    color='BRAND',
                    barmode='group',
                    title='Perbandingan Omzet Brand di Toko Kompetitor',
                    labels={'y': 'Total Omzet (Rp)', 'Toko': 'Toko Kompetitor'}
                )
                st.plotly_chart(fig_brand_comp, use_container_width=True)

    # ==============================================================================
    # TAB 4: DETEKSI PRODUK BARU
    # ==============================================================================
    with tab4:
        st.header("Deteksi Produk Baru di Pasar")
        st.info("Fitur ini membandingkan daftar produk yang tersedia pada dua minggu yang berbeda untuk mendeteksi item baru.")

        df_combined['Minggu'] = df_combined['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
        weeks = sorted(df_combined['Minggu'].unique(), reverse=True)

        col1, col2 = st.columns(2)
        with col1:
            week_after = st.selectbox("Pilih Minggu Penentu (Tanggal Terbaru):", weeks, index=0)
        with col2:
            week_before = st.selectbox("Pilih Minggu Pembanding (Tanggal Sebelumnya):", weeks, index=min(1, len(weeks)-1))

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            all_stores_list = sorted(df_combined['Toko'].unique())
            for store in all_stores_list:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_combined[(df_combined['Toko'] == store) & (df_combined['Minggu'] == week_before) & (df_combined['Status'] == 'Tersedia')]['Nama Produk'])
                    products_after = set(df_combined[(df_combined['Toko'] == store) & (df_combined['Minggu'] == week_after) & (df_combined['Status'] == 'Tersedia')]['Nama Produk'])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_combined[df_combined['Nama Produk'].isin(new_products) & (df_combined['Toko'] == store) & (df_combined['Minggu'] == week_after)].copy()
                        new_products_df['Harga'] = new_products_df['HARGA'].apply(format_rupiah)
                        st.dataframe(new_products_df[['Nama Produk', 'Harga', 'BRAND']], use_container_width=True)

with tab5:
    st.header("Analisis Kinerja Penjualan (Semua Toko)")
    
    all_stores_latest_per_week = latest_entries_weekly.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
    fig_weekly_omzet = px.line(all_stores_latest_per_week, x='Minggu', y='Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko (Berdasarkan Snapshot Terakhir)')
    st.plotly_chart(fig_weekly_omzet, use_container_width=True)
    
    st.subheader("Tabel Rincian Omzet per Tanggal")
    if not df_filtered.empty:
        omzet_pivot = df_filtered.pivot_table(index='Toko', columns='Tanggal', values='Omzet', aggfunc='sum').fillna(0)
        omzet_pivot.columns = [col.strftime('%d %b %Y') for col in omzet_pivot.columns]
        for col in omzet_pivot.columns:
            omzet_pivot[col] = omzet_pivot[col].apply(lambda x: f"Rp {int(x):,}" if x > 0 else "-")
        omzet_pivot.reset_index(inplace=True)
        st.info("Anda bisa scroll tabel ini ke samping untuk melihat tanggal lainnya.")
        st.dataframe(omzet_pivot, use_container_width=True, hide_index=True)
    else:
        st.warning("Tidak ada data untuk ditampilkan dalam tabel.")

with tab6:
    st.header("Analisis Produk Baru Mingguan")
    weeks = sorted(df_filtered['Minggu'].unique())
    if len(weeks) < 2:
        st.info("Butuh setidaknya 2 minggu data untuk melakukan perbandingan produk baru.")
    else:
        col1, col2 = st.columns(2)
        week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0)
        week_after = col2.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            all_stores = sorted(df_filtered['Toko'].unique())
            for store in all_stores:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_before) & (df_filtered['Status'] == 'Tersedia')]['Nama Produk'])
                    products_after = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after) & (df_filtered['Status'] == 'Tersedia')]['Nama Produk'])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_filtered[df_filtered['Nama Produk'].isin(new_products) & (df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)].copy()
                        new_products_df['Harga_fmt'] = new_products_df['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'Harga_fmt', 'Stok', 'Brand']].rename(columns={'Harga_fmt':'Harga'}), use_container_width=True, hide_index=True)

