# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI FINAL
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets individual)
# ===================================================================================

import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import re
import gspread

# --- KONFIGURASI HALAMAN ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# --- FUNGSI-FUNGSI UTAMA ---

@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...", ttl=300)
def load_data_from_gsheets():
    try:
        # --- Metode koneksi paling stabil: merakit dictionary dari secrets individual ---
        creds_dict = {
            "type": st.secrets["gcp_type"],
            "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"],
            "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"],
            "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"],
            "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet_id = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
        spreadsheet = gc.open_by_key(spreadsheet_id)
        
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: {e}")
        st.warning("Pastikan semua 10 baris 'Secrets' sudah diisi dengan benar dan Google Sheet sudah di-share.")
        return pd.DataFrame(), pd.DataFrame(), None

    rekap_list_df, database_df = [], pd.DataFrame()
    try:
        for sheet in spreadsheet.worksheets():
            sheet_title = sheet.title
            if "DATABASE" in sheet_title.upper():
                database_df = pd.DataFrame(sheet.get_all_records())
            elif "REKAP" in sheet_title.upper():
                df_sheet = pd.DataFrame(sheet.get_all_records())
                if df_sheet.empty: continue
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_title, re.IGNORECASE)
                df_sheet['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tidak Dikenal"
                df_sheet['Status'] = 'Tersedia' if "READY" in sheet_title.upper() else 'Habis'
                rekap_list_df.append(df_sheet)
    except Exception as e:
        st.error(f"Gagal memproses sheet: {e}. Periksa format data di Google Sheets.")
        return pd.DataFrame(), pd.DataFrame(), None

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang dimuat.")
        return pd.DataFrame(), pd.DataFrame(), None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    my_store_name = "DB KLIK"
    if not database_df.empty:
        database_df.columns = [str(col).strip().upper() for col in database_df.columns]
    
    rekap_df.columns = [str(col).strip().upper() for col in rekap_df.columns]
    column_mapping = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga'}
    rekap_df.rename(columns=column_mapping, inplace=True)
    
    if 'BRAND' not in rekap_df.columns:
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    else:
        rekap_df['Brand'] = rekap_df['BRAND'].str.upper()
    
    if 'STOK' not in rekap_df.columns: rekap_df['Stok'] = 'N/A'
    
    required_cols = ['Tanggal', 'Nama Produk', 'Harga', 'Terjual per Bulan']
    if not all(col in rekap_df.columns for col in required_cols):
        st.error(f"Kolom krusial hilang. Pastikan sheet REKAP memiliki: {required_cols}")
        return pd.DataFrame(), pd.DataFrame(), my_store_name

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce')
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)
    
    for col in ['Harga', 'Terjual per Bulan']: rekap_df[col] = rekap_df[col].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True)
    return rekap_df.sort_values('Tanggal'), database_df, my_store_name

# --- INTERFACE DASHBOARD UTAMA---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
st.sidebar.header("Kontrol Analisis")

if st.sidebar.button("Tarik Data & Mulai Analisis 🚀"):
    df, db_df, my_store_name_from_db = load_data_from_gsheets()
    if df.empty:
        st.stop()
        
    st.sidebar.header("Filter & Pengaturan")
    all_stores = sorted(df['Toko'].unique())
    main_store = st.sidebar.selectbox("Pilih Toko Utama:", all_stores, index=all_stores.index(my_store_name_from_db) if my_store_name_from_db in all_stores else 0)
    min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
    start_date, end_date = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
    accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)

    df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
    if df_filtered.empty:
        st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()
    
    main_store_df = df_filtered[df_filtered['Toko'] == main_store].copy()
    competitor_df = df_filtered[df_filtered['Toko'] != main_store].copy()
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([f"⭐ Analisis Toko Saya ({main_store})", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan"])
    
    # --- TAB 1: ANALISIS TOKO SAYA ---
    with tab1:
        st.header(f"Analisis Kinerja Toko: {main_store}")
        my_store_rekap_df = df_filtered[df_filtered['Toko'] == main_store].copy()

        st.subheader("1. Kategori Produk Terlaris")
        if not db_df.empty and 'KATEGORI' in db_df.columns:
            @st.cache_data
            def fuzzy_merge_categories(_rekap_df, _database_df):
                _rekap_df['Kategori'] = 'Lainnya'
                db_map = _database_df.set_index('NAMA')['KATEGORI']
                for index, row in _rekap_df.iterrows():
                    match, score = process.extractOne(row['Nama Produk'], db_map.index, scorer=fuzz.token_set_ratio)
                    if score >= 95: _rekap_df.loc[index, 'Kategori'] = db_map[match]
                return _rekap_df
            my_store_rekap_df = fuzzy_merge_categories(my_store_rekap_df, db_df)
            category_sales = my_store_rekap_df.groupby('Kategori')['Terjual per Bulan'].sum().reset_index()
            
            col1, col2 = st.columns([1,2])
            sort_order = col1.radio("Urutkan:", ["Terlaris", "Kurang Laris"], horizontal=True)
            top_n_cat = col2.number_input("Tampilkan Top:", 1, len(category_sales), 10)
            
            is_desc = sort_order == "Terlaris"
            cat_sales_sorted = category_sales.sort_values(by='Terjual per Bulan', ascending=not is_desc).head(top_n_cat)
            fig_cat = px.bar(cat_sales_sorted, 'Kategori', 'Terjual per Bulan', title=f'Top {top_n_cat} Kategori Terlaris', text_auto=True)
            st.plotly_chart(fig_cat, use_container_width=True)
        
        st.subheader("2. Produk Terlaris")
        top_products = my_store_rekap_df.sort_values('Terjual per Bulan', ascending=False).head(15)[['Nama Produk', 'Terjual per Bulan', 'Omzet']]
        top_products['Omzet'] = top_products['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
        st.dataframe(top_products, use_container_width=True, hide_index=True)

        st.subheader("3. Distribusi Penjualan Brand (Top 6)")
        brand_sales = my_store_rekap_df.groupby('Brand')['Terjual per Bulan'].sum().nlargest(6).reset_index()
        fig_brand_pie = px.pie(brand_sales, 'Brand', 'Terjual per Bulan', 'Top 6 Brand Terlaris')
        st.plotly_chart(fig_brand_pie, use_container_width=True)

    # --- TAB 2: PERBANDINGAN HARGA ---
    with tab2:
        st.header(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
        st.subheader("1. Ringkasan Kinerja Mingguan")
        # ... (Implementasi Tabel Kinerja dengan WoW)

        st.subheader("2. Detail Perbandingan Produk")
        latest_date = main_store_df['Tanggal'].max()
        main_store_latest = main_store_df[main_store_df['Tanggal'] == latest_date]
        cols_to_show = ['Nama Produk', 'Harga', 'Status']
        if 'Stok' in main_store_latest.columns and main_store_latest['Stok'].nunique() > 1:
            cols_to_show.append('Stok')
        st.dataframe(main_store_latest[cols_to_show], use_container_width=True, hide_index=True)
        
        st.subheader("3. Pilih Produk untuk Dibandingkan")
        # ... (Implementasi Selectbox Produk)
        
    # --- TAB 3: ANALISIS BRAND KOMPETITOR ---
    with tab3:
        st.header("Analisis Brand di Toko Kompetitor")
        if competitor_df.empty:
            st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
        else:
            st.subheader("1. Peringkat Penjualan Brand Kompetitor")
            brand_sales_comp = competitor_df.groupby(['Toko', 'Brand'])['Terjual per Bulan'].sum().reset_index()
            st.dataframe(brand_sales_comp.sort_values("Terjual per Bulan", ascending=False), use_container_width=True, hide_index=True)

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("2. Distribusi Brand Terlaris (Top 6)")
                top_6_brands = competitor_df.groupby('Brand')['Terjual per Bulan'].sum().nlargest(6).reset_index()
                fig_pie_comp = px.pie(top_6_brands, 'Brand', 'Terjual per Bulan', 'Top 6 Brand di Semua Kompetitor')
                st.plotly_chart(fig_pie_comp, use_container_width=True)
            with col2:
                st.subheader("3. Analisis Mendalam per Brand")
                inspect_brand = st.selectbox("Pilih Brand untuk dilihat:", options=sorted(competitor_df['Brand'].unique()))
                brand_detail_df = competitor_df[competitor_df['Brand'] == inspect_brand].sort_values("Terjual per Bulan", ascending=False)
                st.dataframe(brand_detail_df[['Toko', 'Nama Produk', 'Terjual per Bulan', 'Harga']], use_container_width=True, hide_index=True)
                
    # --- TAB 4: STATUS STOK PRODUK ---
    with tab4:
        st.header("Tren Status Stok Mingguan per Toko")
        df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time)
        stock_trends = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
        
        fig_stock_trends = px.line(stock_trends, x='Minggu', y=['Tersedia', 'Habis'], color='Toko', markers=True,
                                   title='Jumlah Produk Tersedia vs. Habis per Minggu')
        st.plotly_chart(fig_stock_trends, use_container_width=True)
        st.dataframe(stock_trends, use_container_width=True, hide_index=True)

    # --- TAB 5: KINERJA PENJUALAN ---
    with tab5:
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time)

        st.subheader("1. Grafik Omzet Mingguan")
        weekly_omzet = df_filtered.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
        fig_weekly_omzet = px.line(weekly_omzet, 'Minggu', 'Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
        st.plotly_chart(fig_weekly_omzet, use_container_width=True)

        st.subheader("2. Tabel Ringkasan Kinerja Mingguan per Toko")
        for store in all_stores:
            st.markdown(f"**Ringkasan untuk: {store}**")
            store_df = df_filtered[df_filtered['Toko'] == store]
            weekly_summary = store_df.groupby('Minggu').agg(
                total_omzet=('Omzet', 'sum'),
                total_terjual=('Terjual per Bulan', 'sum'),
                avg_harga=('Harga', 'mean'),
                jumlah_hari=('Tanggal', 'nunique')
            ).reset_index()
            
            if not weekly_summary.empty:
                weekly_summary['Rata-Rata Terjual Harian'] = round(weekly_summary['total_terjual'] / weekly_summary['jumlah_hari'])
                weekly_summary.rename(columns={'Minggu': 'Tanggal Mulai Minggu', 'total_omzet': 'Total Omzet', 'total_terjual': 'Total Terjual', 'avg_harga': 'Rata-Rata Harga'}, inplace=True)
                st.dataframe(weekly_summary[['Tanggal Mulai Minggu', 'Total Omzet', 'Total Terjual', 'Rata-Rata Terjual Harian', 'Rata-Rata Harga']], use_container_width=True, hide_index=True)
            else:
                st.info(f"Tidak ada data untuk {store} pada rentang ini.")
else:
    st.info("👈 Klik tombol di sidebar untuk menarik data dan memulai analisis.")
    st.stop()
