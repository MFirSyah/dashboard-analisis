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
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
        
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: {e}")
        st.warning("Pastikan 10 baris 'Secrets' sudah benar dan Google Sheet sudah di-share."); return pd.DataFrame(), pd.DataFrame(), None

    rekap_list_df, database_df = [], pd.DataFrame()
    sheet_names = [
        "DATABASE", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
        "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
        "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS"
    ]
    try:
        for sheet_name in sheet_names:
            sheet = spreadsheet.worksheet(sheet_name)
            df_sheet = pd.DataFrame(sheet.get_all_records())
            
            if "DATABASE" in sheet_name.upper():
                database_df = df_sheet
            elif "REKAP" in sheet_name.upper():
                if df_sheet.empty: continue
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                df_sheet['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
                if "TERSEDIA" in sheet_name.upper() or "READY" in sheet_name.upper():
                    df_sheet['Status'] = 'Tersedia'
                else:
                    df_sheet['Status'] = 'Habis'
                rekap_list_df.append(df_sheet)
    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"GAGAL: Sheet '{e.args[0]}' tidak ditemukan. Periksa daftar 'sheet_names' di dalam kode."); return pd.DataFrame(), pd.DataFrame(), None
    except Exception as e:
        st.error(f"Gagal memproses sheet: {e}. Periksa format data di Google Sheets."); return pd.DataFrame(), pd.DataFrame(), None

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang dimuat."); return pd.DataFrame(), pd.DataFrame(), None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    my_store_name = "DB KLIK"
    if not database_df.empty:
        database_df.columns = [str(col).strip().upper() for col in database_df.columns]
    
    rekap_df.columns = [str(col).strip().upper() for col in rekap_df.columns]
    
    final_rename_mapping = {
        'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan',
        'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand',
        'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'
    }
    rekap_df.rename(columns=final_rename_mapping, inplace=True)

    if 'Brand' not in rekap_df.columns:
        if 'Nama Produk' in rekap_df.columns: rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    if 'Stok' not in rekap_df.columns: rekap_df['Stok'] = 'N/A'
    
    required_cols = ['Tanggal', 'Nama Produk', 'Harga', 'Terjual per Bulan']
    if not all(col in rekap_df.columns for col in required_cols):
        st.error(f"Kolom krusial hilang. Pastikan semua sheet REKAP memiliki: {required_cols}")
        return pd.DataFrame(), pd.DataFrame(), my_store_name

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'], errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)
    
    for col in ['Harga', 'Terjual per Bulan']: rekap_df[col] = rekap_df[col].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    
    cols_for_dedup = ['Nama Produk', 'Toko', 'Tanggal']
    existing_cols = [col for col in cols_for_dedup if col in rekap_df.columns]
    if existing_cols:
        rekap_df.drop_duplicates(subset=existing_cols, inplace=True, keep='last')

    return rekap_df.sort_values('Tanggal'), database_df, my_store_name

def get_smart_matches(query_product_info, competitor_df, score_cutoff=90):
    query_name = query_product_info['Nama Produk']
    competitor_product_list = competitor_df['Nama Produk'].tolist()
    candidates = process.extract(query_name, competitor_product_list, limit=20, scorer=fuzz.token_set_ratio)
    return [match for match in candidates if match[1] >= score_cutoff][:5]

def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"

# --- INTERFACE DASHBOARD UTAMA---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
st.sidebar.header("Kontrol Analisis")

if st.sidebar.button("Tarik Data & Mulai Analisis 🚀"):
    df, db_df, my_store_name_from_db = load_data_from_gsheets()
    if df.empty: st.stop()
        
    st.sidebar.header("Filter & Pengaturan")
    all_stores = sorted(df['Toko'].unique())
    main_store = st.sidebar.selectbox("Pilih Toko Utama:", all_stores, index=all_stores.index(my_store_name_from_db) if my_store_name_from_db in all_stores else 0)
    min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
    
    selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(selected_date_range) != 2:
        st.warning("Silakan pilih rentang tanggal yang valid."); st.stop()
    start_date, end_date = selected_date_range
    
    accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)

    df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
    if df_filtered.empty:
        st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()
    
    main_store_df = df_filtered[df_filtered['Toko'] == main_store].copy()
    competitor_df = df_filtered[df_filtered['Toko'] != main_store].copy()
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([f"⭐ Analisis Toko Saya ({main_store})", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan"])
    
    with tab1:
        st.header(f"Analisis Kinerja Toko: {main_store}")
        
        st.subheader("1. Kategori Produk Terlaris")
        if not db_df.empty and 'KATEGORI' in db_df.columns and 'NAMA' in db_df.columns:
            @st.cache_data
            def fuzzy_merge_categories(_rekap_df, _database_df):
                _rekap_df['Kategori'] = 'Lainnya'
                db_unique = _database_df.drop_duplicates(subset=['NAMA'])
                db_map = db_unique.set_index('NAMA')['KATEGORI']
                for index, row in _rekap_df.iterrows():
                    match, score = process.extractOne(row['Nama Produk'], db_map.index, scorer=fuzz.token_set_ratio)
                    if score >= 95:
                        _rekap_df.loc[index, 'Kategori'] = db_map[match]
                return _rekap_df
            
            main_store_df_cat = fuzzy_merge_categories(main_store_df.copy(), db_df)
            category_sales = main_store_df_cat.groupby('Kategori')['Terjual per Bulan'].sum().reset_index()
            
            col1, col2 = st.columns([1,2])
            sort_order_cat = col1.radio("Urutkan:", ["Terlaris", "Kurang Laris"], horizontal=True, key="cat_sort")
            top_n_cat = col2.number_input("Tampilkan Top:", 1, len(category_sales), 10, key="cat_top_n")
            
            cat_sales_sorted = category_sales.sort_values('Terjual per Bulan', ascending=(sort_order_cat == "Kurang Laris")).head(top_n_cat)
            fig_cat = px.bar(cat_sales_sorted, x='Kategori', y='Terjual per Bulan', title=f'Top {top_n_cat} Kategori', text_auto=True)
            st.plotly_chart(fig_cat, use_container_width=True)
        
        st.subheader("2. Produk Terlaris")
        top_products = main_store_df.sort_values('Terjual per Bulan', ascending=False).head(15)[['Nama Produk', 'Terjual per Bulan', 'Omzet']]
        top_products['Omzet'] = top_products['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
        st.dataframe(top_products, use_container_width=True, hide_index=True)

        st.subheader("3. Distribusi Penjualan Brand (Top 6)")
        brand_sales = main_store_df.groupby('Brand')['Terjual per Bulan'].sum().nlargest(6).reset_index()
        fig_brand_pie = px.pie(brand_sales, names='Brand', values='Terjual per Bulan', title='Top 6 Brand Terlaris')
        st.plotly_chart(fig_brand_pie, use_container_width=True)

    with tab2:
        st.header(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
        
        st.subheader("1. Ringkasan Kinerja Mingguan (WoW Growth)")
        main_store_df['Minggu'] = main_store_df['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
        weekly_summary = main_store_df.groupby('Minggu').agg(Omzet=('Omzet', 'sum'), Penjualan_Unit=('Terjual per Bulan', 'sum')).reset_index()
        weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Omzet'].pct_change().apply(format_wow_growth)
        st.dataframe(weekly_summary, use_container_width=True, hide_index=True)

        st.subheader("2. Detail Produk di Toko Anda (Data Terbaru)")
        latest_date = main_store_df['Tanggal'].max()
        main_store_latest = main_store_df[main_store_df['Tanggal'] == latest_date]
        cols_to_show = ['Nama Produk', 'Harga', 'Status', 'Stok']
        st.dataframe(main_store_latest[cols_to_show], use_container_width=True, hide_index=True)
        
        st.subheader("3. Pilih Produk untuk Dibandingkan")
        # --- PERBAIKAN: Menambahkan pemeriksaan data untuk mencegah error ---
        if not main_store_latest.empty:
            selected_product = st.selectbox("Pilih produk:", sorted(main_store_latest['Nama Produk'].unique()))
            if selected_product:
                product_info_df = main_store_latest[main_store_latest['Nama Produk'] == selected_product]
                if not product_info_df.empty:
                    product_info = product_info_df.iloc[0]
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric(f"Harga di {main_store}", f"Rp {product_info['Harga']:,.0f}")
                    col2.metric(f"Status", product_info['Status'])
                    col3.metric(f"Stok", product_info['Stok'])

                    st.markdown("---")
                    st.markdown(f"**Perbandingan di Toko Kompetitor:**")
                    competitor_latest = competitor_df[competitor_df['Tanggal'] == latest_date]
                    if not competitor_latest.empty:
                        matches = get_smart_matches(product_info, competitor_latest, score_cutoff=accuracy_cutoff)
                        if not matches:
                            st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                        else:
                            for product, score in matches:
                                match_info_df = competitor_latest[competitor_latest['Nama Produk'] == product]
                                if not match_info_df.empty:
                                    match_info = match_info_df.iloc[0]
                                    price_diff = match_info['Harga'] - product_info['Harga']
                                    st.markdown(f"**Toko: {match_info['Toko']}** (Kemiripan Nama: {int(score)}%)")
                                    c1, c2, c3 = st.columns(3)
                                    c1.metric("Harga Kompetitor", f"Rp {match_info['Harga']:,.0f}", delta=f"Rp {price_diff:,.0f}", key=f"price_{match_info['Toko']}_{product}")
                                    c2.metric("Status", match_info['Status'], key=f"status_{match_info['Toko']}_{product}")
                                    c3.metric("Stok", match_info['Stok'], key=f"stok_{match_info['Toko']}_{product}")
    
    with tab3:
        st.header("Analisis Brand di Toko Kompetitor")
        if competitor_df.empty:
            st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
        else:
            # --- FITUR BARU: Menambahkan analisis Omzet ---
            st.subheader("1. Peringkat Brand Kompetitor")
            brand_analysis_comp = competitor_df.groupby(['Toko', 'Brand']).agg(
                Penjualan_Unit=('Terjual per Bulan', 'sum'),
                Total_Omzet=('Omzet', 'sum')
            ).reset_index()
            brand_analysis_comp['Total_Omzet_Formatted'] = brand_analysis_comp['Total_Omzet'].apply(lambda x: f"Rp {x:,.0f}")
            st.dataframe(brand_analysis_comp[['Toko', 'Brand', 'Penjualan_Unit', 'Total_Omzet_Formatted']].rename(columns={'Penjualan_Unit': 'Total Unit Terjual', 'Total_Omzet_Formatted': 'Total Omzet'}), use_container_width=True, hide_index=True)

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("2. Distribusi Omzet Brand Terlaris (Top 6)")
                top_6_brands_omzet = competitor_df.groupby('Brand')['Omzet'].sum().nlargest(6).reset_index()
                fig_pie_comp = px.pie(top_6_brands_omzet, names='Brand', values='Omzet', title='Top 6 Brand di Semua Kompetitor (berdasarkan Omzet)')
                st.plotly_chart(fig_pie_comp, use_container_width=True)
            with col2:
                st.subheader("3. Analisis Mendalam per Brand")
                brand_options = sorted([str(b) for b in competitor_df['Brand'].dropna().unique()])
                if brand_options:
                    inspect_brand = st.selectbox("Pilih Brand untuk dilihat:", brand_options)
                    brand_detail = competitor_df[competitor_df['Brand'] == inspect_brand].sort_values("Terjual per Bulan", ascending=False)
                    st.dataframe(brand_detail[['Toko', 'Nama Produk', 'Terjual per Bulan', 'Harga', 'Omzet']], use_container_width=True, hide_index=True)
                else:
                    st.info("Tidak ada brand kompetitor untuk dianalisis.")
    
    with tab4:
        st.header("Tren Status Stok Mingguan per Toko")
        df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
        stock_trends = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
        stock_trends_melted = stock_trends.melt(id_vars=['Minggu', 'Toko'], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
        
        fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
        st.plotly_chart(fig_stock_trends, use_container_width=True)
        st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)

    with tab5:
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date

        st.subheader("1. Grafik Omzet Mingguan")
        weekly_omzet = df_filtered.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
        fig_weekly_omzet = px.line(weekly_omzet, x='Minggu', y='Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
        st.plotly_chart(fig_weekly_omzet, use_container_width=True)

        st.subheader("2. Tabel Ringkasan Kinerja Mingguan per Toko")
        for store in all_stores:
            with st.expander(f"Lihat Detail untuk: **{store}**"):
                store_df = df_filtered[df_filtered['Toko'] == store]
                weekly_summary = store_df.groupby('Minggu').agg(
                    total_omzet=('Omzet', 'sum'),
                    total_terjual=('Terjual per Bulan', 'sum'),
                    avg_harga=('Harga', 'mean'),
                    jumlah_hari=('Tanggal', 'nunique')
                ).reset_index()
                
                if not weekly_summary.empty and weekly_summary['jumlah_hari'].sum() > 0:
                    weekly_summary['Rata-Rata Terjual Harian'] = round(weekly_summary['total_terjual'] / weekly_summary['jumlah_hari'])
                    st.dataframe(weekly_summary.rename(columns={'Minggu': 'Mulai Minggu', 'total_omzet': 'Total Omzet', 'total_terjual': 'Total Terjual', 'avg_harga': 'Rata-Rata Harga'}), use_container_width=True, hide_index=True)
                else:
                    st.info(f"Tidak ada data untuk {store} pada rentang ini.")
else:
    st.info("👈 Klik tombol di sidebar untuk menarik data dan memulai analisis.")
    st.stop()
