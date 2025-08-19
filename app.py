# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI FINAL
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets individual)
#  Peningkatan: Stabilitas, Fitur Interaktif, dan Ekspor Data
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

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...", ttl=86400)
def load_data_from_gsheets():
    # ... (sisa kode fungsi load_data_from_gsheets tetap sama)
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
        st.warning("Pastikan 10 baris 'Secrets' sudah benar dan Google Sheet sudah di-share."); return None, None, None

    rekap_list_df, database_df = [], pd.DataFrame()
    sheet_names = [
        "DATABASE", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
        "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
        "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS",
        "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS"
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
        st.error(f"GAGAL: Sheet '{e.args[0]}' tidak ditemukan. Periksa daftar 'sheet_names' di dalam kode."); return None, None, None
    except Exception as e:
        st.error(f"Gagal memproses sheet: {e}. Periksa format data di Google Sheets."); return None, None, None

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang dimuat."); return None, None, None

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
        return None, None, None

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

@st.cache_data(show_spinner=False)
def get_smart_matches(query_name, competitor_product_list, score_cutoff=90):
    candidates = process.extract(query_name, competitor_product_list, limit=20, scorer=fuzz.token_set_ratio)
    return [match for match in candidates if match[1] >= score_cutoff][:5]

def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"

# ===================================================================================
# FUNGSI UNTUK EKSPOR DATA
# ===================================================================================
@st.cache_data
def convert_df_for_download(df):
    return df.to_csv(index=False).encode('utf-8')

@st.cache_data
def convert_df_to_json(df):
    return df.to_json(orient='records', indent=4).encode('utf-8')

# ===================================================================================
# INTERFACE DASHBOARD UTAMA
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
st.sidebar.header("Kontrol Analisis")

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if st.sidebar.button("Tarik Data & Mulai Analisis 🚀", key="load_data_button"):
    df, db_df, my_store_name = load_data_from_gsheets()
    if df is not None and not df.empty:
        st.session_state.df = df
        st.session_state.db_df = db_df
        st.session_state.my_store_name = my_store_name
        st.session_state.data_loaded = True
        st.rerun()
    else:
        st.session_state.data_loaded = False
        st.sidebar.error("Gagal memuat data. Periksa pesan error di atas.")

if st.sidebar.button("Hapus Cache & Tarik Ulang 🔄", key="clear_cache_button"):
    st.cache_data.clear()
    st.success("Cache berhasil dihapus. Klik 'Tarik Data' untuk memuat ulang.")
    st.rerun()

if not st.session_state.data_loaded:
    st.info("👈 Klik tombol di sidebar untuk menarik data dan memulai analisis.")
    st.stop()

df = st.session_state.df
db_df = st.session_state.db_df
my_store_name_from_db = st.session_state.my_store_name

# ===================================================================================
# SIDEBAR: FILTER & PENGATURAN
# ===================================================================================
st.sidebar.header("Filter & Pengaturan")
all_stores = sorted(df['Toko'].unique())
main_store = st.sidebar.selectbox("Pilih Toko Utama:", all_stores, index=all_stores.index(my_store_name_from_db) if my_store_name_from_db in all_stores else 0)
min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()

selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
if len(selected_date_range) != 2:
    st.warning("Silakan pilih rentang tanggal yang valid."); st.stop()
start_date, end_date = selected_date_range

accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)

# ===================================================================================
# PEMROSESAN DATA SETELAH FILTER
# ===================================================================================
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()

df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
main_store_df = df_filtered[df_filtered['Toko'] == main_store].copy()
competitor_df = df_filtered[df_filtered['Toko'] != main_store].copy()

# ===================================================================================
# SIDEBAR: FITUR EKSPOR DATA
# ===================================================================================
st.sidebar.divider()
st.sidebar.header("Ekspor Data Hasil Filter")
st.sidebar.info(f"Data yang akan diekspor berisi {len(df_filtered)} baris, dari tanggal {start_date.strftime('%d/%m/%Y')} hingga {end_date.strftime('%d/%m/%Y')}.")

csv_data = convert_df_for_download(df_filtered)
json_data = convert_df_to_json(df_filtered)

st.sidebar.download_button(
    label="📥 Unduh sebagai CSV",
    data=csv_data,
    file_name=f'analisis_data_{start_date}_{end_date}.csv',
    mime='text/csv',
)
st.sidebar.download_button(
    label="📥 Unduh sebagai JSON",
    data=json_data,
    file_name=f'analisis_data_{start_date}_{end_date}.json',
    mime='application/json',
)

# ===================================================================================
# TABS UTAMA
# ===================================================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([f"⭐ Analisis Toko Saya ({main_store})", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

# ===================================================================================
# TAB 1: ANALISIS TOKO SAYA
# ===================================================================================
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
                if pd.notna(row['Nama Produk']):
                    match, score = process.extractOne(row['Nama Produk'], db_map.index, scorer=fuzz.token_set_ratio)
                    if score >= 95:
                        _rekap_df.loc[index, 'Kategori'] = db_map[match]
            return _rekap_df
        
        main_store_df_cat = fuzzy_merge_categories(main_store_df.copy(), db_df)
        category_sales = main_store_df_cat.groupby('Kategori')['Terjual per Bulan'].sum().reset_index()
        
        if not category_sales.empty:
            col1, col2 = st.columns([1,2])
            sort_order_cat = col1.radio("Urutkan:", ["Terlaris", "Kurang Laris"], horizontal=True, key="cat_sort")
            
            # --- PERBAIKAN ERROR ---
            max_cat = len(category_sales)
            default_cat_top = min(10, max_cat)
            top_n_cat = col2.number_input("Tampilkan Top:", 1, max_cat, default_cat_top, key="cat_top_n")
            # --- AKHIR PERBAIKAN ---
            
            cat_sales_sorted = category_sales.sort_values('Terjual per Bulan', ascending=(sort_order_cat == "Kurang Laris")).head(top_n_cat)
            fig_cat = px.bar(cat_sales_sorted, x='Kategori', y='Terjual per Bulan', title=f'Top {top_n_cat} Kategori', text_auto=True)
            st.plotly_chart(fig_cat, use_container_width=True)

            st.markdown("---")
            st.subheader("Detail Produk per Kategori")
            top_categories_list = cat_sales_sorted['Kategori'].tolist()

            if not top_categories_list:
                st.warning("Tidak ada kategori untuk ditampilkan.")
            else:
                selected_category = st.selectbox("Pilih kategori untuk melihat detail produk:", top_categories_list, key="category_select_detail")
                
                if selected_category:
                    products_in_category_df = main_store_df_cat[main_store_df_cat['Kategori'] == selected_category].copy()
                    products_in_category_df = products_in_category_df.sort_values('Terjual per Bulan', ascending=False)
                    
                    products_in_category_df['Harga'] = products_in_category_df['Harga'].apply(lambda x: f"Rp {x:,.0f}")
                    products_in_category_df['Omzet'] = products_in_category_df['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
                    
                    st.dataframe(
                        products_in_category_df[['Nama Produk', 'Harga', 'Terjual per Bulan', 'Omzet']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Terjual per Bulan": st.column_config.ProgressColumn(
                                "Terjual per Bulan",
                                format="%f",
                                min_value=0,
                                max_value=int(max(1, products_in_category_df['Terjual per Bulan'].max())),
                            ),
                        }
                    )
        else:
            st.info("Tidak ada data kategori penjualan untuk ditampilkan pada rentang ini.")
    
    st.subheader("2. Produk Terlaris")
    top_products = main_store_df.sort_values('Terjual per Bulan', ascending=False).head(15)[['Nama Produk', 'Terjual per Bulan', 'Omzet']]
    top_products['Omzet'] = top_products['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
    st.dataframe(top_products, use_container_width=True, hide_index=True)

    st.subheader("3. Distribusi Omzet Brand")
    brand_omzet_main = main_store_df.groupby('Brand')['Omzet'].sum().reset_index()

    if not brand_omzet_main.empty:
        c_sort, c_top_n = st.columns(2)
        sort_order_main = c_sort.radio("Urutkan Omzet Brand:", ["Terbesar", "Terkecil"], horizontal=True, key="main_brand_sort")
        
        # --- PERBAIKAN ERROR ---
        max_brands = len(brand_omzet_main)
        default_top_n = min(6, max_brands)
        top_n_main = c_top_n.number_input("Tampilkan Top Brand:", 1, max_brands, default_top_n, key="main_brand_top_n")
        # --- AKHIR PERBAIKAN ---

        is_ascending_main = sort_order_main == "Terkecil"
        chart_data_main = brand_omzet_main.sort_values('Omzet', ascending=is_ascending_main).head(top_n_main)

        col1, col2 = st.columns(2)
        with col1:
            fig_brand_pie = px.pie(chart_data_main, names='Brand', values='Omzet', title=f'Distribusi Omzet Top {top_n_main} Brand')
            fig_brand_pie.update_traces(texttemplate='%{label}<br>%{percent}<br>Rp %{value:,.0f}')
            st.plotly_chart(fig_brand_pie, use_container_width=True)
        with col2:
            fig_brand_bar = px.bar(chart_data_main, x='Brand', y='Omzet', title=f"Detail Omzet Top {top_n_main} Brand", 
                                   text_auto='.2s', labels={'Omzet': 'Total Omzet (Rp)'})
            fig_brand_bar.update_layout(yaxis_title="Total Omzet (Rp)")
            st.plotly_chart(fig_brand_bar, use_container_width=True)
    else:
        st.info("Tidak ada data omzet brand untuk ditampilkan pada rentang ini.")

# ===================================================================================
# TAB 2: PERBANDINGAN HARGA
# ===================================================================================
with tab2:
    st.header(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
    
    st.subheader("1. Ringkasan Kinerja Mingguan (WoW Growth)")
    weekly_summary = main_store_df.groupby('Minggu').agg(Omzet=('Omzet', 'sum'), Penjualan_Unit=('Terjual per Bulan', 'sum')).reset_index()
    weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Omzet'].pct_change().apply(format_wow_growth)
    weekly_summary['Omzet'] = weekly_summary['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
    st.dataframe(weekly_summary, use_container_width=True, hide_index=True)

    st.subheader("2. Detail Produk di Toko Anda (Data Terbaru)")
    if not main_store_df.empty:
        latest_date = main_store_df['Tanggal'].max()
        main_store_latest = main_store_df[main_store_df['Tanggal'] == latest_date].copy()
        
        main_store_latest['Harga'] = main_store_latest['Harga'].apply(lambda x: f"Rp {x:,.0f}")
        cols_to_show = ['Tanggal', 'Nama Produk', 'Harga', 'Status', 'Stok']
        st.dataframe(main_store_latest[cols_to_show], use_container_width=True, hide_index=True)
        
        st.subheader("3. Pilih Produk untuk Dibandingkan")
        search_query = st.text_input("Cari produk berdasarkan nama, brand, atau kata kunci:", key="search_product")
        product_list = sorted(main_store_latest['Nama Produk'].unique())
        if search_query:
            product_list = [p for p in product_list if search_query.lower() in p.lower()]

        if not product_list:
            st.warning("Tidak ada produk yang cocok dengan pencarian Anda.")
        else:
            selected_product = st.selectbox("Pilih produk dari hasil pencarian:", product_list)
            if selected_product:
                product_info = main_store_latest[main_store_latest['Nama Produk'] == selected_product].iloc[0]
                
                st.markdown(f"**Produk Pilihan Anda:** *{product_info['Nama Produk']}*")
                col1, col2, col3 = st.columns(3)
                col1.metric(f"Harga di {main_store}", product_info['Harga'])
                col2.metric(f"Status", product_info['Status'])
                col3.metric(f"Stok", product_info['Stok'])
                
                st.markdown("---")
                st.markdown(f"**Perbandingan di Toko Kompetitor:**")
                competitor_latest = competitor_df[competitor_df['Tanggal'] == latest_date]
                if not competitor_latest.empty:
                    competitor_products_tuple = tuple(competitor_latest['Nama Produk'].tolist())
                    matches = get_smart_matches(product_info['Nama Produk'], competitor_products_tuple, score_cutoff=accuracy_cutoff)
                    
                    if not matches:
                        st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                    else:
                        for product, score in matches:
                            match_info = competitor_latest[competitor_latest['Nama Produk'] == product].iloc[0]
                            price_diff = int(re.sub(r'[^\d]', '', str(match_info['Harga']))) - int(re.sub(r'[^\d]', '', str(product_info['Harga'])))
                            
                            st.markdown(f"**Toko: {match_info['Toko']}** (Kemiripan: {int(score)}%)")
                            st.markdown(f"*{match_info['Nama Produk']}*")
                            c1, c2, c3 = st.columns(3)
                            c1.metric("Harga Kompetitor", f"Rp {match_info['Harga']:,.0f}", delta=f"Rp {price_diff:,.0f}")
                            c2.metric("Status", match_info['Status'])
                            c3.metric("Stok", match_info['Stok'])

# ===================================================================================
# TAB 3: ANALISIS BRAND KOMPETITOR
# ===================================================================================
with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    if competitor_df.empty:
        st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
    else:
        competitor_list = sorted(competitor_df['Toko'].unique())
        for competitor_store in competitor_list:
            st.subheader(f"Analisis untuk Kompetitor: {competitor_store}")
            single_competitor_df = competitor_df[competitor_df['Toko'] == competitor_store]

            brand_analysis = single_competitor_df.groupby('Brand').agg(
                Total_Omzet=('Omzet', 'sum'),
                Total_Unit_Terjual=('Terjual per Bulan', 'sum')
            ).reset_index().sort_values("Total_Omzet", ascending=False)
            
            if not brand_analysis.empty:
                st.write("**Pengaturan Visualisasi Brand**")
                c_sort, c_top_n = st.columns(2)
                sort_order_comp = c_sort.radio("Urutkan:", ["Terbesar", "Terkecil"], horizontal=True, key=f"comp_sort_{competitor_store}")
                
                # --- PERBAIKAN ERROR ---
                max_comp_brands = len(brand_analysis)
                default_comp_top_n = min(6, max_comp_brands)
                top_n_comp = c_top_n.number_input("Jumlah item:", 1, max_comp_brands, default_comp_top_n, key=f"comp_top_n_{competitor_store}")
                # --- AKHIR PERBAIKAN ---

                is_ascending_comp = sort_order_comp == "Terkecil"
                chart_data = brand_analysis.sort_values("Total_Omzet", ascending=is_ascending_comp).head(top_n_comp)

                col1, col2 = st.columns([3,2])
                with col1:
                    st.markdown("**Peringkat Brand (Semua)**")
                    brand_analysis['Total_Omzet_Formatted'] = brand_analysis['Total_Omzet'].apply(lambda x: f"Rp {x:,.0f}")
                    st.dataframe(brand_analysis[['Brand', 'Total_Unit_Terjual', 'Total_Omzet_Formatted']].rename(columns={'Total_Omzet_Formatted': 'Total Omzet'}), use_container_width=True, hide_index=True)
                with col2:
                    st.markdown(f"**Visualisasi Top {top_n_comp} Brand**")
                    fig_pie_comp = px.pie(chart_data, names='Brand', values='Total_Omzet', title=f'Distribusi Omzet')
                    fig_pie_comp.update_traces(textinfo='percent+label', hovertemplate='<b>%{label}</b><br>Omzet: Rp %{value:,.0f}<br>Persentase: %{percent}')
                    st.plotly_chart(fig_pie_comp, use_container_width=True)

                    fig_bar_comp = px.bar(chart_data, x='Brand', y='Total_Omzet', title=f"Detail Omzet", text_auto='.2s', labels={'Total_Omzet': 'Total Omzet (Rp)'})
                    st.plotly_chart(fig_bar_comp, use_container_width=True)
            
            st.markdown("**Analisis Mendalam per Brand**")
            brand_options = sorted([str(b) for b in single_competitor_df['Brand'].dropna().unique()])
            if brand_options:
                inspect_brand = st.selectbox("Pilih Brand untuk dilihat:", brand_options, key=f"select_brand_{competitor_store}")
                brand_detail = single_competitor_df[single_competitor_df['Brand'] == inspect_brand].sort_values("Terjual per Bulan", ascending=False)
                brand_detail['Harga'] = brand_detail['Harga'].apply(lambda x: f"Rp {x:,.0f}")
                brand_detail['Omzet'] = brand_detail['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
                st.dataframe(brand_detail[['Nama Produk', 'Terjual per Bulan', 'Harga', 'Omzet']], use_container_width=True, hide_index=True)
            st.divider()

# ... (sisa kode untuk Tab 4, 5, dan 6 sama persis)
with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
    stock_trends = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
    
    # Memastikan kolom 'Tersedia' dan 'Habis' ada
    if 'Tersedia' not in stock_trends.columns:
        stock_trends['Tersedia'] = 0
    if 'Habis' not in stock_trends.columns:
        stock_trends['Habis'] = 0
        
    stock_trends_melted = stock_trends.melt(id_vars=['Minggu', 'Toko'], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
    
    fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
    st.plotly_chart(fig_stock_trends, use_container_width=True)
    st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)

with tab5:
    st.header("Analisis Kinerja Penjualan (Semua Toko)")
    
    st.subheader("1. Grafik Omzet Mingguan")
    weekly_omzet = df_filtered.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
    fig_weekly_omzet = px.line(weekly_omzet, x='Minggu', y='Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
    st.plotly_chart(fig_weekly_omzet, use_container_width=True)

    st.subheader("2. Tabel Ringkasan Kinerja Mingguan per Toko")
    for store in all_stores:
        st.markdown(f"**Ringkasan untuk: {store}**")
        store_df = df_filtered[df_filtered['Toko'] == store]
        weekly_summary = store_df.groupby('Minggu').agg(
            total_omzet=('Omzet', 'sum'),
            total_terjual=('Terjual per Bulan', 'sum'),
            avg_harga=('Harga', 'mean')
        ).reset_index()
        
        if not weekly_summary.empty:
            weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['total_omzet'].pct_change().apply(format_wow_growth)
            weekly_summary['Rata-Rata Terjual Harian'] = round(weekly_summary['total_terjual'] / 30)
            weekly_summary['total_omzet_rp'] = weekly_summary['total_omzet'].apply(lambda x: f"Rp {x:,.0f}")
            weekly_summary['avg_harga_rp'] = weekly_summary['avg_harga'].apply(lambda x: f"Rp {x:,.0f}")

            st.dataframe(weekly_summary[['Minggu', 'total_omzet_rp', 'Pertumbuhan Omzet (WoW)', 'total_terjual', 'Rata-Rata Terjual Harian', 'avg_harga_rp']].rename(
                columns={'Minggu': 'Mulai Minggu', 'total_omzet_rp': 'Total Omzet', 'total_terjual': 'Total Terjual', 'avg_harga_rp': 'Rata-Rata Harga'}
            ), use_container_width=True, hide_index=True)
        else:
            st.info(f"Tidak ada data untuk {store} pada rentang ini.")
        st.divider()

with tab6:
    st.header("Analisis Produk Baru Mingguan")
    
    st.subheader("Perbandingan Produk Baru Antar Minggu")
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
            for store in all_stores:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_before)]['Nama Produk'])
                    products_after = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)]['Nama Produk'])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_filtered[df_filtered['Nama Produk'].isin(new_products) & (df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)]
                        new_products_df['Harga'] = new_products_df['Harga'].apply(lambda x: f"Rp {x:,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'Harga', 'Stok', 'Terjual per Bulan']], use_container_width=True, hide_index=True)
