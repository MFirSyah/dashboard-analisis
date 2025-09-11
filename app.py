# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI SIMPLIFIKASI
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets individual)
#  Peningkatan: Fitur standardisasi brand via 'kamus_brand' telah dihapus.
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
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    """
    Fungsi untuk memuat dan memproses data dari Google Sheets.
    """
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
        st.warning("Pastikan 10 baris 'Secrets' sudah benar dan Google Sheet sudah di-share.")
        return None, None

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
            all_values = sheet.get_all_values()
            if not all_values or len(all_values) < 2:
                st.warning(f"Sheet '{sheet_name}' kosong, dilewati.")
                continue
            header = all_values[0]
            data = all_values[1:]
            df_sheet = pd.DataFrame(data, columns=header)
            if '' in df_sheet.columns:
                df_sheet = df_sheet.drop(columns=[''])
            
            if "DATABASE" in sheet_name.upper():
                database_df = df_sheet
            elif "REKAP" in sheet_name.upper():
                if df_sheet.empty: continue
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                df_sheet['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
                df_sheet['Status'] = 'Tersedia' if "READY" in sheet_name.upper() else 'Habis'
                rekap_list_df.append(df_sheet)

    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"GAGAL: Sheet '{e.args[0]}' tidak ditemukan."); return None, None
    except Exception as e:
        st.error(f"Gagal memproses sheet: {e}."); return None, None

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang dimuat."); return None, None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    
    for df in [database_df, rekap_df]:
        if not df.empty:
            df.columns = [str(col).strip().upper() for col in df.columns]

    final_rename_mapping = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'}
    rekap_df.rename(columns=final_rename_mapping, inplace=True)
    
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()

    # --- PERUBAHAN: EKSTRAKSI BRAND SEDERHANA ---
    # Fitur yang menggunakan 'kamus_brand' dan logika standardisasi kompleks telah dihapus.
    # Sekarang, brand diekstrak langsung dari kata pertama 'Nama Produk' untuk simplisitas.
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    elif 'Brand' not in rekap_df.columns:
        rekap_df['Brand'] = 'Tidak Diketahui'

    if 'Stok' not in rekap_df.columns: rekap_df['Stok'] = 'N/A'

    required_cols = ['Tanggal', 'Nama Produk', 'Harga', 'Terjual per Bulan']
    if not all(col in rekap_df.columns for col in required_cols):
        st.error(f"Kolom krusial hilang. Pastikan semua sheet REKAP memiliki: {required_cols}")
        return None, None

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)

    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Terjual per Bulan'] = rekap_df['Terjual per Bulan'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True, keep='last')

    return rekap_df.sort_values('Tanggal'), database_df

@st.cache_data(show_spinner=False)
def get_smart_matches(query_name, competitor_product_list, score_cutoff=90):
    candidates = process.extract(query_name, competitor_product_list, limit=20, scorer=fuzz.token_set_ratio)
    return [match for match in candidates if match[1] >= score_cutoff][:5]

def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"

def style_wow_growth(val):
    color = 'black'
    if isinstance(val, str):
        if '▲' in val: color = 'green'
        elif '▼' in val: color = 'red'
    return f'color: {color}'

@st.cache_data
def convert_df_for_download(df):
    return df.to_csv(index=False).encode('utf-8')

@st.cache_data
def convert_df_to_json(df):
    return df.to_json(orient='records', indent=4).encode('utf-8')

st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if not st.session_state.data_loaded:
    _, col_center, _ = st.columns([2, 3, 2])
    with col_center:
        if st.button("Tarik Data & Mulai Analisis 🚀", key="load_data_main_button", type="primary"):
            df, db_df = load_data_from_gsheets()
            if df is not None and not df.empty:
                st.session_state.df = df
                st.session_state.db_df = db_df
                st.session_state.data_loaded = True
                st.rerun()
            else:
                st.session_state.data_loaded = False
                st.error("Gagal memuat data. Periksa pesan error di atas.")
    st.info("👆 Klik tombol di atas untuk menarik data dan memulai analisis.")
    st.stop()

df = st.session_state.df
db_df = st.session_state.db_df
my_store_name = "DB KLIK"

st.sidebar.header("Kontrol & Filter")

if st.sidebar.button("Hapus Cache & Tarik Ulang 🔄", key="clear_cache_button"):
    st.cache_data.clear()
    st.session_state.data_loaded = False
    st.success("Cache berhasil dihapus. Muat ulang halaman untuk menarik data baru.")
    st.rerun()

st.sidebar.divider()

min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
if len(selected_date_range) != 2:
    st.sidebar.warning("Silakan pilih rentang tanggal yang valid."); st.stop()
start_date, end_date = selected_date_range

accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)

df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()

df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
main_store_df = df_filtered[df_filtered['Toko'] == my_store_name].copy()
competitor_df = df_filtered[df_filtered['Toko'] != my_store_name].copy()

# --- [LOGIKA INTI UNTUK PERHITUNGAN AKURAT] ---
latest_entries_weekly = df_filtered.loc[df_filtered.groupby(['Minggu', 'Toko', 'Nama Produk'])['Tanggal'].idxmax()]
latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
main_store_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == my_store_name]
competitor_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] != my_store_name]

st.info("💡 Kalkulasi omzet telah diperbaiki. Analisis mingguan & total kini didasarkan pada data snapshot terakhir setiap produk untuk akurasi maksimal.")


st.sidebar.divider()
st.sidebar.header("Ekspor & Info Data")
st.sidebar.info(f"Data yang akan diekspor berisi **{len(df_filtered)}** baris.")

csv_data = convert_df_for_download(df_filtered)
json_data = convert_df_to_json(df_filtered)

st.sidebar.download_button(label="📥 Unduh sebagai CSV", data=csv_data, file_name=f'analisis_data_{start_date}_{end_date}.csv', mime='text/csv')
st.sidebar.download_button(label="📥 Unduh sebagai JSON", data=json_data, file_name=f'analisis_data_{start_date}_{end_date}.json', mime='application/json')

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["⭐ Analisis Toko Saya", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

with tab1:
    st.header(f"Analisis Kinerja Toko: {my_store_name}")
    
    section_counter = 1

    st.subheader(f"{section_counter}. Analisis Kategori Terlaris (Berdasarkan Omzet)")
    section_counter += 1
    if not db_df.empty and 'KATEGORI' in db_df.columns and 'NAMA' in db_df.columns:
        @st.cache_data
        def fuzzy_merge_categories(_rekap_df, _database_df):
            _rekap_df['Kategori'] = 'Lainnya'
            db_unique = _database_df.drop_duplicates(subset=['NAMA'])
            db_map = db_unique.set_index('NAMA')['KATEGORI']
            for index, row in _rekap_df.iterrows():
                if pd.notna(row['Nama Produk']):
                    match, score = process.extractOne(row['Nama Produk'], db_map.index, scorer=fuzz.token_set_ratio)
                    if score >= 80:
                        _rekap_df.loc[index, 'Kategori'] = db_map[match]
            return _rekap_df
        
        main_store_cat = fuzzy_merge_categories(main_store_latest_overall.copy(), db_df)
        category_sales = main_store_cat.groupby('Kategori')['Omzet'].sum().reset_index()
        
        if not category_sales.empty:
            col1, col2 = st.columns([1,2])
            sort_order_cat = col1.radio("Urutkan:", ["Omzet Tertinggi", "Omzet Terendah"], horizontal=True, key="cat_sort")
            top_n_cat = col2.number_input("Tampilkan Top:", 1, len(category_sales), min(10, len(category_sales)), key="cat_top_n")
            cat_sales_sorted = category_sales.sort_values('Omzet', ascending=(sort_order_cat == "Omzet Terendah")).head(top_n_cat)
            fig_cat = px.bar(cat_sales_sorted, x='Kategori', y='Omzet', title=f'Top {top_n_cat} Kategori Berdasarkan Omzet', text_auto='.2s')
            st.plotly_chart(fig_cat, use_container_width=True)

    st.subheader(f"{section_counter}. Produk Terlaris")
    section_counter += 1
    top_products = main_store_latest_overall.sort_values('Terjual per Bulan', ascending=False).head(15).copy()
    top_products['Harga_rp'] = top_products['Harga'].apply(lambda x: f"Rp {x:,.0f}")
    top_products['Omzet_rp'] = top_products['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
    top_products['Tanggal_fmt'] = top_products['Tanggal'].dt.strftime('%Y-%m-%d')
    
    display_df_top = top_products[['Nama Produk', 'Harga_rp', 'Omzet_rp', 'Tanggal_fmt']].rename(
        columns={'Harga_rp': 'Harga', 'Omzet_rp': 'Omzet', 'Tanggal_fmt': 'Update Terakhir'}
    )
    st.dataframe(display_df_top, use_container_width=True, hide_index=True)

    st.subheader(f"{section_counter}. Distribusi Omzet Brand")
    section_counter += 1
    brand_omzet_main = main_store_latest_overall.groupby('Brand')['Omzet'].sum().reset_index()
    if not brand_omzet_main.empty:
        fig_brand_pie = px.pie(brand_omzet_main.sort_values('Omzet', ascending=False).head(7), 
                               names='Brand', 
                               values='Omzet', 
                               title='Distribusi Omzet Top 7 Brand (Snapshot Terakhir)')
        fig_brand_pie.update_traces(
            textposition='outside', 
            texttemplate='<b>%{label}</b><br>%{percent}<br>Rp %{value:,.0f}'
        )
        st.plotly_chart(fig_brand_pie, use_container_width=True)
    else:
        st.info("Tidak ada data omzet brand.")

    st.subheader(f"{section_counter}. Ringkasan Kinerja Mingguan (WoW Growth)")
    section_counter += 1
    
    main_store_latest_weekly = main_store_df.loc[main_store_df.groupby(['Minggu', 'Nama Produk'])['Tanggal'].idxmax()]
    
    weekly_summary_tab1 = main_store_latest_weekly.groupby('Minggu').agg(
        Omzet=('Omzet', 'sum'),
        Penjualan_Unit=('Terjual per Bulan', 'sum')
    ).reset_index()
    
    weekly_summary_tab1.sort_values('Minggu', inplace=True) 
    
    weekly_summary_tab1['Pertumbuhan Omzet (WoW)'] = weekly_summary_tab1['Omzet'].pct_change().apply(format_wow_growth)
    weekly_summary_tab1['Omzet'] = weekly_summary_tab1['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
    st.dataframe(
        weekly_summary_tab1[['Minggu', 'Omzet', 'Penjualan_Unit', 'Pertumbuhan Omzet (WoW)']].style.apply(
            lambda s: s.map(style_wow_growth), subset=['Pertumbuhan Omzet (WoW)']
        ), 
        use_container_width=True, 
        hide_index=True
    )


with tab2:
    st.header(f"Perbandingan Produk '{my_store_name}' dengan Kompetitor")
    st.subheader("1. Detail Produk di Toko Anda (Data Terbaru)")
    if not main_store_df.empty:
        latest_date = main_store_df['Tanggal'].max()
        main_store_latest = main_store_df[main_store_df['Tanggal'] == latest_date].copy()
        
        main_store_latest['Harga_fmt'] = main_store_latest['Harga'].apply(lambda x: f"Rp {x:,.0f}")
        st.dataframe(main_store_latest[['Tanggal', 'Nama Produk', 'Harga_fmt', 'Status', 'Stok']].rename(columns={'Harga_fmt':'Harga'}), use_container_width=True, hide_index=True)
        
        st.subheader("2. Pilih Produk untuk Dibandingkan")
        product_list = sorted(main_store_latest['Nama Produk'].unique())
        selected_product = st.selectbox("Pilih produk dari toko Anda:", product_list)
        
        if selected_product:
            product_info_list = main_store_latest[main_store_latest['Nama Produk'] == selected_product]
            if not product_info_list.empty:
                product_info = product_info_list.iloc[0]
                st.markdown(f"**Produk Pilihan Anda:** *{product_info['Nama Produk']}*")
                col1, col2, col3 = st.columns(3)
                col1.metric(f"Harga di {my_store_name}", product_info['Harga_fmt'])
                col2.metric("Status", product_info['Status'])
                col3.metric("Stok", product_info['Stok'])
                
                st.markdown("---")
                competitor_latest = competitor_df[competitor_df['Tanggal'] == latest_date]
                if not competitor_latest.empty:
                    competitor_products_tuple = tuple(competitor_latest['Nama Produk'].tolist())
                    matches = get_smart_matches(product_info['Nama Produk'], competitor_products_tuple, score_cutoff=accuracy_cutoff)
                    
                    st.markdown(f"**Tren Harga Historis**")
                    competitor_product_names = [match[0] for match in matches]
                    products_to_track = [product_info['Nama Produk']] + competitor_product_names
                    price_history_df = df_filtered[df_filtered['Nama Produk'].isin(products_to_track)].copy()

                    if not price_history_df.empty:
                        fig_price_trend = px.line(price_history_df, x='Tanggal', y='Harga', color='Toko', line_dash='Nama Produk', markers=True, title='Perubahan Harga Produk dari Waktu ke Waktu')
                        fig_price_trend.update_layout(yaxis_title="Harga (Rp)")
                        st.plotly_chart(fig_price_trend, use_container_width=True)
                    else:
                        st.info("Tidak ada data historis yang cukup untuk membuat grafik tren harga.")

                    st.markdown(f"**Perbandingan di Toko Kompetitor (Data Terbaru):**")
                    if not matches:
                        st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                    else:
                        for product, score in matches:
                            match_info_list = competitor_latest[competitor_latest['Nama Produk'] == product]
                            if not match_info_list.empty:
                                match_info = match_info_list.iloc[0]
                                price_diff = int(match_info['Harga']) - int(product_info['Harga'])
                                
                                with st.container(border=True):
                                    st.markdown(f"**Toko: {match_info['Toko']}** (Kemiripan: {int(score)}%)")
                                    st.markdown(f"*{match_info['Nama Produk']}*")
                                    c1, c2, c3 = st.columns(3)
                                    c1.metric("Harga Kompetitor", f"Rp {match_info['Harga']:,.0f}", delta=f"Rp {price_diff:,.0f} {'(Lebih Mahal)' if price_diff > 0 else '(Lebih Murah)' if price_diff < 0 else ''}")
                                    c2.metric("Status", match_info['Status'])
                                    c3.metric("Stok", match_info['Stok'])

with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    if competitor_df.empty:
        st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
    else:
        competitor_list = sorted(competitor_df['Toko'].unique())
        for competitor_store in competitor_list:
            with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                single_competitor_df = competitor_latest_overall[competitor_latest_overall['Toko'] == competitor_store]
                brand_analysis = single_competitor_df.groupby('Brand').agg(Total_Omzet=('Omzet', 'sum'), Total_Unit_Terjual=('Terjual per Bulan', 'sum')).reset_index().sort_values("Total_Omzet", ascending=False)
                
                if not brand_analysis.empty:
                    st.dataframe(brand_analysis, use_container_width=True, hide_index=True)
                    fig_pie_comp = px.pie(brand_analysis.head(7), names='Brand', values='Total_Omzet', title=f'Distribusi Omzet Top 7 Brand di {competitor_store} (Snapshot Terakhir)')
                    st.plotly_chart(fig_pie_comp, use_container_width=True)
                else:
                    st.info("Tidak ada data brand untuk toko ini.")

with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    stock_trends = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
    if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
    if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
    stock_trends_melted = stock_trends.melt(id_vars=['Minggu', 'Toko'], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
    
    fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
    st.plotly_chart(fig_stock_trends, use_container_width=True)
    st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)

with tab5:
    st.header("Analisis Kinerja Penjualan (Semua Toko)")
    
    all_stores_latest_per_week = latest_entries_weekly.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
    fig_weekly_omzet = px.line(all_stores_latest_per_week, x='Minggu', y='Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko (Berdasarkan Snapshot Terakhir)')
    st.plotly_chart(fig_weekly_omzet, use_container_width=True)
    
    st.subheader("Tabel Rincian Omzet per Tanggal")
    if not df_filtered.empty:
        omzet_pivot = df_filtered.pivot_table(
            index='Toko', 
            columns='Tanggal', 
            values='Omzet', 
            aggfunc='sum'
        ).fillna(0)
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
                        new_products_df['Harga_fmt'] = new_products_df['Harga'].apply(lambda x: f"Rp {x:,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'Harga_fmt', 'Stok', 'Brand']].rename(columns={'Harga_fmt':'Harga'}), use_container_width=True, hide_index=True)

