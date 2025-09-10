# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi: Pengamanan Kredensial dengan st.secrets untuk Deploy Online
# ===================================================================================

import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import plotly.graph_objects as go
import re
import gspread
from io import BytesIO
import numpy as np
import time

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis - DB KLIK")

# ===================================================================================
# FUNGSI-FUNGSI INTI
# ===================================================================================
@st.cache_data(show_spinner="Mengambil data dari Google Sheets...")
def load_data_from_gsheets():
    """
    Memuat data dari Google Sheets menggunakan kredensial aman dari st.secrets.
    Fungsi ini dirancang untuk bekerja baik secara lokal maupun di Streamlit Cloud.
    """
    try:
        # Mengambil kredensial dari st.secrets
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"],
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key(st.secrets["gsheet_key"])
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: {e}")
        st.info("Pastikan Anda telah mengonfigurasi 'Secrets' di pengaturan aplikasi Streamlit Cloud Anda.")
        return None, None

    # Daftar sheet yang akan diambil
    sheet_names = [
        "DATABASE", "kamus_brand", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS", "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS", "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS", "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS", "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS"
    ]

    rekap_list = []
    database_df = pd.DataFrame()
    kamus_brand_df = pd.DataFrame()

    progress_text = "Memuat sheet... Mohon tunggu."
    my_bar = st.progress(0, text=progress_text)
    total_sheets = len(sheet_names)

    for i, sheet_name in enumerate(sheet_names):
        try:
            ws = spreadsheet.worksheet(sheet_name)
            values = ws.get_all_values()
            time.sleep(1.1) # Jeda untuk menghindari error kuota API

            if not values or len(values) < 2: continue
            header = [str(h).strip() for h in values[0]]
            data = values[1:]
            df = pd.DataFrame(data, columns=header)
            
            # Menghapus kolom yang sepenuhnya kosong atau tidak bernama
            df = df.loc[:, [col for col in df.columns if col]]

            if sheet_name.upper() == 'DATABASE':
                database_df = df
            elif sheet_name.lower().startswith('kamus'):
                kamus_brand_df = df
            elif 'REKAP' in sheet_name.upper():
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                toko = store_name_match.group(1).strip() if store_name_match else sheet_name
                df['Toko'] = toko
                df['Status'] = 'Ready' if 'READY' in sheet_name.upper() else 'Habis'
                rekap_list.append(df)
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Sheet '{sheet_name}' tidak ditemukan, dilewati.")
        except Exception as e:
            st.error(f"Gagal memuat sheet '{sheet_name}': {e}")
            if "Quota exceeded" in str(e):
                st.error("Google Sheets API Quota Exceeded. Coba lagi dalam beberapa menit.")
            return None, None
        
        my_bar.progress((i + 1) / total_sheets, text=f"Memuat sheet: {sheet_name}")

    my_bar.empty()
    if not rekap_list:
        st.error('Tidak ada data REKAP yang valid di Google Sheets.')
        return None, None

    rekap_df = pd.concat(rekap_list, ignore_index=True)

    def norm_cols(df_to_norm):
        df_to_norm.columns = [str(c).strip().upper() for c in df_to_norm.columns]
        return df_to_norm

    database_df = norm_cols(database_df) if not database_df.empty else database_df
    kamus_brand_df = norm_cols(kamus_brand_df) if not kamus_brand_df.empty else kamus_brand_df
    rekap_df = norm_cols(rekap_df)

    rename_map = {
        'NAMA': 'Nama Produk', 'NAMA PRODUK': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan',
        'TERJUAL': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga',
        'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status', 'KATEGORI': 'Kategori'
    }
    rekap_df.rename(columns=lambda x: rename_map.get(x, x), inplace=True)
    if not database_df.empty:
        database_df.rename(columns=rename_map, inplace=True)

    rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df.get('Terjual per Bulan', 0), errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)

    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Terjual per Bulan'] = rekap_df['Terjual per Bulan'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']

    if not kamus_brand_df.empty and all(c in kamus_brand_df.columns for c in ['ALIAS', 'BRAND_UTAMA']):
        alias_map = {str(k).strip().upper(): str(v).strip().upper() for k,v in kamus_brand_df.set_index('ALIAS')['BRAND_UTAMA'].to_dict().items()}
        rekap_df['Brand'] = rekap_df.get('Brand', pd.Series(dtype='str')).apply(lambda b: alias_map.get(str(b).strip().upper(), str(b).strip().upper()) if pd.notna(b) else 'UNKNOWN')
    elif 'Brand' not in rekap_df.columns:
         rekap_df['Brand'] = rekap_df['Nama Produk'].str.split().str[0].str.upper()

    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True, keep='last')
    return rekap_df.sort_values('Tanggal'), database_df

@st.cache_data(show_spinner="Mencocokkan kategori produk...")
def match_categories(product_df, db_df, accuracy_cutoff):
    if db_df.empty or 'Nama Produk' not in db_df.columns or 'Kategori' not in db_df.columns:
        product_df['Kategori'] = 'Tidak Diketahui'
        return product_df

    db_choices = db_df['Nama Produk'].dropna().unique().tolist()
    db_map = db_df.dropna(subset=['Nama Produk', 'Kategori']).set_index('Nama Produk')['Kategori'].to_dict()

    def get_category(name):
        match = process.extractOne(name, db_choices, scorer=fuzz.token_set_ratio, score_cutoff=accuracy_cutoff)
        return db_map.get(match[0]) if match else 'Lainnya'

    product_df['Kategori'] = product_df['Nama Produk'].apply(get_category)
    return product_df

def format_rupiah(value):
    return f"Rp {value:,.0f}".replace(',', '.')

def format_wow_growth(pct):
    if pd.isna(pct) or np.isinf(pct): return 'N/A'
    if pct > 0: return f'🟢 ▲ {pct:.1%}'
    if pct < 0: return f'🔴 ▼ {abs(pct):.1%}'
    return f'⚪ ▬ 0.0%'

@st.cache_data
def convert_df_for_download(df_to_convert, format_type='csv'):
    if format_type == 'csv':
        return df_to_convert.to_csv(index=False).encode('utf-8')
    else:
        return df_to_convert.to_json(orient='records', indent=4).encode('utf-8')

# ===================================================================================
# UI - TAMPILAN UTAMA
# ===================================================================================
st.title('📊 Dashboard Analisis Penjualan & Kompetitor')

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if not st.session_state.data_loaded:
    col1, col2, col3 = st.columns([2, 3, 2])
    with col2:
        if st.button('🚀 Tarik Data & Mulai Analisis', use_container_width=True):
            df, db_df = load_data_from_gsheets()
            if df is not None:
                st.session_state.df = df
                st.session_state.db_df = db_df
                st.session_state.data_loaded = True
                st.rerun()
    st.info("Klik tombol untuk memulai. Pastikan Anda telah mengonfigurasi 'Secrets' di pengaturan aplikasi online Anda.")
    st.stop()

# --- BAGIAN UTAMA APLIKASI SETELAH DATA DIMUAT ---
df = st.session_state.df
db_df = st.session_state.db_df
MY_STORE_NAME = 'DB KLIK'

with st.sidebar:
    st.header('⚙️ Kontrol & Filter')
    if st.button('🔄 Hapus Cache & Tarik Ulang', use_container_width=True):
        st.cache_data.clear()
        st.session_state.data_loaded = False
        st.rerun()
    st.divider()
    min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
    selected_date_range = st.date_input('Rentang Tanggal Analisis', [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(selected_date_range) != 2: st.stop()
    start_date, end_date = selected_date_range
    accuracy_cutoff = st.slider('Tingkat Akurasi Fuzzy Match (%)', 70, 100, 90)

df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error(f"Tidak ada data pada rentang tanggal {start_date} hingga {end_date}."); st.stop()

df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-MON').apply(lambda p: p.start_time).dt.date
latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()].copy()
main_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == MY_STORE_NAME].copy()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    f"📈 Kinerja {MY_STORE_NAME}", "🆚 Perbandingan Produk", "🏢 Brand Kompetitor",
    "📦 Tren Stok", "💰 Omzet Toko", "🆕 Produk Baru"
])

with tab1:
    st.header(f"Analisis Kinerja Toko {MY_STORE_NAME}")
    st.subheader("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
    main_categorized_df = match_categories(main_latest_overall.copy(), db_df, accuracy_cutoff)
    category_omzet = main_categorized_df.groupby('Kategori')['Omzet'].sum().sort_values(ascending=False).reset_index()
    col1, col2 = st.columns([1, 2])
    with col1:
        sort_order = st.radio("Urutkan Kategori:", ('Terlaris', 'Paling Tidak Laris'), horizontal=True, key="sort_cat")
        num_bars = st.slider('Jumlah Kategori', 3, len(category_omzet), 10)
        ascending = (sort_order == 'Paling Tidak Laris')
        category_omzet_sorted = category_omzet.sort_values('Omzet', ascending=ascending).head(num_bars)
    with col2:
        fig_cat = px.bar(category_omzet_sorted, x='Omzet', y='Kategori', orientation='h', title=f'Top {num_bars} Kategori', text='Omzet')
        fig_cat.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig_cat.update_layout(yaxis={'categoryorder':'total ascending' if not ascending else 'total descending'})
        st.plotly_chart(fig_cat, use_container_width=True)
    
    st.write("Produk Terlaris Berdasarkan Kategori")
    selected_category = st.selectbox('Pilih Kategori', category_omzet['Kategori'].unique())
    products_in_category = main_categorized_df[main_categorized_df['Kategori'] == selected_category]
    products_in_category['Harga'] = products_in_category['Harga'].apply(format_rupiah)
    st.dataframe(products_in_category.sort_values('Terjual per Bulan', ascending=False)[['Nama Produk', 'Harga', 'Terjual per Bulan', 'Status']], use_container_width=True)

    st.subheader("2. Produk Terlaris (Global)")
    weekly_omzet_pivot = df_filtered[df_filtered['Toko'] == MY_STORE_NAME].pivot_table(index='Nama Produk', columns='Minggu', values='Omzet', aggfunc='sum')
    if len(weekly_omzet_pivot.columns) > 1:
        growth_map = (weekly_omzet_pivot.iloc[:, -1].div(weekly_omzet_pivot.iloc[:, -2]) - 1).to_dict()
        main_latest_overall['Indikator'] = main_latest_overall['Nama Produk'].map(growth_map).apply(format_wow_growth)
    else:
        main_latest_overall['Indikator'] = 'N/A'
    top_products_global = main_latest_overall.sort_values('Omzet', ascending=False)
    top_products_global['Harga'] = top_products_global['Harga'].apply(format_rupiah)
    top_products_global['Omzet'] = top_products_global['Omzet'].apply(format_rupiah)
    st.dataframe(top_products_global[['Nama Produk', 'Harga', 'Terjual per Bulan', 'Omzet', 'Tanggal', 'Indikator']], use_container_width=True)

    col_brand, col_wow = st.columns(2)
    with col_brand:
        st.subheader("3. Distribusi Omzet Brand")
        brand_omzet = main_latest_overall.groupby('Brand')['Omzet'].sum().nlargest(7)
        fig_brand = px.pie(values=brand_omzet.values, names=brand_omzet.index, title=f'Distribusi Omzet Brand', hole=0.3)
        st.plotly_chart(fig_brand, use_container_width=True)
    with col_wow:
        st.subheader("4. Ringkasan Kinerja Mingguan (WoW)")
        weekly_summary = df_filtered[df_filtered['Toko'] == MY_STORE_NAME].groupby('Minggu').agg(Omzet=('Omzet', 'sum'), Penjualan_Unit=('Nama Produk', 'count')).reset_index()
        weekly_summary['Pertumbuhan Omzet'] = weekly_summary['Omzet'].pct_change().apply(format_wow_growth)
        weekly_summary['Omzet'] = weekly_summary['Omzet'].apply(format_rupiah)
        st.dataframe(weekly_summary.sort_values('Minggu', ascending=False), use_container_width=True)

with tab2:
    st.header("Perbandingan Harga & Stok Produk")
    product_list = main_latest_overall['Nama Produk'].unique()
    selected_product = st.selectbox("Pilih Produk", options=product_list, index=None, placeholder="Ketik untuk mencari...")
    if selected_product:
        main_product_data = main_latest_overall[main_latest_overall['Nama Produk'] == selected_product].iloc[0]
        st.subheader(f"Analisis: {selected_product}")
        history_main = df_filtered[(df_filtered['Nama Produk'] == selected_product) & (df_filtered['Toko'] == MY_STORE_NAME)]
        fig_price_trend = go.Figure(go.Scatter(x=history_main['Tanggal'], y=history_main['Harga'], mode='lines+markers', name=MY_STORE_NAME))
        
        matches = process.extract(selected_product, latest_entries_overall[latest_entries_overall['Toko'] != MY_STORE_NAME]['Nama Produk'].unique(), limit=5, scorer=fuzz.token_set_ratio, score_cutoff=accuracy_cutoff)
        for match_name, score in matches:
            competitor_data = latest_entries_overall[latest_entries_overall['Nama Produk'] == match_name].iloc[0]
            history_competitor = df_filtered[(df_filtered['Nama Produk'] == match_name) & (df_filtered['Toko'] == competitor_data['Toko'])]
            fig_price_trend.add_trace(go.Scatter(x=history_competitor['Tanggal'], y=history_competitor['Harga'], mode='lines+markers', name=f"{competitor_data['Toko']} ({score}%)"))
        
        st.plotly_chart(fig_price_trend.update_layout(title="Tren Harga Produk"), use_container_width=True)

with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    brand_analysis = latest_entries_overall[latest_entries_overall['Toko'] != MY_STORE_NAME].groupby('Brand').agg(Total_Omzet=('Omzet', 'sum'), Unit_Terjual=('Terjual per Bulan', 'sum')).nlargest(10, 'Total_Omzet').reset_index()
    fig_brand_comp = px.pie(brand_analysis, values='Total_Omzet', names='Brand', title='Top 10 Brand Kompetitor')
    st.plotly_chart(fig_brand_comp, use_container_width=True)
    st.dataframe(brand_analysis, use_container_width=True)

with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    stock_status_weekly = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
    fig_stock = px.line(stock_status_weekly.melt(id_vars=['Minggu', 'Toko'], value_vars=['Ready', 'Habis']), x='Minggu', y='value', color='Toko', line_dash='Status', markers=True)
    st.plotly_chart(fig_stock, use_container_width=True)
    st.dataframe(stock_status_weekly, use_container_width=True)

with tab5:
    st.header("Perbandingan Omzet Total Antar Toko")
    omzet_pivot = df_filtered.pivot_table(index='Tanggal', columns='Toko', values='Omzet', aggfunc='sum').fillna(0)
    fig_omzet_toko = px.line(omzet_pivot, x=omzet_pivot.index, y=omzet_pivot.columns)
    st.plotly_chart(fig_omzet_toko, use_container_width=True)
    st.dataframe(omzet_pivot.applymap(lambda x: f"{x:,.0f}"), use_container_width=True)

with tab6:
    st.header("Analisis Produk Baru Mingguan")
    unique_weeks = sorted(df_filtered['Minggu'].unique(), reverse=True)
    if len(unique_weeks) > 1:
        target_week, comparison_week = st.columns(2)
        target = target_week.selectbox("Pilih Minggu Target", options=unique_weeks, index=0)
        comparison = comparison_week.selectbox("Pilih Minggu Pembanding", options=[w for w in unique_weeks if w < target], index=0)
        
        products_target = set(df_filtered[(df_filtered['Minggu'] == target) & (df_filtered['Toko'] == MY_STORE_NAME)]['Nama Produk'])
        products_comparison = set(df_filtered[(df_filtered['Minggu'] == comparison) & (df_filtered['Toko'] == MY_STORE_NAME)]['Nama Produk'])
        new_products = list(products_target - products_comparison)
        
        if new_products:
            new_products_df = latest_entries_overall[latest_entries_overall['Nama Produk'].isin(new_products) & (latest_entries_overall['Toko'] == MY_STORE_NAME)]
            st.dataframe(new_products_df[['Nama Produk', 'Harga', 'Stok', 'Brand']], use_container_width=True)
        else:
            st.success("Tidak ada produk baru.")

with st.sidebar:
    st.divider()
    st.header('📄 Ekspor & Info Data')
    st.info(f"Baris data diolah: **{len(df_filtered):,}**")
    st.download_button('📥 Unduh CSV', data=convert_df_for_download(df_filtered, 'csv'), file_name=f'analisis.csv', use_container_width=True)
    st.download_button('📥 Unduh JSON', data=convert_df_for_download(df_filtered, 'json'), file_name=f'analisis.json', use_container_width=True)

