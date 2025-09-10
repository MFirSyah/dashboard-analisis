# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI FINAL V7
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Catatan: Versi ini mengimplementasikan semua 6 tab analisis yang diminta
#  dengan fitur caching, fuzzy matching, dan visualisasi interaktif.
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

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis - DB KLIK")

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

@st.cache_data(show_spinner="Mengambil data dari Google Sheets...")
def load_data_from_gsheets(gsheet_key):
    """
    Memuat data dari Google Sheets menggunakan kredensial yang tersimpan di st.secrets.
    Fungsi ini di-cache untuk mencegah pengambilan data berulang kali.
    """
    try:
        creds_dict = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key(gsheet_key)
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: Pastikan file secrets.toml sudah benar. Error: {e}")
        return None, None

    sheet_names = [
        "DATABASE", "kamus_brand", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
        "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
        "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS",
        "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS"
    ]

    rekap_list = []
    database_df = pd.DataFrame()

    try:
        with st.spinner("Memproses setiap sheet..."):
            for sheet_name in sheet_names:
                try:
                    ws = spreadsheet.worksheet(sheet_name)
                    values = ws.get_all_values()
                    if not values or len(values) < 2: continue
                    
                    df = pd.DataFrame(values[1:], columns=values[0])
                    df = df.loc[:, ~df.columns.str.strip().eq('')] # Hapus kolom tanpa nama

                    if sheet_name.upper() == 'DATABASE':
                        database_df = df
                    elif 'REKAP' in sheet_name.upper():
                        store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                        toko = store_name_match.group(1).strip() if store_name_match else sheet_name
                        df['Toko'] = toko
                        df['Status'] = 'READY' if 'READY' in sheet_name.upper() else 'HABIS'
                        rekap_list.append(df)
                except gspread.exceptions.WorksheetNotFound:
                    # Jika sheet tidak ditemukan, berikan peringatan dan lanjutkan ke sheet berikutnya.
                    st.warning(f"Sheet '{sheet_name}' tidak ditemukan, dilewati.")
                    continue
    except Exception as e:
        st.error(f"Gagal memuat sheet: {e}")
        return None, None

    if not rekap_list:
        st.error('Tidak ada data REKAP yang valid di Google Sheets.')
        return None, None

    rekap_df = pd.concat(rekap_list, ignore_index=True)
    return rekap_df, database_df

@st.cache_data(show_spinner="Membersihkan dan memproses data...")
def process_data(rekap_df):
    """
    Membersihkan, menormalkan tipe data, dan menghitung kolom turunan seperti Omzet.
    """
    df = rekap_df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]

    rename_map = {
        'NAMA': 'Nama Produk', 'NAMA PRODUK': 'Nama Produk', 
        'TERJUAL/BLN': 'Terjual/Bulan', 'TERJUAL': 'Terjual/Bulan',
        'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand', 
        'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'
    }
    df.rename(columns=lambda x: rename_map.get(x.strip().upper(), x.strip().upper()), inplace=True)
    
    # Memastikan kolom esensial ada
    for col in ['Nama Produk', 'Tanggal', 'Harga', 'Toko', 'Status']:
        if col not in df.columns:
            st.error(f"Kolom wajib '{col}' tidak ditemukan di data rekap. Proses dihentikan.")
            return None
            
    if 'Terjual/Bulan' not in df.columns:
        df['Terjual/Bulan'] = 0
    if 'Brand' not in df.columns:
        df['Brand'] = "UNKNOWN"

    # Konversi tipe data
    df['Nama Produk'] = df['Nama Produk'].astype(str).str.strip()
    df['Tanggal'] = pd.to_datetime(df['Tanggal'], errors='coerce', dayfirst=True)
    df['Harga'] = pd.to_numeric(df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    df['Terjual/Bulan'] = pd.to_numeric(df['Terjual/Bulan'], errors='coerce').fillna(0)
    
    # Hapus baris dengan data krusial yang hilang
    df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)
    
    df['Harga'] = df['Harga'].astype(int)
    df['Terjual/Bulan'] = df['Terjual/Bulan'].astype(int)
    df['Omzet'] = df['Harga'] * df['Terjual/Bulan']
    
    # Standardisasi Brand (opsional, bisa ditambahkan jika ada kamus_brand)
    df['Brand'] = df['Brand'].astype(str).str.upper().str.strip()
    df.loc[df['Brand'] == '', 'Brand'] = 'UNKNOWN'

    # Hapus duplikat berdasarkan entri terakhir pada hari yang sama
    df.sort_values('Tanggal', ascending=True, inplace=True)
    df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True, keep='last')
    
    return df

@st.cache_data(show_spinner="Menyesuaikan kategori produk (fuzzy matching)...")
def assign_categories(toko_df, db_df, score_cutoff=90):
    """
    Memberikan label kategori ke produk toko berdasarkan fuzzy matching dengan DATABASE.
    """
    if db_df.empty or 'NAMA' not in db_df.columns or 'Kategori' not in db_df.columns:
        st.warning("Sheet 'DATABASE' tidak valid atau tidak memiliki kolom 'NAMA'/'Kategori'. Kategori tidak dapat dipetakan.")
        toko_df['Kategori'] = 'Tidak Diketahui'
        return toko_df

    toko_df_copy = toko_df.copy()
    db_products = db_df['NAMA'].dropna().unique()
    db_mapping = db_df.set_index('NAMA')['Kategori'].to_dict()

    unique_store_products = toko_df_copy['Nama Produk'].unique()
    category_map = {}

    for product in unique_store_products:
        match = process.extractOne(product, db_products, scorer=fuzz.token_set_ratio)
        if match and match[1] >= score_cutoff:
            category_map[product] = db_mapping.get(match[0], 'Tidak Diketahui')
        else:
            category_map[product] = 'Tidak Diketahui'

    toko_df_copy['Kategori'] = toko_df_copy['Nama Produk'].map(category_map)
    return toko_df_copy

# ===================================================================================
# FUNGSI-FUNGSI BANTU & FORMATTING
# ===================================================================================
def format_rupiah(angka):
    return f"Rp {angka:,.0f}".replace(",", ".")

def format_wow_growth(pct):
    if pd.isna(pct) or np.isinf(pct):
        return 'N/A'
    if pct > 0:
        return f'▲ {pct:.1%}'
    if pct < 0:
        return f'▼ {abs(pct):.1%}'
    return '▬ 0.0%'

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

@st.cache_data
def convert_df_to_json(df):
    return df.to_json(orient='records', indent=4).encode('utf-8')

# ===================================================================================
# UI - TAMPILAN AWAL & LOADING DATA
# ===================================================================================

st.title('📊 Dashboard Analisis Penjualan & Kompetitor')

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df = None
    st.session_state.db_df = None

# Tampilan tombol start jika data belum dimuat
if not st.session_state.data_loaded:
    st.info("Aplikasi ini akan menarik dan menganalisis data penjualan dari Google Sheets.")
    c1, c2, c3 = st.columns([2, 3, 2])
    with c2:
        if st.button('🚀 Tarik Data & Mulai Analisis', use_container_width=True):
            gsheet_id = st.secrets.get("gsheet_key", "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
            if not gsheet_id:
                st.error("gsheet_key tidak ditemukan di secrets.toml.")
            else:
                rekap_df, db_df = load_data_from_gsheets(gsheet_id)
                if rekap_df is not None:
                    processed_df = process_data(rekap_df)
                    if processed_df is not None:
                        st.session_state.df = processed_df
                        st.session_state.db_df = db_df
                        st.session_state.data_loaded = True
                        st.rerun()
                    else:
                        st.error("Gagal memproses data.")
                else:
                    st.error("Gagal memuat data dari Google Sheets.")
    st.stop()

# ===================================================================================
# UI - SIDEBAR & FILTER (Tampil setelah data dimuat)
# ===================================================================================
df = st.session_state.df
db_df = st.session_state.db_df
my_store_name = 'DB KLIK'

st.sidebar.header('⚙️ Kontrol & Filter')

if st.sidebar.button('Hapus Cache & Tarik Ulang 🔄', use_container_width=True):
    st.cache_data.clear()
    st.session_state.data_loaded = False
    st.rerun()

st.sidebar.divider()

min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
selected_date_range = st.sidebar.date_input(
    'Pilih Rentang Tanggal Analisis',
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date,
    help="Filter semua data dalam rentang tanggal yang dipilih."
)
if len(selected_date_range) != 2:
    st.sidebar.warning('Pilih rentang tanggal yang valid.'); st.stop()
start_date, end_date = selected_date_range

accuracy_cutoff = st.sidebar.slider(
    'Tingkat Akurasi Fuzzy (%)', 80, 100, 91,
    help="Tingkat kemiripan nama produk untuk pencocokan (Tab 1 & 2)."
)

# Filter utama
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error('Tidak ada data pada rentang tanggal yang dipilih.'); st.stop()

# Pre-kalkulasi data untuk efisiensi
df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-MON').apply(lambda p: p.start_time).dt.date
latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
main_store_df = df_filtered[df_filtered['Toko'] == my_store_name].copy()
competitor_df = df_filtered[df_filtered['Toko'] != my_store_name].copy()
main_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == my_store_name].copy()

# Sidebar Lanjutan & Ekspor
st.sidebar.header('📄 Ekspor & Info')
st.sidebar.info(f"Total baris data diolah: **{len(df_filtered):,}**")
st.sidebar.download_button('📥 Unduh CSV', data=convert_df_to_csv(df_filtered), file_name=f'analisis_{start_date}_to_{end_date}.csv', use_container_width=True)
st.sidebar.download_button('📥 Unduh JSON', data=convert_df_to_json(df_filtered), file_name=f'analisis_{start_date}_to_{end_date}.json', use_container_width=True)


# ===================================================================================
# UI - TABS UTAMA
# ===================================================================================

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Analisis DB KLIK", "⚖️ Perbandingan Produk", "🏢 Analisis Kompetitor",
    "📦 Tren Stok", "💰 Omzet Lintas Toko", "🆕 Produk Baru"
])

# ===================================================================================
# TAB 1: ANALISIS DB KLIK
# ===================================================================================
with tab1:
    st.header(f"Analisis Kinerja Toko: {my_store_name}")
    if main_store_df.empty:
        st.warning(f"Tidak ada data untuk '{my_store_name}' pada rentang tanggal ini.")
    else:
        # --- 1. Analisis Kategori Terlaris ---
        st.subheader("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
        
        main_store_categorized = assign_categories(main_latest_overall, db_df, accuracy_cutoff)
        
        category_omzet = main_store_categorized.groupby('Kategori')['Omzet'].sum().sort_values(ascending=False).reset_index()
        category_omzet = category_omzet[category_omzet['Kategori'] != 'Tidak Diketahui']

        col1, col2 = st.columns([2,1])
        with col1:
             show_top_bars = st.slider('Jumlah Kategori untuk Ditampilkan', 3, len(category_omzet), 10)
        with col2:
            sort_order = st.radio("Urutkan Berdasarkan", ["Terlaris", "Paling Tidak Laris"], horizontal=True)

        if sort_order == "Paling Tidak Laris":
            category_omzet = category_omzet.sort_values('Omzet', ascending=True)

        fig_cat = px.bar(
            category_omzet.head(show_top_bars),
            x='Omzet',
            y='Kategori',
            orientation='h',
            title=f'Top {show_top_bars} Kategori dengan Omzet Tertinggi',
            text_auto=True,
            labels={'Omzet': 'Total Omzet', 'Kategori': 'Kategori Produk'}
        )
        fig_cat.update_traces(texttemplate='%{x:,.0f}', textposition='outside', marker_color='#1f77b4')
        fig_cat.update_layout(yaxis={'categoryorder':'total ascending' if sort_order == "Terlaris" else 'total descending'}, height=max(400, show_top_bars * 35))
        st.plotly_chart(fig_cat, use_container_width=True)

        st.markdown("##### Produk Terlaris dari Kategori Pilihan")
        selected_cat = st.selectbox("Pilih kategori untuk melihat detail produk", category_omzet['Kategori'].unique())
        if selected_cat:
            top_products_in_cat = main_store_categorized[main_store_categorized['Kategori'] == selected_cat]\
                .sort_values('Omzet', ascending=False)\
                .head(10)[['Nama Produk', 'Harga', 'Terjual/Bulan', 'Status']]
            top_products_in_cat['Harga'] = top_products_in_cat['Harga'].apply(format_rupiah)
            st.dataframe(top_products_in_cat, use_container_width=True)

        st.divider()

        # --- 2. Produk Terlaris (Global) ---
        st.subheader("2. Produk Terlaris (Global) & Tren Mingguan")
        latest_weekly = main_store_df.loc[main_store_df.groupby(['Minggu', 'Nama Produk'])['Tanggal'].idxmax()].copy()
        latest_weekly.sort_values(['Nama Produk', 'Minggu'], inplace=True)
        latest_weekly['Omzet Sebelumnya'] = latest_weekly.groupby('Nama Produk')['Omzet'].shift(1)
        latest_weekly['Pertumbuhan Omzet'] = (latest_weekly['Omzet'] - latest_weekly['Omzet Sebelumnya']) / latest_weekly['Omzet Sebelumnya']
        
        latest_snapshot = latest_weekly.loc[latest_weekly.groupby('Nama Produk')['Minggu'].idxmax()]
        top_global_products = latest_snapshot.sort_values('Omzet', ascending=False).head(20)
        
        top_global_products['Indikator'] = top_global_products['Pertumbuhan Omzet'].apply(format_wow_growth)
        top_global_products['Harga'] = top_global_products['Harga'].apply(format_rupiah)
        top_global_products['Omzet'] = top_global_products['Omzet'].apply(format_rupiah)

        st.dataframe(top_global_products[['Nama Produk', 'Harga', 'Terjual/Bulan', 'Omzet', 'Tanggal', 'Indikator']], use_container_width=True)

        st.divider()

        # --- 3. Distribusi Omzet Brand ---
        st.subheader("3. Distribusi Omzet per Brand")
        brand_omzet = main_latest_overall.groupby('Brand')['Omzet'].sum().sort_values(ascending=False).reset_index()
        fig_brand = px.pie(
            brand_omzet.head(15), 
            values='Omzet', 
            names='Brand',
            title='Kontribusi Omzet dari 15 Brand Teratas',
            hole=0.3
        )
        fig_brand.update_traces(textinfo='percent+label', texttemplate='%{label}<br>%{percent:.1%}', hovertemplate='<b>%{label}</b><br>Omzet: %{value:,.0f}<extra></extra>')
        st.plotly_chart(fig_brand, use_container_width=True)

        st.divider()

        # --- 4. Ringkasan Kinerja Mingguan (WoW Growth) ---
        st.subheader("4. Ringkasan Kinerja Mingguan (WoW Growth)")
        weekly_summary = main_store_df.groupby('Minggu').agg(
            Omzet=('Omzet', 'sum'),
            Penjualan_Unit=('Nama Produk', 'nunique') # Jumlah produk unik yang tercatat
        ).reset_index().sort_values('Minggu')

        weekly_summary['Pertumbuhan Omzet'] = weekly_summary['Omzet'].pct_change()
        weekly_summary['Pertumbuhan Omzet'] = weekly_summary['Pertumbuhan Omzet'].apply(format_wow_growth)
        weekly_summary['Omzet'] = weekly_summary['Omzet'].apply(format_rupiah)
        
        st.dataframe(weekly_summary, use_container_width=True)


# ===================================================================================
# TAB 2: PERBANDINGAN PRODUK
# ===================================================================================
with tab2:
    st.header("Perbandingan Harga dan Ketersediaan Produk")
    
    product_list = main_latest_overall['Nama Produk'].unique()
    selected_product = st.selectbox("Pilih Produk dari DB KLIK untuk Dibandingkan", product_list)

    if selected_product:
        # Info produk utama
        my_product_data = main_store_df[main_store_df['Nama Produk'] == selected_product].sort_values('Tanggal', ascending=False)
        my_latest_data = my_product_data.iloc[0]
        
        st.markdown(f"#### Analisis untuk: **{selected_product}**")
        c1, c2, c3 = st.columns(3)
        c1.metric("Harga Terakhir (DB KLIK)", format_rupiah(my_latest_data['Harga']))
        c2.metric("Status", my_latest_data['Status'])
        c3.metric("Stok", my_latest_data.get('Stok', 'N/A'))
        
        # Line chart harga produk utama
        fig_price_trend = go.Figure()
        fig_price_trend.add_trace(go.Scatter(
            x=my_product_data['Tanggal'], 
            y=my_product_data['Harga'],
            mode='lines+markers',
            name=f'{my_store_name} (Produk Pilihan)'
        ))

        st.markdown("---")
        st.subheader("Perbandingan dengan Toko Kompetitor")

        # Fuzzy matching dengan kompetitor
        competitor_product_names = competitor_df['Nama Produk'].unique()
        matches = process.extract(selected_product, competitor_product_names, limit=5, scorer=fuzz.token_set_ratio)
        
        found_match = False
        for match_name, score in matches:
            if score >= accuracy_cutoff:
                found_match = True
                competitor_product_data = competitor_df[competitor_df['Nama Produk'] == match_name].sort_values('Tanggal', ascending=False)
                latest_competitor_data = competitor_product_data.iloc[0]
                
                price_diff = latest_competitor_data['Harga'] - my_latest_data['Harga']
                
                st.markdown(f"##### Ditemukan di: **{latest_competitor_data['Toko']}** (Kemiripan: {score}%)")
                st.info(f"Nama Produk: {match_name}")

                cc1, cc2, cc3, cc4 = st.columns(4)
                cc1.metric("Harga", format_rupiah(latest_competitor_data['Harga']))
                cc2.metric("Status", latest_competitor_data['Status'])
                cc3.metric("Stok", latest_competitor_data.get('Stok', 'N/A'))
                
                if price_diff < 0:
                    cc4.success(f"Lebih Murah (Selisih {format_rupiah(abs(price_diff))})")
                elif price_diff > 0:
                    cc4.error(f"Lebih Mahal (Selisih {format_rupiah(price_diff)})")
                else:
                    cc4.info("Harga Sama")

                # Tambahkan ke line chart
                fig_price_trend.add_trace(go.Scatter(
                    x=competitor_product_data['Tanggal'],
                    y=competitor_product_data['Harga'],
                    mode='lines+markers',
                    name=f"{latest_competitor_data['Toko']}"
                ))

        if not found_match:
            st.warning("Tidak ditemukan produk yang cukup mirip di toko kompetitor dengan tingkat akurasi yang dipilih.")
        
        fig_price_trend.update_layout(title="Tren Perubahan Harga Produk", xaxis_title="Tanggal", yaxis_title="Harga (Rp)")
        st.plotly_chart(fig_price_trend, use_container_width=True)

# ===================================================================================
# TAB 3: ANALISIS BRAND KOMPETITOR
# ===================================================================================
with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    
    competitor_list = competitor_df['Toko'].unique()
    selected_competitor = st.selectbox("Pilih Toko Kompetitor", competitor_list)

    if selected_competitor:
        competitor_latest_data = latest_entries_overall[latest_entries_overall['Toko'] == selected_competitor]
        
        brand_summary = competitor_latest_data.groupby('Brand').agg(
            Total_Omzet=('Omzet', 'sum'),
            Unit_Terjual=('Terjual/Bulan', 'sum')
        ).sort_values('Total_Omzet', ascending=False).reset_index()

        st.subheader(f"Kinerja Brand di {selected_competitor}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### Tabel Kinerja Brand")
            brand_summary_display = brand_summary.copy()
            brand_summary_display['Total_Omzet'] = brand_summary_display['Total_Omzet'].apply(format_rupiah)
            st.dataframe(brand_summary_display, height=400)

        with col2:
            st.markdown("##### Distribusi Omzet Brand")
            fig_comp_brand = px.pie(
                brand_summary.head(10),
                values='Total_Omzet',
                names='Brand',
                title=f'Kontribusi Omzet 10 Brand Teratas di {selected_competitor}',
                hole=0.3
            )
            fig_comp_brand.update_traces(textinfo='percent+label', texttemplate='%{label}<br>%{percent:.1%}', hovertemplate='<b>%{label}</b><br>Omzet: %{value:,.0f}<extra></extra>')
            st.plotly_chart(fig_comp_brand, use_container_width=True)

# ===================================================================================
# TAB 4: TREN STATUS STOK MINGGUAN
# ===================================================================================
with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    
    # Menghitung produk READY dan HABIS per minggu per toko
    stock_status_weekly = pd.crosstab(
        index=[df_filtered['Minggu'], df_filtered['Toko']],
        columns=df_filtered['Status']
    ).reset_index()
    
    # Memastikan kolom READY dan HABIS ada
    if 'READY' not in stock_status_weekly.columns:
        stock_status_weekly['READY'] = 0
    if 'HABIS' not in stock_status_weekly.columns:
        stock_status_weekly['HABIS'] = 0
        
    st.subheader("Tabel Status Stok Mingguan")
    st.dataframe(stock_status_weekly, use_container_width=True)
    
    st.subheader("Visualisasi Tren Status Stok")
    fig_stock_trend = px.line(
        stock_status_weekly,
        x='Minggu',
        y=['READY', 'HABIS'],
        color='Toko',
        facet_row='Toko',
        labels={'value': 'Jumlah Produk', 'variable': 'Status'},
        title='Perubahan Jumlah Produk Ready vs Habis per Minggu'
    )
    fig_stock_trend.update_layout(height=len(stock_status_weekly['Toko'].unique()) * 200)
    st.plotly_chart(fig_stock_trend, use_container_width=True)


# ===================================================================================
# TAB 5: OMZET LINTAS TOKO
# ===================================================================================
with tab5:
    st.header("Perbandingan Omzet Mingguan Lintas Toko")
    
    weekly_omzet_all_stores = df_filtered.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
    
    omzet_pivot = weekly_omzet_all_stores.pivot(index='Minggu', columns='Toko', values='Omzet').fillna(0)
    
    st.subheader("Tabel Omzet Mingguan (Geser untuk melihat semua toko)")
    st.dataframe(omzet_pivot.style.format(format_rupiah), use_container_width=True)
    
    st.subheader("Visualisasi Tren Omzet Mingguan")
    fig_omzet_trend = px.line(
        weekly_omzet_all_stores,
        x='Minggu',
        y='Omzet',
        color='Toko',
        title='Tren Omzet Mingguan per Toko',
        labels={'Omzet': 'Total Omzet Mingguan'}
    )
    st.plotly_chart(fig_omzet_trend, use_container_width=True)

# ===================================================================================
# TAB 6: ANALISIS PRODUK BARU
# ===================================================================================
with tab6:
    st.header("Analisis Produk Baru Mingguan")
    
    weeks = sorted(df_filtered['Minggu'].unique())
    
    col1, col2 = st.columns(2)
    with col1:
        comparison_week = st.selectbox("Pilih Minggu Pembanding", weeks, index=len(weeks)-2 if len(weeks)>1 else 0)
    with col2:
        target_week = st.selectbox("Pilih Minggu Target", weeks, index=len(weeks)-1 if len(weeks)>0 else 0)
        
    if comparison_week and target_week and comparison_week < target_week:
        products_comparison = set(df_filtered[df_filtered['Minggu'] == comparison_week]['Nama Produk'].unique())
        products_target = set(df_filtered[df_filtered['Minggu'] == target_week]['Nama Produk'].unique())
        
        new_products = products_target - products_comparison
        
        st.subheader(f"Produk Baru yang Muncul pada Minggu {target_week.strftime('%d-%m-%Y')}")
        st.info(f"Ditemukan **{len(new_products)}** produk baru dibandingkan dengan minggu {comparison_week.strftime('%d-%m-%Y')}.")
        
        if new_products:
            new_products_df = latest_entries_overall[latest_entries_overall['Nama Produk'].isin(new_products)]
            new_products_display = new_products_df[['Nama Produk', 'Harga', 'Stok', 'Brand', 'Toko']].copy()
            new_products_display['Harga'] = new_products_display['Harga'].apply(format_rupiah)
            st.dataframe(new_products_display, use_container_width=True)
    elif comparison_week >= target_week:
        st.warning("Minggu Target harus setelah Minggu Pembanding.")
    else:
        st.info("Pilih dua minggu yang berbeda untuk memulai perbandingan.")

