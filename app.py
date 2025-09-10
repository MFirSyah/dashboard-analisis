# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi: Adaptasi Lanjutan
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
# FUNGSI-FUNGSI
# ===================================================================================
@st.cache_data(show_spinner="Mengambil data dari Google Sheets...")
def load_data_from_gsheets():
    """
    Memuat data dari Google Sheets. Fungsi ini mengambil data dari berbagai sheet,
    membersihkan, menormalkan, dan menggabungkannya menjadi DataFrame yang siap diolah.
    """
    try:
        # Menggunakan st.secrets untuk otentikasi ke Google Sheets
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key(st.secrets["gsheet_key"])
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: Pastikan st.secrets telah dikonfigurasi dengan benar. Detail: {e}")
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

    # Membaca setiap sheet
    for sheet_name in sheet_names:
        try:
            ws = spreadsheet.worksheet(sheet_name)
            values = ws.get_all_values()
            if not values or len(values) < 2: continue
            header = values[0]
            data = values[1:]
            df = pd.DataFrame(data, columns=header)
            df = df.loc[:, ~df.columns.str.strip().eq('')] # Hapus kolom kosong

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
            return None, None

    if not rekap_list:
        st.error('Tidak ada data REKAP yang valid di Google Sheets.')
        return None, None

    rekap_df = pd.concat(rekap_list, ignore_index=True)

    # Normalisasi nama kolom
    def norm_cols(df_to_norm):
        df_to_norm.columns = [str(c).strip().upper() for c in df_to_norm.columns]
        return df_to_norm

    database_df = norm_cols(database_df) if not database_df.empty else database_df
    rekap_df = norm_cols(rekap_df)

    # Pemetaan nama kolom ke format standar
    rename_map = {
        'NAMA': 'Nama Produk', 'NAMA PRODUK': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan',
        'TERJUAL': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga',
        'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'
    }
    rekap_df.rename(columns=lambda x: rename_map.get(x, x), inplace=True)
    if not database_df.empty:
        database_df.rename(columns={'NAMA':'Nama Produk'}, inplace=True)


    # Pembersihan dan konversi tipe data
    rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df.get('Terjual per Bulan', 0), errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)

    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Terjual per Bulan'] = rekap_df['Terjual per Bulan'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']

    # Standarisasi Brand menggunakan kamus (jika ada)
    if not kamus_brand_df.empty and all(c in kamus_brand_df.columns for c in ['ALIAS', 'BRAND_UTAMA']):
        alias_map = {str(k).strip().upper(): str(v).strip().upper() for k,v in kamus_brand_df.set_index('ALIAS')['BRAND_UTAMA'].to_dict().items()}
        rekap_df['Brand'] = rekap_df.get('Brand', pd.Series(dtype='str')).apply(lambda b: alias_map.get(str(b).strip().upper(), str(b).strip().upper()) if pd.notna(b) else 'UNKNOWN')
    elif 'Brand' not in rekap_df.columns:
         rekap_df['Brand'] = rekap_df['Nama Produk'].str.split().str[0].str.upper()


    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True, keep='last')
    return rekap_df.sort_values('Tanggal'), database_df

@st.cache_data(show_spinner="Mencocokkan kategori produk...")
def match_categories(product_df, db_df, accuracy_cutoff):
    """
    Mencocokkan produk dengan database untuk mendapatkan kategori menggunakan fuzzy matching.
    """
    if db_df.empty or 'Nama Produk' not in db_df.columns or 'KATEGORI' not in db_df.columns:
        product_df['Kategori'] = 'Tidak Diketahui'
        return product_df

    db_choices = db_df['Nama Produk'].tolist()
    db_map = db_df.set_index('Nama Produk')['KATEGORI'].to_dict()

    def get_category(name):
        match = process.extractOne(name, db_choices, scorer=fuzz.token_set_ratio, score_cutoff=accuracy_cutoff)
        return db_map[match[0]] if match else 'Lainnya'

    product_df['Kategori'] = product_df['Nama Produk'].apply(get_category)
    return product_df

def format_rupiah(value):
    """Format angka menjadi format mata uang Rupiah."""
    return f"Rp {value:,.0f}".replace(',', '.')

def format_wow_growth(pct):
    """Format persentase pertumbuhan mingguan dengan ikon panah."""
    if pd.isna(pct) or np.isinf(pct):
        return 'N/A'
    if pct > 0:
        return f'🟢 ▲ {pct:.1%}'
    if pct < 0:
        return f'🔴 ▼ {abs(pct):.1%}'
    return f'⚪ ▬ 0.0%'

@st.cache_data
def convert_df_for_download(df_to_convert, format_type='csv'):
    """Konversi DataFrame ke CSV atau JSON untuk diunduh."""
    if format_type == 'csv':
        return df_to_convert.to_csv(index=False).encode('utf-8')
    else: # json
        return df_to_convert.to_json(orient='records', indent=4).encode('utf-8')

# ===================================================================================
# UI - Tampilan Awal & Pemuatan Data
# ===================================================================================
st.title('📊 Dashboard Analisis Penjualan & Kompetitor')

# Inisialisasi session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

# Tampilkan tombol hanya jika data belum dimuat
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
            else:
                st.error("Gagal memuat data. Silakan periksa log di atas atau konfigurasi `secrets.toml` Anda.")
    st.info("Pastikan file `secrets.toml` sudah terkonfigurasi untuk mengakses Google Sheets.")
    st.stop()

# ===================================================================================
# UI - Sidebar & Filter (Setelah data dimuat)
# ===================================================================================
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
    selected_date_range = st.date_input(
        'Rentang Tanggal Analisis',
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    if len(selected_date_range) != 2:
        st.warning('Pilih rentang tanggal yang valid.'); st.stop()

    start_date, end_date = selected_date_range
    accuracy_cutoff = st.slider('Tingkat Akurasi Fuzzy Match (%)', 70, 100, 90, help="Tingkat kemiripan minimum untuk mencocokkan produk/kategori.")

# Filter data utama berdasarkan rentang tanggal
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error(f"Tidak ada data pada rentang tanggal {start_date} hingga {end_date}."); st.stop()

# Siapkan snapshot data: data terbaru per produk & data mingguan
df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-MON').apply(lambda p: p.start_time).dt.date
latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()].copy()
main_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == MY_STORE_NAME].copy()

# ===================================================================================
# UI - Tampilan Tab Utama
# ===================================================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    f"📈 Kinerja {MY_STORE_NAME}", "🆚 Perbandingan Produk", "🏢 Brand Kompetitor",
    "📦 Tren Stok", "💰 Omzet Toko", "🆕 Produk Baru"
])

# ===================================================================================
# TAB 1: KINERJA TOKO UTAMA (DB KLIK)
# ===================================================================================
with tab1:
    st.header(f"Analisis Kinerja Toko {MY_STORE_NAME}")

    # 1. ANALISIS KATEGORI TERLARIS
    st.subheader("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
    # Lakukan fuzzy matching untuk mendapatkan kategori
    main_categorized_df = match_categories(main_latest_overall.copy(), db_df, accuracy_cutoff)
    category_omzet = main_categorized_df.groupby('Kategori')['Omzet'].sum().sort_values(ascending=False).reset_index()

    col1, col2 = st.columns([1, 2])
    with col1:
        sort_order = st.radio("Urutkan Kategori:", ('Terlaris', 'Paling Tidak Laris'), horizontal=True)
        num_bars = st.slider('Jumlah Kategori untuk Ditampilkan', 3, len(category_omzet), 10)
        ascending = (sort_order == 'Paling Tidak Laris')
        
        category_omzet_sorted = category_omzet.sort_values('Omzet', ascending=ascending).head(num_bars)

    with col2:
        fig_cat = px.bar(
            category_omzet_sorted,
            x='Omzet', y='Kategori', orientation='h',
            title=f'Top {num_bars} Kategori Berdasarkan Omzet',
            text='Omzet',
            labels={'Omzet': 'Total Omzet (Rp)', 'Kategori': 'Kategori Produk'}
        )
        fig_cat.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig_cat.update_layout(yaxis={'categoryorder':'total ascending' if not ascending else 'total descending'}, uniformtext_minsize=8, uniformtext_mode='hide')
        st.plotly_chart(fig_cat, use_container_width=True)

    st.markdown("---")
    st.write("Produk Terlaris Berdasarkan Kategori")
    selected_category = st.selectbox(
        'Pilih Kategori untuk melihat produk terlaris',
        category_omzet['Kategori'].unique()
    )
    products_in_category = main_categorized_df[main_categorized_df['Kategori'] == selected_category]
    products_in_category_display = products_in_category.sort_values('Terjual per Bulan', ascending=False)
    products_in_category_display['Harga'] = products_in_category_display['Harga'].apply(format_rupiah)
    st.dataframe(products_in_category_display[['Nama Produk', 'Harga', 'Terjual per Bulan', 'Status']], use_container_width=True)

    # 2. PRODUK TERLARIS (GLOBAL)
    st.subheader("2. Produk Terlaris (Global)")
    # Hitung WoW growth
    weekly_sales = df_filtered[df_filtered['Toko'] == MY_STORE_NAME].copy()
    weekly_omzet_pivot = weekly_sales.pivot_table(index='Nama Produk', columns='Minggu', values='Omzet', aggfunc='sum')
    if len(weekly_omzet_pivot.columns) > 1:
        weekly_omzet_pivot['Pertumbuhan'] = weekly_omzet_pivot.iloc[:, -1].div(weekly_omzet_pivot.iloc[:, -2]) - 1
        growth_map = weekly_omzet_pivot['Pertumbuhan'].to_dict()
        main_latest_overall['Indikator'] = main_latest_overall['Nama Produk'].map(growth_map).apply(format_wow_growth)
    else:
        main_latest_overall['Indikator'] = 'N/A'

    top_products_global = main_latest_overall.sort_values('Omzet', ascending=False)
    top_products_global['Harga'] = top_products_global['Harga'].apply(format_rupiah)
    top_products_global['Omzet'] = top_products_global['Omzet'].apply(format_rupiah)
    st.dataframe(top_products_global[['Nama Produk', 'Harga', 'Terjual per Bulan', 'Omzet', 'Tanggal', 'Indikator']], use_container_width=True)

    # 3. DISTRIBUSI OMZET BRAND & 4. KINERJA MINGGUAN
    col_brand, col_wow = st.columns(2)
    with col_brand:
        st.subheader("3. Distribusi Omzet Brand")
        brand_omzet = main_latest_overall.groupby('Brand')['Omzet'].sum().sort_values(ascending=False)
        top_brands = brand_omzet.head(7)
        if len(brand_omzet) > 7:
            other_omzet = brand_omzet.iloc[7:].sum()
            top_brands['Lainnya'] = other_omzet
        
        fig_brand = px.pie(
            values=top_brands.values, names=top_brands.index, title=f'Distribusi Omzet Brand di {MY_STORE_NAME}',
            hole=0.3
        )
        fig_brand.update_traces(textinfo='percent+label', hovertemplate='Brand: %{label}<br>Omzet: Rp %{value:,.0f}<extra></extra>')
        st.plotly_chart(fig_brand, use_container_width=True)

    with col_wow:
        st.subheader("4. Ringkasan Kinerja Mingguan (WoW)")
        weekly_summary = df_filtered[df_filtered['Toko'] == MY_STORE_NAME].groupby('Minggu').agg(
            Omzet=('Omzet', 'sum'),
            Penjualan_Unit=('Nama Produk', 'count')
        ).reset_index().sort_values('Minggu')
        weekly_summary['Pertumbuhan Omzet'] = weekly_summary['Omzet'].pct_change()
        weekly_summary['Pertumbuhan Omzet'] = weekly_summary['Pertumbuhan Omzet'].apply(format_wow_growth)
        weekly_summary['Omzet'] = weekly_summary['Omzet'].apply(format_rupiah)
        st.dataframe(weekly_summary.rename(columns={'Penjualan_Unit':'Penjualan Unit'}), use_container_width=True)


# ===================================================================================
# TAB 2: PERBANDINGAN PRODUK
# ===================================================================================
with tab2:
    st.header("Perbandingan Harga & Stok Produk")
    
    product_list = main_latest_overall['Nama Produk'].unique()
    selected_product = st.selectbox(
        "Pilih Produk untuk Dibandingkan",
        options=product_list,
        index=None,
        placeholder="Ketik untuk mencari produk..."
    )

    if selected_product:
        # Data produk utama
        main_product_data = main_latest_overall[main_latest_overall['Nama Produk'] == selected_product].iloc[0]
        
        st.subheader(f"🔍 Analisis untuk: {selected_product}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Harga Terakhir", format_rupiah(main_product_data['Harga']))
            st.metric("Status", main_product_data['Status'])
            st.metric("Stok", main_product_data.get('Stok', 'Data tidak ditemukan'))
        
        with col2:
            # Line chart harga produk utama
            history_main = df_filtered[(df_filtered['Nama Produk'] == selected_product) & (df_filtered['Toko'] == MY_STORE_NAME)]
            fig_price_trend = go.Figure()
            fig_price_trend.add_trace(go.Scatter(x=history_main['Tanggal'], y=history_main['Harga'], mode='lines+markers', name=MY_STORE_NAME))

        st.write(f"**Mencari produk serupa di toko kompetitor...** (Akurasi > {accuracy_cutoff}%)")
        
        competitor_choices = latest_entries_overall[latest_entries_overall['Toko'] != MY_STORE_NAME]['Nama Produk'].unique().tolist()
        matches = process.extract(selected_product, competitor_choices, limit=5, scorer=fuzz.token_set_ratio, score_cutoff=accuracy_cutoff)

        if not matches:
            st.warning("Tidak ditemukan produk yang cukup mirip di toko kompetitor.")
        else:
            for match_name, score in matches:
                competitor_data = latest_entries_overall[latest_entries_overall['Nama Produk'] == match_name].iloc[0]
                price_diff = competitor_data['Harga'] - main_product_data['Harga']
                
                if price_diff < 0:
                    comparison_text = f"🟢 **Lebih Murah** (Selisih: {format_rupiah(abs(price_diff))})"
                elif price_diff > 0:
                    comparison_text = f"🔴 **Lebih Mahal** (Selisih: {format_rupiah(price_diff)})"
                else:
                    comparison_text = "🔵 **Harga Sama**"

                with st.expander(f"**{competitor_data['Toko']}**: {match_name} (Kemiripan: {score}%) - {comparison_text}"):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Harga", format_rupiah(competitor_data['Harga']))
                    c2.metric("Status", competitor_data['Status'])
                    c3.metric("Stok", competitor_data.get('Stok', 'Data tidak ditemukan'))
                
                # Tambahkan ke line chart
                history_competitor = df_filtered[(df_filtered['Nama Produk'] == match_name) & (df_filtered['Toko'] == competitor_data['Toko'])]
                fig_price_trend.add_trace(go.Scatter(x=history_competitor['Tanggal'], y=history_competitor['Harga'], mode='lines+markers', name=f"{competitor_data['Toko']}"))
        
        fig_price_trend.update_layout(title="Tren Perubahan Harga Produk", xaxis_title="Tanggal", yaxis_title="Harga (Rp)")
        st.plotly_chart(fig_price_trend, use_container_width=True)


# ===================================================================================
# TAB 3: ANALISIS BRAND KOMPETITOR
# ===================================================================================
with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    competitor_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] != MY_STORE_NAME].copy()
    
    if competitor_latest_overall.empty:
        st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
    else:
        brand_analysis = competitor_latest_overall.groupby('Brand').agg(
            Total_Omzet=('Omzet', 'sum'),
            Unit_Terjual=('Terjual per Bulan', 'sum')
        ).sort_values('Total_Omzet', ascending=False).reset_index()

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Peringkat Brand Kompetitor")
            brand_analysis_display = brand_analysis.copy()
            brand_analysis_display['Total_Omzet'] = brand_analysis_display['Total_Omzet'].apply(format_rupiah)
            st.dataframe(brand_analysis_display.rename(columns={'Total_Omzet': 'Total Omzet', 'Unit_Terjual': 'Unit Terjual'}), use_container_width=True)
        
        with col2:
            st.subheader("Distribusi Omzet Brand Kompetitor")
            top_brands_comp = brand_analysis.head(10)
            fig_brand_comp = px.pie(
                top_brands_comp, values='Total_Omzet', names='Brand',
                title='Top 10 Brand Kompetitor Berdasarkan Omzet', hole=0.3
            )
            fig_brand_comp.update_traces(textinfo='percent+label', hovertemplate='Brand: %{label}<br>Omzet: Rp %{value:,.0f}<extra></extra>')
            st.plotly_chart(fig_brand_comp, use_container_width=True)


# ===================================================================================
# TAB 4: TREN STATUS STOK MINGGUAN
# ===================================================================================
with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    
    stock_status_weekly = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
    stock_status_weekly = stock_status_weekly.melt(id_vars=['Minggu', 'Toko'], var_name='Status', value_name='Jumlah Produk')
    
    st.subheader("Tabel Status Stok")
    st.dataframe(stock_status_weekly.pivot_table(index=['Minggu', 'Toko'], columns='Status', values='Jumlah Produk').fillna(0).astype(int), use_container_width=True)

    st.subheader("Visualisasi Tren Stok")
    fig_stock = px.line(
        stock_status_weekly,
        x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Status',
        title='Perbandingan Jumlah Produk Ready vs Habis per Minggu',
        markers=True,
        labels={'Jumlah Produk': 'Jumlah Produk', 'Minggu': 'Minggu'}
    )
    st.plotly_chart(fig_stock, use_container_width=True)


# ===================================================================================
# TAB 5: PERBANDINGAN OMZET ANTAR TOKO
# ===================================================================================
with tab5:
    st.header("Perbandingan Omzet Total Antar Toko")
    
    omzet_per_toko_daily = df_filtered.groupby(['Tanggal', 'Toko'])['Omzet'].sum().reset_index()
    
    st.subheader("Tabel Pivot Omzet Harian per Toko")
    omzet_pivot = omzet_per_toko_daily.pivot_table(index='Tanggal', columns='Toko', values='Omzet').fillna(0)
    omzet_pivot = omzet_pivot.applymap(lambda x: f"{x:,.0f}")
    st.dataframe(omzet_pivot, use_container_width=True)
    
    st.subheader("Visualisasi Tren Omzet Toko")
    fig_omzet_toko = px.line(
        omzet_per_toko_daily,
        x='Tanggal', y='Omzet', color='Toko',
        title='Tren Omzet Harian Antar Toko',
        labels={'Omzet': 'Total Omzet (Rp)', 'Tanggal': 'Tanggal'}
    )
    st.plotly_chart(fig_omzet_toko, use_container_width=True)


# ===================================================================================
# TAB 6: ANALISIS PRODUK BARU MINGGUAN
# ===================================================================================
with tab6:
    st.header("Analisis Produk Baru Mingguan")
    
    unique_weeks = sorted(df_filtered['Minggu'].unique(), reverse=True)
    
    if len(unique_weeks) < 2:
        st.warning("Data tidak cukup untuk perbandingan mingguan (kurang dari 2 minggu data).")
    else:
        col1, col2 = st.columns(2)
        with col1:
            target_week = st.selectbox("Pilih Minggu Target", options=unique_weeks, index=0)
        with col2:
            comparison_options = [w for w in unique_weeks if w < target_week]
            if not comparison_options:
                st.warning("Tidak ada minggu sebelumnya untuk dijadikan pembanding.")
                st.stop()
            comparison_week = st.selectbox("Pilih Minggu Pembanding", options=comparison_options, index=0)
        
        st.info(f"Menampilkan produk yang ada di minggu **{target_week.strftime('%d %b %Y')}** tapi TIDAK ADA di minggu **{comparison_week.strftime('%d %b %Y')}**.")
        
        # Ambil produk unik untuk setiap minggu dari toko utama
        products_target = set(df_filtered[(df_filtered['Minggu'] == target_week) & (df_filtered['Toko'] == MY_STORE_NAME)]['Nama Produk'].unique())
        products_comparison = set(df_filtered[(df_filtered['Minggu'] == comparison_week) & (df_filtered['Toko'] == MY_STORE_NAME)]['Nama Produk'].unique())
        
        new_products = list(products_target - products_comparison)
        
        if not new_products:
            st.success("Tidak ada produk baru yang terdeteksi pada minggu target.")
        else:
            new_products_df = latest_entries_overall[
                (latest_entries_overall['Nama Produk'].isin(new_products)) &
                (latest_entries_overall['Toko'] == MY_STORE_NAME)
            ].copy()
            
            new_products_df['Harga'] = new_products_df['Harga'].apply(format_rupiah)
            st.dataframe(new_products_df[['Nama Produk', 'Harga', 'Stok', 'Brand']], use_container_width=True)
            st.metric("Jumlah Produk Baru Ditemukan", len(new_products_df))


# ===================================================================================
# UI - Sidebar Bawah (Info & Unduh)
# ===================================================================================
with st.sidebar:
    st.divider()
    st.header('📄 Ekspor & Info Data')
    st.info(f"Total baris data yang diolah: **{len(df_filtered):,}**")
    
    st.download_button(
        '📥 Unduh Data (CSV)',
        data=convert_df_for_download(df_filtered, 'csv'),
        file_name=f'analisis_{start_date}_{end_date}.csv',
        mime='text/csv',
        use_container_width=True
    )
    st.download_button(
        '📥 Unduh Data (JSON)',
        data=convert_df_for_download(df_filtered, 'json'),
        file_name=f'analisis_{start_date}_{end_date}.json',
        mime='application/json',
        use_container_width=True
    )
