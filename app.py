import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from fuzzywuzzy import fuzz, process
import plotly.express as px
import json
from datetime import datetime, timedelta
import numpy as np

# Konfigurasi Halaman Streamlit
st.set_page_config(
    page_title="Dashboard Analisis E-commerce",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- FUNGSI OTENTIKASI DAN PENGAMBILAN DATA ---

@st.cache_resource(ttl=600)
def get_gsheet_client():
    """Menghubungkan ke Google Sheets menggunakan kredensial dari st.secrets."""
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
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
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

@st.cache_data(ttl=600)
def load_all_data(_client, sheet_id, sheet_names):
    """Memuat data dari semua sheet yang ditentukan dan menggabungkannya."""
    spreadsheet = _client.open_by_key(sheet_id)
    all_data = {}
    for name in sheet_names:
        try:
            worksheet = spreadsheet.worksheet(name)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            # Konversi kolom tanggal
            if 'Tanggal' in df.columns:
                df['Tanggal'] = pd.to_datetime(df['Tanggal'], errors='coerce')
            # Membersihkan dan konversi kolom numerik
            numeric_cols = ['Harga', 'Terjual/Bulan', 'Stok']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
            df.dropna(subset=['Tanggal'], inplace=True)
            df['Toko'] = name.split(' - ')[0]
            all_data[name] = df
        except gspread.WorksheetNotFound:
            st.warning(f"Sheet '{name}' tidak ditemukan.")
        except Exception as e:
            st.error(f"Gagal memuat data dari sheet '{name}': {e}")
    return all_data

# --- FUNGSI PEMROSESAN DAN ANALISIS DATA ---

def preprocess_and_calculate_metrics(data, date_range):
    """Melakukan pra-pemrosesan data dan menghitung metrik penting."""
    processed_data = {}
    for name, df in data.items():
        if not df.empty and 'Tanggal' in df.columns:
            # Filter berdasarkan rentang tanggal
            mask = (df['Tanggal'] >= date_range[0]) & (df['Tanggal'] <= date_range[1])
            df_filtered = df.loc[mask].copy()
            
            if not df_filtered.empty:
                # Hitung Omzet
                df_filtered['Omzet'] = df_filtered['Harga'].fillna(0) * df_filtered['Terjual/Bulan'].fillna(0)
                
                # Ekstrak Brand
                df_filtered['Brand'] = df_filtered['Nama Produk'].apply(lambda x: str(x).split(' ')[0] if isinstance(x, str) else 'UNKNOWN')
                
                # Tambahkan kolom Minggu
                df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W').apply(lambda r: r.start_time).dt.date

                processed_data[name] = df_filtered

    return processed_data

def fuzzy_merge_categories(df_db_klik, df_database, threshold=85):
    """Melakukan fuzzy matching untuk melabeli kategori produk."""
    db_klik_products = df_db_klik['Nama Produk'].dropna().unique()
    database_products = df_database[['Nama Produk', 'Kategori']].dropna()
    
    product_map = {}
    
    # Buat dictionary untuk mapping cepat
    database_dict = pd.Series(database_products.Kategori.values, index=database_products['Nama Produk']).to_dict()

    for product in db_klik_products:
        # Cari kecocokan terbaik
        match, score = process.extractOne(product, database_dict.keys(), scorer=fuzz.token_sort_ratio)
        if score >= threshold:
            product_map[product] = database_dict[match]
        else:
            product_map[product] = 'Lainnya'
            
    return df_db_klik['Nama Produk'].map(product_map)

# --- INISIALISASI STATE ---

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

# --- UI UTAMA ---

st.title("📊 Dashboard Analisis Kompetitor E-commerce")
st.markdown("Selamat datang di dashboard analisis. Silakan klik tombol di bawah untuk memulai.")

# Tombol untuk memulai analisis
if not st.session_state.data_loaded:
    if st.button("🚀 Tarik Data & Mulai Analisis", type="primary"):
        with st.spinner("Menghubungkan ke Google Sheets dan menarik data... Ini mungkin memakan waktu beberapa saat."):
            try:
                g_client = get_gsheet_client()
                sheet_id = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
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
                raw_data = load_all_data(g_client, sheet_id, sheet_names)
                st.session_state.raw_data = raw_data
                st.session_state.data_loaded = True
                st.rerun() # Rerun untuk menampilkan sidebar dan tab
            except Exception as e:
                st.error(f"Terjadi kesalahan saat menarik data: {e}")
                st.error("Pastikan Anda telah mengatur kredensial Google Sheets dengan benar di st.secrets.")

if st.session_state.data_loaded:
    # --- SIDEBAR ---
    st.sidebar.header("⚙️ Pengaturan Analisis")

    if st.sidebar.button("Hapus Cache & Mulai Ulang Analisis"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.session_state.data_loaded = False
        st.rerun()

    # Gabungkan semua tanggal dari semua dataframe untuk menentukan rentang
    all_dates = pd.concat([df['Tanggal'] for df in st.session_state.raw_data.values() if 'Tanggal' in df.columns and not df.empty])
    min_date = all_dates.min().date()
    max_date = all_dates.max().date()
    
    selected_date_range = st.sidebar.date_input(
        "Pilih Rentang Tanggal Analisis",
        value=(max_date - timedelta(days=30), max_date),
        min_value=min_date,
        max_value=max_date,
        key="date_range_selector"
    )
    
    # Pastikan rentang tanggal valid
    if len(selected_date_range) == 2:
        start_date = pd.to_datetime(selected_date_range[0])
        end_date = pd.to_datetime(selected_date_range[1])
        
        # Proses data berdasarkan rentang tanggal yang dipilih
        processed_data = preprocess_and_calculate_metrics(st.session_state.raw_data, (start_date, end_date))
        st.session_state.processed_data = processed_data
        
        fuzzy_threshold = st.sidebar.slider(
            "Tingkat Akurasi Fuzzy Match (%)", 
            min_value=0, max_value=100, value=85,
            help="Menentukan seberapa mirip nama produk harus untuk dianggap sama."
        )

        total_rows = sum(len(df) for df in processed_data.values())
        st.sidebar.info(f"📈 Total baris data yang diolah: **{total_rows:,}**")

        # Tombol Unduh
        st.sidebar.subheader("Unduh Data Olahan")
        
        # Gabungkan semua data yang telah diproses untuk diunduh
        downloadable_df = pd.concat(processed_data.values(), ignore_index=True)
        
        csv_data = downloadable_df.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="📥 Unduh sebagai CSV",
            data=csv_data,
            file_name=f"analisis_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

        json_data = downloadable_df.to_json(orient='records', date_format='iso')
        st.sidebar.download_button(
            label="📥 Unduh sebagai JSON",
            data=json_data,
            file_name=f"analisis_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.json",
            mime="application/json",
        )

    else:
        st.warning("Silakan pilih rentang tanggal yang valid di sidebar.")
        st.stop()
        
    # --- TABS ---
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Analisis DB KLIK", 
        "🆚 Perbandingan Produk", 
        "🏢 Analisis Brand Kompetitor",
        "📈 Tren Stok",
        "💰 Omzet Semua Toko",
        "🆕 Produk Baru Mingguan"
    ])
    
    # --- TAB 1: ANALISIS DB KLIK ---
    with tab1:
        st.header("Analisis Kinerja Toko DB KLIK")
        
        df_db_klik_ready = st.session_state.processed_data.get("DB KLIK - REKAP - READY", pd.DataFrame())
        df_db_klik_habis = st.session_state.processed_data.get("DB KLIK - REKAP - HABIS", pd.DataFrame())
        df_database = st.session_state.raw_data.get("DATABASE", pd.DataFrame())

        if not df_db_klik_ready.empty and not df_database.empty:
            with st.spinner("Melakukan fuzzy matching kategori..."):
                df_db_klik_ready['Kategori'] = fuzzy_merge_categories(df_db_klik_ready, df_database, fuzzy_threshold)
            
            # 1. Analisis Kategori Terlaris (Berdasarkan Omzet)
            st.subheader("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
            kategori_omzet = df_db_klik_ready.groupby('Kategori')['Omzet'].sum().sort_values(ascending=False).reset_index()

            col1, col2 = st.columns([1,3])
            with col1:
                sort_order = st.radio("Urutkan berdasarkan", ('Terlaris', 'Paling Tidak Laris'), key="cat_sort")
                num_bars = st.slider("Jumlah Kategori untuk Ditampilkan", 1, len(kategori_omzet), min(10, len(kategori_omzet)))

            if sort_order == 'Paling Tidak Laris':
                kategori_omzet = kategori_omzet.sort_values('Omzet', ascending=True)

            fig_kategori = px.bar(
                kategori_omzet.head(num_bars),
                x='Omzet',
                y='Kategori',
                orientation='h',
                title=f'{num_bars} Kategori {sort_order} Berdasarkan Omzet',
                text='Omzet'
            )
            fig_kategori.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title="Total Omzet (Rp)", yaxis_title="Kategori")
            fig_kategori.update_traces(texttemplate='Rp%{text:,.0f}', textposition='outside')
            with col2:
                st.plotly_chart(fig_kategori, use_container_width=True)

            st.markdown("---")
            st.write("Produk Terlaris per Kategori")
            selected_kategori = st.selectbox("Pilih Kategori untuk melihat produk", options=kategori_omzet['Kategori'].unique())
            
            produk_terlaris_kategori = df_db_klik_ready[df_db_klik_ready['Kategori'] == selected_kategori].copy()
            produk_terlaris_kategori = produk_terlaris_kategori.sort_values('Omzet', ascending=False)
            
            # Dapatkan data terbaru untuk setiap produk
            produk_terbaru = produk_terlaris_kategori.loc[produk_terlaris_kategori.groupby('Nama Produk')['Tanggal'].idxmax()]
            produk_terbaru['Status'] = 'READY'
            
            st.dataframe(produk_terbaru[['Nama Produk', 'Harga', 'Terjual/Bulan', 'Status']].head(10), use_container_width=True)

            # 2. Produk Terlaris
            st.subheader("2. Produk Terlaris (Global)")
            produk_terlaris_global = df_db_klik_ready.sort_values('Omzet', ascending=False)
            produk_terlaris_global_terbaru = produk_terlaris_global.loc[produk_terlaris_global.groupby('Nama Produk')['Tanggal'].idxmax()].copy()
            
            # Menghitung pertumbuhan WoW
            produk_terlaris_global_terbaru['Omzet Minggu Lalu'] = produk_terlaris_global_terbaru.apply(
                lambda row: df_db_klik_ready[
                    (df_db_klik_ready['Nama Produk'] == row['Nama Produk']) &
                    (df_db_klik_ready['Tanggal'] < row['Tanggal']) &
                    (df_db_klik_ready['Tanggal'] >= row['Tanggal'] - timedelta(days=7))
                ]['Omzet'].sum(),
                axis=1
            )
            
            produk_terlaris_global_terbaru['Pertumbuhan'] = ((produk_terlaris_global_terbaru['Omzet'] - produk_terlaris_global_terbaru['Omzet Minggu Lalu']) / produk_terlaris_global_terbaru['Omzet Minggu Lalu'].replace(0, np.nan)) * 100
            
            def get_indicator(value):
                if pd.isna(value) or value == 0:
                    return "⚪ 0%"
                elif value > 0:
                    return f"🟢 ▲ {value:.2f}%"
                else:
                    return f"🔴 ▼ {abs(value):.2f}%"

            produk_terlaris_global_terbaru['Indikator'] = produk_terlaris_global_terbaru['Pertumbuhan'].apply(get_indicator)
            
            st.dataframe(
                produk_terlaris_global_terbaru[['Nama Produk', 'Harga', 'Terjual/Bulan', 'Omzet', 'Tanggal', 'Indikator']].head(15),
                use_container_width=True
            )

            # 3. Distribusi Omzet Brand
            st.subheader("3. Distribusi Omzet Brand")
            brand_omzet = df_db_klik_ready.groupby('Brand')['Omzet'].sum().sort_values(ascending=False).reset_index()
            fig_brand = px.pie(
                brand_omzet.head(10), 
                values='Omzet', 
                names='Brand', 
                title='Top 10 Kontribusi Brand terhadap Omzet',
                hole=.3
            )
            fig_brand.update_traces(textinfo='percent+label', pull=[0.1] + [0]*9)
            st.plotly_chart(fig_brand, use_container_width=True)

            # 4. Ringkasan Kinerja Mingguan (WoW Growth)
            st.subheader("4. Ringkasan Kinerja Mingguan (WoW Growth)")
            df_db_klik_all = pd.concat([df_db_klik_ready, df_db_klik_habis])
            kinerja_mingguan = df_db_klik_all.groupby('Minggu').agg(
                Omzet=('Omzet', 'sum'),
                Penjualan_Unit=('Terjual/Bulan', 'sum') # Perkiraan
            ).reset_index().sort_values('Minggu')

            kinerja_mingguan['Omzet Sebelumnya'] = kinerja_mingguan['Omzet'].shift(1)
            kinerja_mingguan['Pertumbuhan Omzet'] = ((kinerja_mingguan['Omzet'] - kinerja_mingguan['Omzet Sebelumnya']) / kinerja_mingguan['Omzet Sebelumnya'].replace(0, np.nan)) * 100
            kinerja_mingguan['Indikator'] = kinerja_mingguan['Pertumbuhan Omzet'].apply(get_indicator)
            
            st.dataframe(
                kinerja_mingguan[['Minggu', 'Omzet', 'Penjualan_Unit', 'Indikator']],
                use_container_width=True
            )
        else:
            st.warning("Data untuk DB KLIK atau DATABASE tidak ditemukan dalam rentang tanggal yang dipilih.")
            
    # --- TAB 2: PERBANDINGAN PRODUK ---
    with tab2:
        st.header("Perbandingan Harga dan Ketersediaan Produk")
        
        df_db_klik_ready = st.session_state.processed_data.get("DB KLIK - REKAP - READY")
        if df_db_klik_ready is not None and not df_db_klik_ready.empty:
            produk_unik_db_klik = df_db_klik_ready['Nama Produk'].unique()
            selected_product = st.selectbox("Pilih Produk dari DB KLIK untuk Dibandingkan", options=produk_unik_db_klik)

            if selected_product:
                # Informasi produk dari DB KLIK
                st.subheader(f"🔍 Analisis untuk: {selected_product}")
                produk_db_klik_df = df_db_klik_ready[df_db_klik_ready['Nama Produk'] == selected_product].sort_values('Tanggal', ascending=False)
                info_terbaru_db_klik = produk_db_klik_df.iloc[0]

                col1, col2, col3 = st.columns(3)
                col1.metric("Harga Terbaru", f"Rp{info_terbaru_db_klik['Harga']:,.0f}")
                col2.metric("Status", "READY")
                col3.metric("Stok", "Data tidak ditemukan") # Sesuai permintaan

                # Line chart untuk DB KLIK
                fig_db_klik = px.line(produk_db_klik_df, x='Tanggal', y='Harga', title=f"Perubahan Harga di DB KLIK", markers=True)
                fig_db_klik.update_layout(xaxis_title="Tanggal", yaxis_title="Harga (Rp)")
                st.plotly_chart(fig_db_klik, use_container_width=True)

                st.markdown("---")
                st.subheader("Perbandingan dengan Kompetitor")

                kompetitor_dfs = {
                    name: df for name, df in st.session_state.processed_data.items() 
                    if "DB KLIK" not in name and not df.empty
                }

                found_matches = False
                for toko, df_kompetitor in kompetitor_dfs.items():
                    # Fuzzy match
                    produk_kompetitor = df_kompetitor['Nama Produk'].dropna().unique()
                    matches = process.extract(selected_product, produk_kompetitor, limit=1, scorer=fuzz.token_sort_ratio)
                    
                    if matches and matches[0][1] >= fuzzy_threshold:
                        found_matches = True
                        match_name, score = matches[0]
                        
                        st.markdown(f"##### Ditemukan di: **{toko.split(' - ')[0]}** (Kemiripan: {score}%)")
                        
                        produk_kompetitor_df = df_kompetitor[df_kompetitor['Nama Produk'] == match_name].sort_values('Tanggal', ascending=False)
                        info_terbaru_kompetitor = produk_kompetitor_df.iloc[0]

                        harga_kompetitor = info_terbaru_kompetitor['Harga']
                        selisih = info_terbaru_db_klik['Harga'] - harga_kompetitor
                        
                        status_harga = ""
                        if selisih > 0:
                            status_harga = f"🟢 **Lebih murah** (Selisih Rp{abs(selisih):,.0f})"
                        elif selisih < 0:
                            status_harga = f"🔴 **Lebih mahal** (Selisih Rp{abs(selisih):,.0f})"
                        else:
                            status_harga = "🔵 **Harga Sama**"

                        st.markdown(status_harga)
                        kcol1, kcol2, kcol3 = st.columns(3)
                        kcol1.metric(f"Harga di {toko.split(' - ')[0]}", f"Rp{harga_kompetitor:,.0f}")
                        kcol2.metric("Status", toko.split(' - ')[-1].strip())
                        kcol3.metric("Stok", "Data tidak ditemukan")
                        
                        # Gabungkan data untuk line chart perbandingan
                        produk_db_klik_df['Toko'] = 'DB KLIK'
                        produk_kompetitor_df['Toko'] = toko.split(' - ')[0]
                        combined_df = pd.concat([produk_db_klik_df, produk_kompetitor_df])

                        fig_compare = px.line(combined_df, x='Tanggal', y='Harga', color='Toko', title=f"Perbandingan Perubahan Harga", markers=True)
                        st.plotly_chart(fig_compare, use_container_width=True)
                        st.markdown("---")
                
                if not found_matches:
                    st.info("Tidak ditemukan produk yang cukup mirip di toko kompetitor lain.")

        else:
            st.warning("Data DB KLIK tidak tersedia untuk perbandingan.")

    # --- TAB 3: ANALISIS BRAND KOMPETITOR ---
    with tab3:
        st.header("Analisis Brand di Toko Kompetitor")
        
        all_ready_dfs = [df for name, df in st.session_state.processed_data.items() if 'READY' in name and not df.empty]
        if all_ready_dfs:
            df_all_ready = pd.concat(all_ready_dfs, ignore_index=True)
            
            brand_analysis = df_all_ready.groupby(['Toko', 'Brand']).agg(
                Total_Omzet=('Omzet', 'sum'),
                Unit_Terjual=('Terjual/Bulan', 'sum')
            ).reset_index().sort_values('Total_Omzet', ascending=False)

            st.subheader("Tabel Analisis Brand per Toko")
            st.dataframe(brand_analysis, use_container_width=True)
            
            st.subheader("Visualisasi Omzet Brand per Toko")
            selected_toko_brand = st.selectbox("Pilih Toko untuk Visualisasi", options=brand_analysis['Toko'].unique())
            
            if selected_toko_brand:
                toko_brand_data = brand_analysis[brand_analysis['Toko'] == selected_toko_brand]
                fig_brand_toko = px.pie(
                    toko_brand_data.head(10),
                    values='Total_Omzet',
                    names='Brand',
                    title=f'Top 10 Kontribusi Brand di {selected_toko_brand}',
                    hole=.3
                )
                fig_brand_toko.update_traces(textinfo='percent+label', pull=[0.1] + [0]*9)
                st.plotly_chart(fig_brand_toko, use_container_width=True)
        else:
            st.warning("Tidak ada data 'READY' yang tersedia untuk analisis brand kompetitor.")

    # --- TAB 4: TREN STATUS STOK ---
    with tab4:
        st.header("Tren Status Stok Mingguan per Toko")

        all_dfs = list(st.session_state.processed_data.values())
        if all_dfs:
            df_full = pd.concat(all_dfs, ignore_index=True)
            df_full['Status'] = df_full.apply(lambda row: 'READY' if 'READY' in row['Toko'] else 'HABIS', axis=1)
            df_full['Toko'] = df_full['Toko'].apply(lambda x: x.split(' - ')[0])

            stok_tren = df_full.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
            stok_tren = stok_tren.rename(columns={'READY': 'Ready', 'HABIS': 'Habis'})
            stok_tren = stok_tren.sort_values(['Toko', 'Minggu'])
            
            st.subheader("Tabel Perhitungan Produk Ready dan Habis")
            st.dataframe(stok_tren, use_container_width=True)

            st.subheader("Visualisasi Tren Stok")
            selected_toko_stok = st.multiselect("Pilih Toko untuk Ditampilkan di Grafik", options=stok_tren['Toko'].unique(), default=stok_tren['Toko'].unique()[:3])
            
            if selected_toko_stok:
                plot_data = stok_tren[stok_tren['Toko'].isin(selected_toko_stok)]
                plot_data_melted = plot_data.melt(id_vars=['Minggu', 'Toko'], value_vars=['Ready', 'Habis'], var_name='Status', value_name='Jumlah Produk')

                fig_stok = px.line(
                    plot_data_melted,
                    x='Minggu',
                    y='Jumlah Produk',
                    color='Toko',
                    line_dash='Status',
                    title='Tren Jumlah Produk Ready vs Habis per Minggu',
                    markers=True
                )
                st.plotly_chart(fig_stok, use_container_width=True)
        else:
            st.warning("Tidak ada data yang tersedia untuk analisis tren stok.")
            
    # --- TAB 5: OMZET SEMUA TOKO ---
    with tab5:
        st.header("Perbandingan Omzet Harian Semua Toko")
        
        all_ready_dfs = [df for name, df in st.session_state.processed_data.items() if 'READY' in name and not df.empty]
        if all_ready_dfs:
            df_all_ready = pd.concat(all_ready_dfs, ignore_index=True)
            df_all_ready['Tanggal'] = df_all_ready['Tanggal'].dt.date

            omzet_pivot = df_all_ready.pivot_table(index='Tanggal', columns='Toko', values='Omzet', aggfunc='sum').fillna(0)
            
            st.subheader("Tabel Omzet Harian")
            st.dataframe(omzet_pivot)

            st.subheader("Visualisasi Omzet Harian")
            omzet_pivot_reset = omzet_pivot.reset_index()
            omzet_melted = omzet_pivot_reset.melt(id_vars='Tanggal', var_name='Toko', value_name='Omzet')
            
            fig_omzet_all = px.line(
                omzet_melted,
                x='Tanggal',
                y='Omzet',
                color='Toko',
                title='Tren Omzet Harian Semua Toko'
            )
            st.plotly_chart(fig_omzet_all, use_container_width=True)
        else:
            st.warning("Tidak ada data omzet yang tersedia untuk ditampilkan.")
            
    # --- TAB 6: ANALISIS PRODUK BARU MINGGUAN ---
    with tab6:
        st.header("Analisis Penambahan Produk Baru Mingguan")
        
        all_dfs = list(st.session_state.processed_data.values())
        if all_dfs:
            df_full = pd.concat(all_dfs, ignore_index=True)
            minggu_unik = sorted(df_full['Minggu'].unique(), reverse=True)
            
            if len(minggu_unik) >= 2:
                col1, col2 = st.columns(2)
                with col1:
                    minggu_target = st.selectbox("Pilih Minggu Target", options=minggu_unik, index=0)
                with col2:
                    minggu_pembanding = st.selectbox("Pilih Minggu Pembanding", options=minggu_unik, index=1)
                
                if minggu_target and minggu_pembanding and minggu_target != minggu_pembanding:
                    produk_target = set(df_full[df_full['Minggu'] == minggu_target]['Nama Produk'].unique())
                    produk_pembanding = set(df_full[df_full['Minggu'] == minggu_pembanding]['Nama Produk'].unique())
                    
                    produk_baru = list(produk_target - produk_pembanding)
                    
                    st.subheader(f"Ditemukan {len(produk_baru)} Produk Baru di Minggu {minggu_target} (Dibandingkan dengan {minggu_pembanding})")
                    
                    if produk_baru:
                        df_produk_baru = df_full[
                            (df_full['Minggu'] == minggu_target) & 
                            (df_full['Nama Produk'].isin(produk_baru))
                        ].copy()

                        # Ambil data entri terakhir untuk setiap produk baru
                        df_produk_baru_terbaru = df_produk_baru.loc[df_produk_baru.groupby('Nama Produk')['Tanggal'].idxmax()]
                        
                        st.dataframe(
                            df_produk_baru_terbaru[['Toko', 'Nama Produk', 'Harga', 'Brand']],
                            use_container_width=True
                        )
                    else:
                        st.success("Tidak ada penambahan produk baru pada minggu target.")

                else:
                    st.warning("Silakan pilih minggu target dan pembanding yang berbeda.")
            else:
                st.info("Data tidak cukup untuk melakukan perbandingan antar minggu.")
        else:
            st.warning("Tidak ada data yang tersedia untuk analisis produk baru.")
