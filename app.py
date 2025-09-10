import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from fuzzywuzzy import process, fuzz
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import json

# --- KONFIGURASI AWAL ---
st.set_page_config(
    page_title="Dashboard Analisis E-commerce V2",
    page_icon="🚀",
    layout="wide"
)

# --- FUNGSI-FUNGSI UTAMA ---

def init_connection():
    """Menginisialisasi koneksi ke Google Sheets menggunakan st.secrets."""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Gagal menginisialisasi koneksi. Cek konfigurasi st.secrets. Detail: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner="Menarik data dari Google Sheets...")
def tarik_data_dari_gsheet(spreadsheet_id, sheet_names):
    """Menarik data dari beberapa sheet dalam satu spreadsheet."""
    try:
        client = init_connection()
        if client is None: return None, "Koneksi Gagal"
        spreadsheet = client.open_by_key(spreadsheet_id)
        data_frames = {}
        for name in sheet_names:
            worksheet = spreadsheet.worksheet(name)
            all_values = worksheet.get_all_values()
            if not all_values or not all_values[0]:
                df = pd.DataFrame()
            else:
                headers = all_values[0]
                data = all_values[1:]
                df = pd.DataFrame(data, columns=headers)

            df.rename(columns={'NAMA': 'NAMA', 'TERJUAL/BLN': 'Terjual/Bulan', 'BRAND': 'BRAND'}, inplace=True)
            for col in ['HARGA', 'Terjual/Bulan', 'Omzet']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce')
            if 'TANGGAL' in df.columns:
                df['TANGGAL'] = pd.to_datetime(df['TANGGAL'], errors='coerce', dayfirst=True)
            data_frames[name] = df
        return data_frames, None
    except Exception as e:
        error_message = f"Terjadi kesalahan saat menarik data: {e}"
        st.error(error_message)
        st.info("Pastikan kredensial Google Sheets benar dan service account memiliki akses 'Editor'.")
        return None, error_message

def preprocess_data(data_frames, start_date, end_date):
    """Membersihkan, memfilter, dan menghitung omzet."""
    processed_data = {}
    for name, df in data_frames.items():
        if df.empty or 'TANGGAL' not in df.columns:
            processed_data[name] = df
            continue
        df_filtered = df.dropna(subset=['TANGGAL'])
        df_filtered = df_filtered[(df_filtered['TANGGAL'] >= start_date) & (df_filtered['TANGGAL'] <= end_date)].copy()
        if 'Omzet' not in df_filtered.columns and 'HARGA' in df_filtered.columns and 'Terjual/Bulan' in df_filtered.columns:
            df_filtered['Omzet'] = df_filtered['HARGA'] * df_filtered['Terjual/Bulan']
        processed_data[name] = df_filtered
    return processed_data

@st.cache_data
def fuzzy_match_kategori(df_db, df_database, threshold=90):
    """Melakukan fuzzy matching untuk melabeli kategori produk."""
    if df_database.empty or 'NAMA' not in df_database.columns or 'Kategori' not in df_database.columns:
        df_db['Kategori'] = 'Tidak Terkategori'
        return df_db
    db_products = df_db['NAMA'].tolist()
    database_products = df_database['NAMA'].unique().tolist()
    kategori_map = pd.Series(df_database['Kategori'].values, index=df_database['NAMA']).to_dict()
    kategori_list = []
    for product in db_products:
        try:
            match, score = process.extractOne(product, database_products, scorer=fuzz.token_sort_ratio)
            if score >= threshold:
                kategori_list.append(kategori_map.get(match, 'Tidak Terkategori'))
            else:
                kategori_list.append('Tidak Terkategori')
        except:
            kategori_list.append('Tidak Terkategori')
    df_db['Kategori'] = kategori_list
    return df_db

def format_indicator(val):
    """Format nilai menjadi indikator panah berwarna."""
    if pd.isna(val) or val == np.inf or val == -np.inf: return "⚪ ▬"
    if val > 0: return f"🟢 ▲ {val:.1f}%"
    elif val < 0: return f"🔴 ▼ {abs(val):.1f}%"
    else: return "⚪ ▬"

# --- UI STREAMLIT ---

if 'analisis_dimulai' not in st.session_state: st.session_state.analisis_dimulai = False
if 'data_cache' not in st.session_state: st.session_state.data_cache = None
if 'error_log' not in st.session_state: st.session_state.error_log = None

if not st.session_state.analisis_dimulai:
    st.title("📊 Dashboard Analisis Kompetitor E-commerce V2")
    st.markdown("Selamat datang! Dashboard ini dirancang untuk menganalisis data penjualan dari berbagai toko dan kompetitor secara komprehensif.")
    if st.button("🚀 Tarik Data & Mulai Analisis"):
        GSHEET_ID = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
        SHEET_NAMES = ["DATABASE", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS", "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS", "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS", "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS", "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS", "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS", "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS", "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS", "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS"]
        data_frames, error = tarik_data_dari_gsheet(GSHEET_ID, SHEET_NAMES)
        if data_frames is not None:
            st.session_state.data_cache = data_frames
            st.session_state.analisis_dimulai = True
            st.session_state.error_log = None
            st.rerun()
        else:
            st.session_state.error_log = error
    if st.session_state.error_log: st.error(f"Gagal memulai analisis. {st.session_state.error_log}")
else:
    all_data = st.session_state.data_cache
    with st.sidebar:
        st.title("⚙️ Pengaturan Analisis")
        if st.button("🔄 Hapus Cache & Ulangi"):
            st.session_state.analisis_dimulai = False
            st.session_state.data_cache = None
            st.cache_data.clear()
            st.rerun()

        list_of_date_series = [df['TANGGAL'].dropna() for df in all_data.values() if 'TANGGAL' in df.columns and not df.empty and not df['TANGGAL'].dropna().empty]
        if not list_of_date_series:
            st.error("Gagal menemukan data TANGGAL yang valid.")
            st.stop()

        all_dates = pd.concat(list_of_date_series)
        min_date, max_date = all_dates.min().date(), all_dates.max().date()
        selected_date_range = st.date_input("Pilih Rentang Analisis", (min_date, max_date), min_value=min_date, max_value=max_date, help="Data akan difilter berdasarkan rentang tanggal ini.")
        start_date = datetime.combine(selected_date_range[0], datetime.min.time()) if len(selected_date_range) > 0 else datetime.now()
        end_date = datetime.combine(selected_date_range[1], datetime.max.time()) if len(selected_date_range) > 1 else datetime.now()
        
        fuzzy_threshold = st.slider("Akurasi Pencocokan Kategori (%)", 50, 100, 90, help="Tingkat kemiripan nama produk untuk penentuan kategori. Turunkan jika banyak produk tidak terkategori.")
        
    # --- PEMROSESAN DATA UTAMA ---
    processed_data = preprocess_data(all_data, start_date, end_date)
    store_list = sorted([name.replace(" - REKAP - READY", "") for name in processed_data if "READY" in name])
    
    all_stores_df_list = []
    for store in store_list:
        df_ready = processed_data.get(f"{store} - REKAP - READY", pd.DataFrame())
        df_habis = processed_data.get(f"{store} - REKAP - HABIS", pd.DataFrame())
        df_full = pd.concat([df_ready, df_habis])
        if not df_full.empty:
            df_full['Toko'] = store
            all_stores_df_list.append(df_full)
    
    if not all_stores_df_list:
        st.warning("Tidak ada data yang ditemukan pada rentang tanggal yang dipilih. Silakan sesuaikan filter di sidebar.")
        st.stop()

    all_stores_df = pd.concat(all_stores_df_list, ignore_index=True)
    all_stores_df.dropna(subset=['Omzet'], inplace=True)
    df_database = processed_data.get("DATABASE", pd.DataFrame())
    all_stores_df_kategori = fuzzy_match_kategori(all_stores_df.copy(), df_database, fuzzy_threshold)


    st.title("Dashboard Analisis E-commerce V2")
    tab1, tab2, tab3, tab4 = st.tabs(["-  Ringkasan Pasar", "🏢 Analisis Toko Tunggal", "⚖️ Perbandingan Toko", "📈 Analisis Pasar & Produk"])

    with tab1:
        st.header("Ringkasan Kinerja Pasar")
        st.markdown(f"Analisis dari **{start_date.strftime('%d %b %Y')}** hingga **{end_date.strftime('%d %b %Y')}**")
        
        market_omzet = all_stores_df_kategori['Omzet'].sum()
        market_share = all_stores_df_kategori.groupby('Toko')['Omzet'].sum().sort_values(ascending=False)
        market_leader = market_share.index[0] if not market_share.empty else "N/A"
        
        brand_share = all_stores_df_kategori.groupby('BRAND')['Omzet'].sum().sort_values(ascending=False)
        top_brand = brand_share.index[0] if not brand_share.empty else "N/A"
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Omzet Pasar", f"Rp{market_omzet:,.0f}")
        col2.metric("Pemimpin Pasar (Omzet)", market_leader)
        col3.metric("Brand Terkuat di Pasar", top_brand)

        st.markdown("---")
        
        col1, col2 = st.columns([1, 2])
        with col1:
            st.subheader("Pangsa Pasar per Toko")
            fig_market_share = px.pie(market_share.reset_index(), values='Omzet', names='Toko', hole=0.4)
            fig_market_share.update_traces(textposition='inside', textinfo='percent+label', hovertemplate='Toko: %{label}<br>Omzet: Rp%{value:,.0f}<extra></extra>')
            st.plotly_chart(fig_market_share, use_container_width=True)
            
        with col2:
            st.subheader(f"Top 10 Brand di Seluruh Pasar")
            fig_brand_share = px.bar(brand_share.head(10).reset_index(), x='Omzet', y='BRAND', orientation='h', text='Omzet')
            fig_brand_share.update_traces(texttemplate='Rp%{text:,.0f}', textposition='outside')
            fig_brand_share.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_tickprefix='Rp', xaxis_tickformat=',.0f', yaxis_title=None)
            st.plotly_chart(fig_brand_share, use_container_width=True)

    with tab2:
        st.header("Analisis Mendalam per Toko")
        selected_store = st.selectbox("Pilih Toko untuk dianalisis:", store_list, index=store_list.index("DB KLIK") if "DB KLIK" in store_list else 0)

        if selected_store:
            df_toko_terpilih = all_stores_df_kategori[all_stores_df_kategori['Toko'] == selected_store]
            
            if df_toko_terpilih.empty:
                st.warning(f"Tidak ada data untuk {selected_store} pada rentang tanggal ini.")
            else:
                st.subheader(f"Analisis Kategori Terlaris di {selected_store}")
                kategori_omzet = df_toko_terpilih.groupby('Kategori')['Omzet'].sum().sort_values(ascending=False).reset_index()
                fig_kategori = px.bar(kategori_omzet.head(10), x='Omzet', y='Kategori', orientation='h', title=f'Top 10 Kategori di {selected_store}', text='Omzet')
                fig_kategori.update_traces(texttemplate='Rp%{text:,.0f}', textposition='outside')
                fig_kategori.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_tickprefix='Rp', xaxis_tickformat=',.0f', yaxis_title=None)
                st.plotly_chart(fig_kategori, use_container_width=True)
                
                st.subheader(f"Produk Terlaris di {selected_store} (Berdasarkan Omzet Terakhir)")
                produk_terlaris = df_toko_terpilih.sort_values('TANGGAL').drop_duplicates('NAMA', keep='last').sort_values('Omzet', ascending=False)
                st.dataframe(produk_terlaris[['NAMA', 'HARGA', 'Terjual/Bulan', 'Omzet', 'Kategori']].head(20),
                    column_config={"HARGA": st.column_config.NumberColumn(format="Rp %d"), "Omzet": st.column_config.NumberColumn(format="Rp %d")},
                    use_container_width=True)

                st.subheader(f"Ringkasan Kinerja Mingguan di {selected_store}")
                df_toko_terpilih['Minggu'] = df_toko_terpilih['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
                kinerja_mingguan = df_toko_terpilih.groupby('Minggu').agg(Omzet=('Omzet', 'sum'), Unit_Terjual=('Terjual/Bulan', 'sum')).sort_index().reset_index()
                kinerja_mingguan['Pertumbuhan Omzet'] = ((kinerja_mingguan['Omzet'] - kinerja_mingguan['Omzet'].shift(1)) / kinerja_mingguan['Omzet'].shift(1).replace(0,np.nan)) * 100
                kinerja_mingguan['Indikator'] = kinerja_mingguan['Pertumbuhan Omzet'].apply(format_indicator)
                st.dataframe(kinerja_mingguan[['Minggu', 'Omzet', 'Unit_Terjual', 'Indikator']],
                    column_config={"Omzet": st.column_config.NumberColumn(format="Rp %d")}, use_container_width=True)

    with tab3:
        st.header("Perbandingan Kinerja Antar Toko")
        selected_stores_compare = st.multiselect("Pilih toko untuk dibandingkan:", store_list, default=store_list[:2] if len(store_list) > 1 else store_list)

        if len(selected_stores_compare) < 2:
            st.info("Pilih minimal 2 toko untuk memulai perbandingan.")
        else:
            df_perbandingan = all_stores_df_kategori[all_stores_df_kategori['Toko'].isin(selected_stores_compare)]
            
            st.subheader("Perbandingan Total Omzet")
            omzet_compare = df_perbandingan.groupby('Toko')['Omzet'].sum().sort_values(ascending=False).reset_index()
            fig_omzet_compare = px.bar(omzet_compare, x='Toko', y='Omzet', text='Omzet', title="Total Omzet per Toko")
            fig_omzet_compare.update_traces(texttemplate='Rp%{text:,.0f}', textposition='outside')
            fig_omzet_compare.update_layout(yaxis_tickprefix='Rp', yaxis_tickformat=',.0f')
            st.plotly_chart(fig_omzet_compare, use_container_width=True)

            st.subheader("Tren Omzet Mingguan")
            df_perbandingan['Minggu'] = df_perbandingan['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
            omzet_tren = df_perbandingan.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
            fig_omzet_tren = px.line(omzet_tren, x='Minggu', y='Omzet', color='Toko', title="Tren Omzet Mingguan per Toko", markers=True)
            fig_omzet_tren.update_layout(yaxis_tickprefix='Rp', yaxis_tickformat=',.0f', hovermode="x unified")
            fig_omzet_tren.update_traces(hovertemplate='<b>%{full_data.name}</b><br>Omzet: Rp%{y:,.0f}<extra></extra>')
            st.plotly_chart(fig_omzet_tren, use_container_width=True)

    with tab4:
        st.header("Analisis Pasar & Produk")
        
        st.subheader("Kategori Paling Menguntungkan di Pasar")
        market_kategori = all_stores_df_kategori.groupby('Kategori')['Omzet'].sum().sort_values(ascending=False).reset_index()
        fig_market_kategori = px.bar(market_kategori.head(15), x='Omzet', y='Kategori', orientation='h', title="Top 15 Kategori dengan Omzet Tertinggi di Pasar", text='Omzet')
        fig_market_kategori.update_traces(texttemplate='Rp%{text:,.0f}', textposition='outside')
        fig_market_kategori.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_tickprefix='Rp', xaxis_tickformat=',.0f', yaxis_title=None)
        st.plotly_chart(fig_market_kategori, use_container_width=True)

        st.markdown("---")
        st.subheader("Analisis Produk Baru Mingguan")
        all_stores_df['Minggu'] = all_stores_df['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
        unique_weeks = sorted(all_stores_df['Minggu'].unique(), reverse=True)

        if len(unique_weeks) < 2:
            st.warning("Data tidak cukup untuk perbandingan minggu. Dibutuhkan data dari minimal 2 minggu.")
        else:
            col1, col2 = st.columns(2)
            target_week = col1.selectbox("Pilih Minggu Target:", unique_weeks, index=0)
            comparison_week = col2.selectbox("Bandingkan dengan Minggu:", unique_weeks, index=min(1, len(unique_weeks)-1))

            if target_week and comparison_week and target_week != comparison_week:
                produk_target = set(all_stores_df[all_stores_df['Minggu'] == target_week]['NAMA'].unique())
                produk_pembanding = set(all_stores_df[all_stores_df['Minggu'] == comparison_week]['NAMA'].unique())
                produk_baru = list(produk_target - produk_pembanding)
                st.success(f"Ditemukan **{len(produk_baru)}** produk baru pada minggu {target_week.strftime('%d %b')} dibandingkan minggu {comparison_week.strftime('%d %b')}")
                if produk_baru:
                    df_produk_baru = all_stores_df[all_stores_df['NAMA'].isin(produk_baru)].sort_values('TANGGAL', ascending=False).drop_duplicates('NAMA', keep='first')
                    st.dataframe(df_produk_baru[['NAMA', 'HARGA', 'BRAND', 'Toko']], column_config={"HARGA": st.column_config.NumberColumn(format="Rp %d")}, use_container_width=True)
            elif target_week == comparison_week:
                st.warning("Minggu target dan pembanding tidak boleh sama.")

