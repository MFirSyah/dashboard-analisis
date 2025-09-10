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
    page_title="Dashboard Analisis E-commerce",
    page_icon="📊",
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
        if client is None:
            return None, "Koneksi Gagal"
            
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
                
                processed_records = []
                for row in data:
                    record = {}
                    for i, header in enumerate(headers):
                        if header:
                            try:
                                record[header] = row[i]
                            except IndexError:
                                record[header] = None
                    if record:
                        processed_records.append(record)
                
                if processed_records:
                    df = pd.DataFrame(processed_records)
                else:
                    df = pd.DataFrame(columns=[h for h in headers if h])

            # --- PERBAIKAN: Standarisasi Nama Kolom ---
            df.rename(columns={
                'NAMA': 'Nama Produk',
                'TERJUAL/BLN': 'Terjual/Bulan',
                'BRAND': 'Brand'
            }, inplace=True)
            # --- AKHIR PERBAIKAN ---

            for col in ['Harga', 'Terjual/Bulan', 'Omzet', 'Stok']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce')
            if 'TANGGAL' in df.columns:
                 df['TANGGAL'] = pd.to_datetime(df['TANGGAL'], errors='coerce', dayfirst=True)
            data_frames[name] = df
        return data_frames, None
    except Exception as e:
        error_message = f"Terjadi kesalahan spesifik saat menarik data: **{e}**"
        st.error(f"Terjadi kesalahan saat menarik data: {e}")
        st.info("Pastikan Anda telah mengatur kredensial Google Sheets dengan benar di st.secrets dan service account memiliki akses 'Editor' ke file Google Sheet.")
        return None, error_message


def preprocess_data(data_frames, start_date, end_date):
    """Membersihkan dan memproses data yang sudah ditarik."""
    processed_data = {}
    for name, df in data_frames.items():
        if df.empty or 'TANGGAL' not in df.columns:
            processed_data[name] = df
            continue
        
        df_filtered = df.dropna(subset=['TANGGAL'])
        df_filtered = df_filtered[(df_filtered['TANGGAL'] >= start_date) & (df_filtered['TANGGAL'] <= end_date)].copy()
        
        # Logika ekstraksi Brand dihapus karena Brand sudah ada dari sumber (setelah standarisasi)
        
        if 'Omzet' not in df_filtered.columns and 'Harga' in df_filtered.columns and 'Terjual/Bulan' in df_filtered.columns:
            df_filtered['Omzet'] = df_filtered['Harga'] * df_filtered['Terjual/Bulan']

        processed_data[name] = df_filtered
    return processed_data


def fuzzy_match_kategori(df_db, df_database, threshold=85):
    """Melakukan fuzzy matching untuk melabeli kategori produk."""
    if df_database.empty or 'Nama Produk' not in df_database.columns or 'Kategori' not in df_database.columns:
        df_db['Kategori'] = 'Tidak Terkategori'
        return df_db

    db_products = df_db['Nama Produk'].tolist()
    database_products = df_database['Nama Produk'].unique().tolist()
    kategori_map = pd.Series(df_database['Kategori'].values, index=df_database['Nama Produk']).to_dict()

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
    if pd.isna(val) or val == np.inf or val == -np.inf:
        return "⚪ ▬"
    if val > 0:
        return f"🟢 ▲ {val:.1f}%"
    elif val < 0:
        return f"🔴 ▼ {abs(val):.1f}%"
    else:
        return "⚪ ▬"

# --- UI STREAMLIT ---

if 'analisis_dimulai' not in st.session_state:
    st.session_state.analisis_dimulai = False
if 'data_cache' not in st.session_state:
    st.session_state.data_cache = None
if 'error_log' not in st.session_state:
    st.session_state.error_log = None

if not st.session_state.analisis_dimulai:
    st.title("📊 Dashboard Analisis Kompetitor E-commerce")
    st.markdown("Selamat datang! Dashboard ini dirancang untuk menganalisis data penjualan dari berbagai toko dan kompetitor.")
    st.markdown("---")
    
    if st.button("🚀 Tarik Data & Mulai Analisis"):
        GSHEET_ID = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
        SHEET_NAMES = [
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
        data_frames, error = tarik_data_dari_gsheet(GSHEET_ID, SHEET_NAMES)
        if data_frames is not None:
            st.session_state.data_cache = data_frames
            st.session_state.analisis_dimulai = True
            st.session_state.error_log = None
            st.rerun()
        else:
            st.session_state.error_log = error
    if st.session_state.error_log:
        st.error(f"Gagal memulai analisis. {st.session_state.error_log}")
else:
    all_data = st.session_state.data_cache

    with st.expander("🔍 Klik untuk melihat status data mentah yang ditarik"):
        for name, df in all_data.items():
            st.markdown(f"**Sheet: `{name}`**")
            if df.empty:
                st.warning("   -> DataFrame ini kosong.")
            else:
                st.success(f"   -> Berhasil ditarik, berisi **{len(df)}** baris.")
                st.write("   -> Kolom yang ditemukan:", df.columns.tolist())
    
    with st.sidebar:
        st.title("⚙️ Pengaturan Analisis")
        if st.button("🔄 Hapus Cache & Ulangi Analisis"):
            st.session_state.analisis_dimulai = False
            st.session_state.data_cache = None
            st.cache_data.clear()
            st.rerun()

        list_of_date_series = [df['TANGGAL'].dropna() for df in all_data.values() if 'TANGGAL' in df.columns and not df.empty and not df['TANGGAL'].dropna().empty]
        if not list_of_date_series:
            st.error("Gagal menemukan data TANGGAL yang valid di semua sheet.")
            st.warning("Mohon periksa nama kolom 'TANGGAL' dan format isinya di Google Sheet.")
            st.stop()

        all_dates = pd.concat(list_of_date_series)
        min_date, max_date = all_dates.min().date(), all_dates.max().date()
        selected_date_range = st.date_input("Pilih Rentang TANGGAL Analisis", (min_date, max_date), min_value=min_date, max_value=max_date)
        
        start_date = datetime.combine(selected_date_range[0], datetime.min.time()) if len(selected_date_range) > 0 else datetime.now()
        end_date = datetime.combine(selected_date_range[1], datetime.max.time()) if len(selected_date_range) > 1 else datetime.now()
        
        fuzzy_threshold = st.slider("Tingkat Akurasi Fuzzy Match (%)", 50, 100, 85)
        total_rows = sum(len(df) for df in all_data.values())
        st.info(f"Total baris data yang diolah: **{total_rows}**")

        processed_data = preprocess_data(all_data, start_date, end_date)
        
        db_klik_ready = processed_data.get("DB KLIK - REKAP - READY", pd.DataFrame())
        db_klik_habis = processed_data.get("DB KLIK - REKAP - HABIS", pd.DataFrame())
        df_db_klik_full = pd.concat([db_klik_ready, db_klik_habis]) if not db_klik_ready.empty or not db_klik_habis.empty else pd.DataFrame()
        
        # --- PERBAIKAN: Inisialisasi df_db_klik untuk mencegah KeyError ---
        df_db_klik = pd.DataFrame()
        if not df_db_klik_full.empty:
            df_db_klik = df_db_klik_full.sort_values('TANGGAL', ascending=False).drop_duplicates('Nama Produk', keep='first')
            @st.cache_data
            def convert_df(df): return df.to_csv(index=False).encode('utf-8')
            st.download_button(label="📥 Unduh Data CSV", data=convert_df(df_db_klik), file_name=f"db_klik_data.csv", mime="text/csv")
        # --- AKHIR PERBAIKAN ---

    st.title("Hasil Analisis E-commerce")
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📈 Analisis DB KLIK", "⚖️ Perbandingan Produk", "🏢 Analisis Kompetitor", "📦 Tren Stok", "💰 Omzet Toko", "🆕 Produk Baru"])

    with tab1:
        st.header("Analisis Kinerja Toko DB KLIK")
        if df_db_klik.empty:
            st.warning("Tidak ada data untuk DB KLIK pada rentang tanggal yang dipilih.")
        else:
            df_database = processed_data.get("DATABASE", pd.DataFrame())
            # Buat DataFrame dengan kategori terlebih dahulu
            df_db_klik_kategori = fuzzy_match_kategori(df_db_klik.copy(), df_database, fuzzy_threshold)
            
            # --- PERBAIKAN DIMULAI DI SINI ---
            # Periksa apakah kolom Omzet ada SEBELUM melakukan analisis
            if 'Omzet' in df_db_klik_kategori.columns:
                st.subheader("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
                kategori_omzet = df_db_klik_kategori.groupby('Kategori')['Omzet'].sum().sort_values(ascending=False).reset_index()
                col1, col2 = st.columns([1,3])
                sort_order = col1.radio("Urutkan Kategori", ["Terlaris", "Paling Tidak Laris"])
                num_bars = col1.slider("Jumlah Kategori", 1, len(kategori_omzet), min(10, len(kategori_omzet)))
                if sort_order == "Paling Tidak Laris": kategori_omzet = kategori_omzet.sort_values('Omzet', ascending=True)
                fig_kategori = px.bar(kategori_omzet.head(num_bars), x='Omzet', y='Kategori', orientation='h', title=f'Top {num_bars} Kategori', text='Omzet', labels={'Omzet': 'Total Omzet (Rp)', 'Kategori': 'Kategori'})
                fig_kategori.update_traces(texttemplate='Rp%{text:,.0f}', textposition='outside')
                fig_kategori.update_layout(yaxis={'categoryorder':'total ascending'})
                col2.plotly_chart(fig_kategori, use_container_width=True)
                
                st.subheader("Produk Terlaris per Kategori")
                selected_kategori = st.selectbox("Pilih Kategori", kategori_omzet['Kategori'].unique())
                produk_per_kategori = df_db_klik_kategori[df_db_klik_kategori['Kategori'] == selected_kategori].sort_values('Omzet', ascending=False)
                produk_per_kategori['Status'] = np.where(produk_per_kategori['Nama Produk'].isin(db_klik_ready['Nama Produk']), 'READY', 'HABIS')
                st.dataframe(produk_per_kategori[['Nama Produk', 'Harga', 'Terjual/Bulan', 'Status']].rename(columns={'Harga': 'Harga Terakhir', 'Terjual/Bulan': 'Terjual/Bulan Terakhir'}), use_container_width=True)

                st.subheader("2. Produk Terlaris (Global)")
                df_db_klik_full['Minggu'] = df_db_klik_full['Tanggal'].dt.to_period('W')
                produk_terkini = df_db_klik_full.sort_values('Tanggal').drop_duplicates('Nama Produk', keep='last')
                produk_minggu_lalu = df_db_klik_full[df_db_klik_full['Minggu'] == (produk_terkini['Minggu'].max() - 1)].sort_values('Tanggal').drop_duplicates('Nama Produk', keep='last')
                merged_produk = pd.merge(produk_terkini, produk_minggu_lalu[['Nama Produk', 'Omzet']], on='Nama Produk', how='left', suffixes=('', '_lalu'))
                merged_produk['Indikator'] = ((merged_produk['Omzet'] - merged_produk['Omzet_lalu']) / merged_produk['Omzet_lalu'].replace(0,1)) * 100
                merged_produk['Indikator'] = merged_produk['Indikator'].apply(format_indicator)
                st.dataframe(merged_produk[['Nama Produk', 'Harga', 'Terjual/Bulan', 'Omzet', 'Tanggal', 'Indikator']].sort_values('Omzet', ascending=False), use_container_width=True)

                st.subheader("3. Distribusi Omzet Brand")
                brand_omzet = df_db_klik_kategori.groupby('Brand')['Omzet'].sum().sort_values(ascending=False).reset_index()
                fig_brand = px.pie(brand_omzet.head(10), values='Omzet', names='Brand', title='Distribusi Omzet 10 Brand Teratas', hover_data=['Omzet'])
                fig_brand.update_traces(textposition='inside', textinfo='percent+label', hovertemplate='Brand: %{label}<br>Omzet: Rp%{value:,.0f}<extra></extra>')
                st.plotly_chart(fig_brand, use_container_width=True)

                st.subheader("4. Ringkasan Kinerja Mingguan (WoW Growth)")
                df_db_klik_full['Minggu'] = df_db_klik_full['Tanggal'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
                kinerja_mingguan = df_db_klik_full.groupby('Minggu').agg(Omzet=('Omzet', 'sum'), Penjualan_Unit=('Terjual/Bulan', 'sum')).sort_index().reset_index()
                kinerja_mingguan['Pertumbuhan Omzet'] = ((kinerja_mingguan['Omzet'] - kinerja_mingguan['Omzet'].shift(1)) / kinerja_mingguan['Omzet'].shift(1).replace(0,1)) * 100
                kinerja_mingguan['Pertumbuhan Omzet'] = kinerja_mingguan['Pertumbuhan Omzet'].apply(format_indicator)
                st.dataframe(kinerja_mingguan[['Minggu', 'Omzet', 'Penjualan_Unit', 'Pertumbuhan Omzet']], use_container_width=True)
            else:
                # Tampilkan pesan jika kolom Omzet tidak ada
                st.error("❌ Analisis Berdasarkan Omzet Gagal")
                st.warning("Kolom 'Omzet' tidak dapat ditemukan atau dihitung. Pastikan sheet 'DB KLIK' memiliki kolom **'Harga'** dan **'Terjual/Bulan'** yang valid.")
                st.info("Menampilkan data dasar yang tersedia:")
                st.dataframe(df_db_klik_kategori)
            # --- AKHIR PERBAIKAN ---
    with tab2:
        st.header("⚖️ Pilih Produk untuk Dibandingkan")
        all_products_list = df_db_klik['Nama Produk'].unique().tolist() if not df_db_klik.empty else []
        if not all_products_list:
            st.warning("Tidak ada produk DB KLIK yang tersedia untuk perbandingan.")
        else:
            selected_product = st.selectbox("Pilih produk dari DB KLIK", all_products_list, index=0)
            
            if selected_product:
                # Data produk utama
                produk_db_klik_hist = df_db_klik_full[df_db_klik_full['Nama Produk'] == selected_product].sort_values('TANGGAL')
                produk_db_klik_latest = produk_db_klik_hist.iloc[-1]
                
                st.subheader(f"Data Utama: {selected_product}")
                col1, col2, col3 = st.columns(3)
                col1.metric("Harga Terakhir", f"Rp{produk_db_klik_latest['Harga']:,.0f}")
                status_db_klik = "READY" if produk_db_klik_latest['Nama Produk'] in db_klik_ready['Nama Produk'].values else "HABIS"
                col2.metric("Status", status_db_klik)
                col3.metric("Stok", produk_db_klik_latest.get('Stok', 'N/A'))

                # Line chart harga
                fig_price = px.line(produk_db_klik_hist, x='TANGGAL', y='Harga', title=f'Perubahan Harga {selected_product}', markers=True)
                
                # Cari di toko lain
                st.subheader("Perbandingan di Toko Kompetitor")
                competitor_data = []
                store_names = [name.replace(" - REKAP - READY", "") for name in processed_data.keys() if "READY" in name and "DB KLIK" not in name]
                
                for store in store_names:
                    df_ready = processed_data.get(f"{store} - REKAP - READY", pd.DataFrame())
                    df_habis = processed_data.get(f"{store} - REKAP - HABIS", pd.DataFrame())
                    df_store = pd.concat([df_ready, df_habis])
                    if df_store.empty or 'Nama Produk' not in df_store.columns: continue

                    match, score = process.extractOne(selected_product, df_store['Nama Produk'].unique(), scorer=fuzz.token_sort_ratio)
                    if score >= fuzzy_threshold:
                        matched_hist = df_store[df_store['Nama Produk'] == match].sort_values('TANGGAL')
                        if matched_hist.empty: continue
                        matched_latest = matched_hist.iloc[-1]
                        status_kompetitor = "READY" if matched_latest['Nama Produk'] in df_ready['Nama Produk'].values else "HABIS"
                        selisih = matched_latest['Harga'] - produk_db_klik_latest['Harga']
                        
                        competitor_data.append({
                            'Toko': store, 'Produk Kompetitor': match, 'Harga': matched_latest['Harga'],
                            'Status': status_kompetitor, 'Stok': matched_latest.get('Stok', 'N/A'), 'Selisih': selisih
                        })
                        # Tambah ke line chart
                        fig_price.add_scatter(x=matched_hist['TANGGAL'], y=matched_hist['Harga'], mode='lines+markers', name=store)
                
                st.plotly_chart(fig_price, use_container_width=True)

                if competitor_data:
                    df_competitor = pd.DataFrame(competitor_data)
                    def format_selisih(s):
                        if s > 0: return f"Lebih Mahal (Rp{s:,.0f})"
                        elif s < 0: return f"Lebih Murah (Rp{abs(s):,.0f})"
                        return "Harga Sama"
                    df_competitor['Perbandingan'] = df_competitor['Selisih'].apply(format_selisih)
                    st.dataframe(df_competitor[['Toko', 'Produk Kompetitor', 'Harga', 'Status', 'Stok', 'Perbandingan']], use_container_width=True)
                else:
                    st.info("Tidak ditemukan produk serupa di toko kompetitor.")

    with tab3:
        st.header("🏢 Analisis Brand di Toko Kompetitor")
        store_list = sorted([name.replace(" - REKAP - READY", "") for name in processed_data if "READY" in name])
        if not store_list:
            st.warning("Tidak ada data toko yang tersedia untuk dianalisis.")
        else:
            selected_store = st.selectbox("Pilih Toko untuk dianalisis", store_list)
            
            if selected_store:
                df_ready = processed_data.get(f"{selected_store} - REKAP - READY", pd.DataFrame())
                df_habis = processed_data.get(f"{selected_store} - REKAP - HABIS", pd.DataFrame())
                df_full_store = pd.concat([df_ready, df_habis]) if not df_ready.empty or not df_habis.empty else pd.DataFrame()

                if df_full_store.empty:
                    st.warning(f"Tidak ada data untuk {selected_store} pada rentang TANGGAL ini.")
                else:
                    brand_analysis = df_full_store.groupby('Brand').agg(
                        Total_Omzet=('Omzet', 'sum'),
                        Unit_Terjual=('Terjual/Bulan', 'sum')
                    ).sort_values('Total_Omzet', ascending=False).reset_index()

                    st.subheader(f"Analisis Brand di {selected_store}")
                    col1, col2 = st.columns(2)
                    col1.dataframe(brand_analysis, use_container_width=True)
                    
                    fig_brand_pie = px.pie(brand_analysis.head(10), values='Total_Omzet', names='Brand', title=f'Top 10 Brand di {selected_store}')
                    fig_brand_pie.update_traces(textposition='inside', textinfo='percent+label')
                    col2.plotly_chart(fig_brand_pie, use_container_width=True)

    with tab4:
        st.header("📦 Tren Status Stok Mingguan per Toko")
        stock_data = []
        store_list_stock = sorted([name.replace(" - REKAP - READY", "") for name in processed_data if "READY" in name])
        
        for store in store_list_stock:
            df_ready = processed_data.get(f"{store} - REKAP - READY", pd.DataFrame())
            df_habis = processed_data.get(f"{store} - REKAP - HABIS", pd.DataFrame())
            
            if not df_ready.empty:
                df_ready['Minggu'] = df_ready['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
                ready_counts = df_ready.groupby('Minggu')['Nama Produk'].nunique().reset_index().rename(columns={'Nama Produk': 'Ready'})
                ready_counts['Toko'] = store
                stock_data.append(ready_counts)

            if not df_habis.empty:
                df_habis['Minggu'] = df_habis['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
                habis_counts = df_habis.groupby('Minggu')['Nama Produk'].nunique().reset_index().rename(columns={'Nama Produk': 'Habis'})
                habis_counts['Toko'] = store
                stock_data.append(habis_counts)
        
        if stock_data:
            df_stock = pd.concat(stock_data)
            df_stock_pivot = df_stock.pivot_table(index=['Minggu', 'Toko'], values=['Ready', 'Habis'], aggfunc='sum').reset_index().fillna(0)
            
            st.dataframe(df_stock_pivot, use_container_width=True)

            fig_stock = px.line(df_stock_pivot, x='Minggu', y=['Ready', 'Habis'], color='Toko', title="Tren Stok Mingguan per Toko", markers=True)
            st.plotly_chart(fig_stock, use_container_width=True)
        else:
            st.warning("Tidak ada data stok untuk divisualisasikan.")

    with tab5:
        st.header("💰 Tabel Omzet Semua Toko per Minggu")
        omzet_data = []
        store_list_omzet = sorted([name.replace(" - REKAP - READY", "") for name in processed_data if "READY" in name])

        for store in store_list_omzet:
            df_ready = processed_data.get(f"{store} - REKAP - READY", pd.DataFrame())
            df_habis = processed_data.get(f"{store} - REKAP - HABIS", pd.DataFrame())
            df_full = pd.concat([df_ready, df_habis])
            if not df_full.empty and 'Omzet' in df_full.columns:
                df_full['Minggu'] = df_full['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
                weekly_omzet = df_full.groupby('Minggu')['Omzet'].sum().reset_index()
                weekly_omzet['Toko'] = store
                omzet_data.append(weekly_omzet)
        
        if omzet_data:
            df_omzet_all = pd.concat(omzet_data)
            df_omzet_pivot = df_omzet_all.pivot(index='Minggu', columns='Toko', values='Omzet').fillna(0)
            df_omzet_pivot = df_omzet_pivot.astype(int)
            st.dataframe(df_omzet_pivot, use_container_width=True)

            fig_omzet_trend = px.line(df_omzet_all, x='Minggu', y='Omzet', color='Toko', title="Tren Omzet Mingguan per Toko", markers=True)
            st.plotly_chart(fig_omzet_trend, use_container_width=True)
        else:
            st.warning("Tidak ada data omzet untuk ditampilkan.")
            
    with tab6:
        st.header("🆕 Analisis Produk Baru Mingguan")
        
        # Gabungkan semua data produk dari semua toko
        all_products_df = pd.concat([df for name, df in processed_data.items() if 'REKAP' in name and not df.empty])

        if not all_products_df.empty and 'TANGGAL' in all_products_df.columns:
            all_products_df['Minggu'] = all_products_df['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
            unique_weeks = sorted(all_products_df['Minggu'].unique(), reverse=True)

            if len(unique_weeks) < 2:
                st.warning("Data tidak cukup untuk perbandingan minggu. Dibutuhkan data dari minimal 2 minggu yang berbeda.")
            else:
                col1, col2 = st.columns(2)
                target_week = col1.selectbox("Pilih Minggu Target", unique_weeks, index=0)
                comparison_week = col2.selectbox("Pilih Minggu Pembanding", unique_weeks, index=min(1, len(unique_weeks)-1))

                if target_week and comparison_week and target_week != comparison_week:
                    produk_target = set(all_products_df[all_products_df['Minggu'] == target_week]['Nama Produk'].unique())
                    produk_pembanding = set(all_products_df[all_products_df['Minggu'] == comparison_week]['Nama Produk'].unique())
                    
                    produk_baru = list(produk_target - produk_pembanding)
                    
                    st.subheader(f"Ditemukan {len(produk_baru)} produk baru pada minggu {target_week} dibandingkan {comparison_week}")
                    
                    if produk_baru:
                        df_produk_baru = all_products_df[all_products_df['Nama Produk'].isin(produk_baru)].sort_values('TANGGAL', ascending=False).drop_duplicates('Nama Produk', keep='first')
                        st.dataframe(df_produk_baru[['Nama Produk', 'Harga', 'Stok', 'Brand']], use_container_width=True)
                elif target_week == comparison_week:
                    st.warning("Minggu target dan pembanding tidak boleh sama.")
        else:
            st.warning("Tidak ada data produk yang cukup untuk analisis produk baru.")



