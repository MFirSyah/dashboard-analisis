# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI FINAL
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Tanggal: 1 Agustus 2025
#  Metode Koneksi: Aman (gspread + st.secrets)
# ===================================================================================

import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import re
import gspread
from gspread_pandas import Spread

st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# --- FUNGSI-FUNGSI UTAMA ---

@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...", ttl=300)
def load_data_from_gsheets():
    try:
        spreadsheet_id = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
        gc = gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        spreadsheet = gc.open_by_key(spreadsheet_id)
        spread = Spread(spread=spreadsheet)
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: {e}")
        st.warning("Pastikan Anda sudah mengatur 'Secrets' dengan benar di Streamlit Cloud dan sudah membagikan (share) Google Sheet Anda ke email service account.")
        return pd.DataFrame(), pd.DataFrame(), None

    rekap_list_df = []
    database_df = pd.DataFrame()
    
    try:
        for sheet in spread.sheets:
            sheet_title = sheet.title
            if "DATABASE" in sheet_title.upper():
                database_df = spread.sheet_to_df(index=None, sheet=sheet)
            elif "REKAP" in sheet_title.upper():
                df_sheet = spread.sheet_to_df(index=None, sheet=sheet, header_rows=1)
                if df_sheet.empty: continue
                
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_title, re.IGNORECASE)
                df_sheet['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tidak Dikenal"
                df_sheet['Status'] = 'Tersedia' if "READY" in sheet_title.upper() else 'Habis'
                rekap_list_df.append(df_sheet)
    except Exception as e:
        st.error(f"Gagal memproses salah satu sheet: {e}. Periksa format data di Google Sheets Anda.")
        return pd.DataFrame(), pd.DataFrame(), None

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang berhasil dimuat.")
        return pd.DataFrame(), pd.DataFrame(), None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    my_store_name = "DB KLIK" # Nama Toko Utama Anda
    if not database_df.empty:
        database_df.columns = [str(col).strip().upper() for col in database_df.columns]
    
    column_mapping = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga'}
    rekap_df.rename(columns=column_mapping, inplace=True)
    
    if 'BRAND' not in rekap_df.columns:
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    else:
        rekap_df['Brand'] = rekap_df['BRAND'].str.upper()
    
    if 'STOK' not in rekap_df.columns:
        rekap_df['Stok'] = 'N/A'
    
    required_cols = ['Tanggal', 'Nama Produk', 'Harga', 'Terjual per Bulan']
    if not all(col in rekap_df.columns for col in required_cols):
        st.error(f"Kolom krusial tidak ditemukan. Pastikan sheet REKAP memiliki: {required_cols}")
        return pd.DataFrame(), pd.DataFrame(), my_store_name

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce')
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[Rp,.]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)
    
    for col in ['Harga', 'Terjual per Bulan']:
        rekap_df[col] = rekap_df[col].astype(int)

    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True)
    return rekap_df.sort_values('Tanggal'), database_df, my_store_name

def get_smart_matches(query_product_info, competitor_df, limit=5, score_cutoff=90):
    query_name = query_product_info['Nama Produk']
    competitor_product_list = competitor_df['Nama Produk'].tolist()
    candidates = process.extract(query_name, competitor_product_list, limit=20, scorer=fuzz.token_set_ratio)
    return [match for match in candidates if match[1] >= score_cutoff][:limit]

# --- INTERFACE DASHBOARD UTAMA---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
st.sidebar.header("Kontrol Analisis")

if st.sidebar.button("Tarik Data & Mulai Analisis 🚀"):
    df, db_df, my_store_name_from_db = load_data_from_gsheets()
    if df.empty:
        st.error("Gagal memuat data. Periksa pesan error di atas dan pengaturan Anda.")
        st.stop()
        
    st.sidebar.header("Filter & Pengaturan")
    all_stores_list = sorted(df['Toko'].unique())
    main_store_for_comp = st.sidebar.selectbox("Pilih Toko Utama:", options=all_stores_list, index=all_stores_list.index(my_store_name_from_db) if my_store_name_from_db in all_stores_list else 0)
    min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
    start_date = st.sidebar.date_input("Tanggal Mulai:", min_date, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("Tanggal Akhir:", max_date, min_value=start_date, max_value=max_date)
    accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1, help="Mengatur seberapa mirip nama produk agar dianggap sama.")

    df_filtered = df[(df['Tanggal'] >= pd.to_datetime(start_date)) & (df['Tanggal'] <= pd.to_datetime(end_date))].copy()
    if df_filtered.empty:
        st.error("Tidak ada data pada rentang tanggal yang dipilih.")
        st.stop()
    
    main_store_df = df_filtered[df_filtered['Toko'] == main_store_for_comp].copy()
    competitor_df = df_filtered[df_filtered['Toko'] != main_store_for_comp].copy()
    
    tab_titles = []
    if not db_df.empty: tab_titles.append(f"⭐ Analisis Toko Saya ({main_store_for_comp})")
    tab_titles.extend(["⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan"])
    created_tabs = st.tabs(tab_titles)
    
    tab_index = 0

    if not db_df.empty:
        with created_tabs[tab_index]:
            st.header(f"Analisis Kinerja Toko: {main_store_for_comp}")
            my_store_rekap_df = df_filtered[df_filtered['Toko'] == main_store_for_comp].copy()

            st.subheader("1. Kategori Produk Terlaris")
            if not db_df.empty and 'KATEGORI' in db_df.columns:
                @st.cache_data
                def fuzzy_merge_categories(_rekap_df, _database_df):
                    _rekap_df['Kategori'] = 'Lainnya'
                    db_map = _database_df.set_index('NAMA')['KATEGORI']
                    for index, row in _rekap_df.iterrows():
                        match, score = process.extractOne(row['Nama Produk'], db_map.index, scorer=fuzz.token_set_ratio)
                        if score >= 95:
                            _rekap_df.loc[index, 'Kategori'] = db_map[match]
                    return _rekap_df
                
                my_store_rekap_df = fuzzy_merge_categories(my_store_rekap_df, db_df)
                category_sales = my_store_rekap_df.groupby('Kategori')['Terjual per Bulan'].sum().reset_index()

                col1, col2 = st.columns(2)
                sort_order = col1.radio("Urutkan:", ["Terlaris ke Terendah", "Terendah ke Terlaris"], horizontal=True)
                top_n_cat = col2.number_input("Tampilkan Top:", min_value=1, max_value=len(category_sales), value=10)
                
                is_desc = sort_order == "Terlaris ke Terendah"
                category_sales_sorted = category_sales.sort_values(by='Terjual per Bulan', ascending=not is_desc).head(top_n_cat)
                
                fig_cat = px.bar(category_sales_sorted, x='Kategori', y='Terjual per Bulan', title=f'Top {top_n_cat} Kategori Terlaris', text_auto=True)
                st.plotly_chart(fig_cat, use_container_width=True)
            else:
                st.warning("Sheet DATABASE atau kolom KATEGORI tidak ditemukan.")

            st.subheader("2. Produk Terlaris")
            top_products = my_store_rekap_df.sort_values(by='Terjual per Bulan', ascending=False).head(15)[['Nama Produk', 'Terjual per Bulan', 'Omzet']]
            top_products['Omzet'] = top_products['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
            st.dataframe(top_products, use_container_width=True, hide_index=True)

            st.subheader("3. Distribusi Penjualan Brand (Top 6)")
            brand_sales = my_store_rekap_df.groupby('Brand')['Terjual per Bulan'].sum().nlargest(6).reset_index()
            fig_brand_pie = px.pie(brand_sales, names='Brand', values='Terjual per Bulan', title='Top 6 Brand Terlaris')
            st.plotly_chart(fig_brand_pie, use_container_width=True)
        tab_index += 1

    with created_tabs[tab_index]:
        st.header(f"Perbandingan dengan Kompetitor")

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
    tab_index += 1

    with created_tabs[tab_index]:
        st.header("Analisis Brand di Toko Kompetitor")
        # ... (Implementasi Analisis Brand)
    tab_index += 1
    
    with created_tabs[tab_index]:
        st.header("Tren Status Stok Mingguan")
        # ... (Implementasi Line Chart Stok)
    tab_index += 1
    
    with created_tabs[tab_index]:
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        # ... (Implementasi Kinerja Penjualan)
    tab_index += 1
    
else:
    st.info("👈 Klik tombol di sidebar untuk menarik data dan memulai analisis.")
    st.stop()
