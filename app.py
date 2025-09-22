# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 4.1 (MODIFIKASI SESUAI REQUEST)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi ini menggabungkan backend pembaruan otomatis dari 'uji_coba'
#  dengan analisis dan visualisasi lengkap dari 'v3' (Struktur Asli)
#  dan menerapkan 10 poin perbaikan yang diminta.
# ===================================================================================

import streamlit as st
import pandas as pd
from rapidfuzz import process, fuzz # Menggunakan rapidfuzz dari versi uji_coba
import plotly.express as px
import re
import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe

# ================================
# KONFIGURASI HALAMAN
# ================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis v4.1")

# ================================
# FUNGSI KONEKSI GOOGLE SHEETS (Dari uji_coba)
# ================================
@st.cache_resource(show_spinner="Menghubungkan ke Google Sheets...")
def connect_to_gsheets():
    """
    Membuat koneksi aman ke Google Sheets menggunakan st.secrets.
    """
    creds_dict = {
        "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
        "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    gc = gspread.service_account_from_dict(creds_dict)
    return gc

# ================================
# FUNGSI MEMUAT SEMUA DATA (Dari uji_coba)
# ================================
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_all_data(spreadsheet_key):
    gc = connect_to_gsheets()
    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
    except Exception as e:
        st.error(f"GAGAL KONEKSI/OPEN SPREADSHEET: {e}")
        return None, None, None

    sheet_names = [
        "DATABASE", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS", "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS", "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS", "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS", "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
        "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS"
    ]
    rekap_list_df, database_df, matches_df = [], pd.DataFrame(), pd.DataFrame()

    for sheet_name in sheet_names:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            all_values = worksheet.get_all_values()
            if not all_values or len(all_values) < 2: continue
            header, data = all_values[0], all_values[1:]
            df_sheet = pd.DataFrame(data, columns=header)
            if '' in df_sheet.columns: df_sheet = df_sheet.drop(columns=[''])
            if "DATABASE" in sheet_name.upper():
                database_df = df_sheet
            elif "REKAP" in sheet_name.upper():
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                toko_name = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
                df_sheet['Toko'] = toko_name
                if 'Status' not in df_sheet.columns:
                    df_sheet['Status'] = 'Tersedia' if "READY" in sheet_name.upper() else 'Habis'
                rekap_list_df.append(df_sheet)
        except gspread.exceptions.WorksheetNotFound: continue
        except Exception as e: st.warning(f"Gagal baca sheet '{sheet_name}': {e}"); continue

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang berhasil dimuat."); return None, None, None
    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]
    
    # PERHATIAN: Kolom KATEGORI & SKU tidak di-rename agar tetap sesuai
    final_rename = {
        'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 
        'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand', 
        'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'
    }
    rekap_df.rename(columns=final_rename, inplace=True)

    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    if 'Tanggal' in rekap_df.columns:
        rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    if 'Harga' in rekap_df.columns:
        rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    if 'Terjual per Bulan' in rekap_df.columns:
        rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga', 'Toko'], inplace=True)
    if 'Brand' not in rekap_df.columns or rekap_df['Brand'].isnull().all():
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    rekap_df['Omzet'] = (rekap_df['Harga'].fillna(0) * rekap_df.get('Terjual per Bulan', 0).fillna(0)).astype(int)

    try:
        matches_sheet = spreadsheet.worksheet("HASIL_MATCHING")
        matches_df = pd.DataFrame(matches_sheet.get_all_records())
        if not matches_df.empty: matches_df.columns = [str(c).strip() for c in matches_df.columns]
    except gspread.exceptions.WorksheetNotFound: pass
    except Exception as e: st.warning(f"Gagal memuat 'HASIL_MATCHING': {e}")

    return rekap_df.sort_values('Tanggal'), database_df, matches_df

# ================================
# FUNGSI UNTUK PROSES UPDATE HARGA (Dari uji_coba)
# ================================
def load_source_data_for_update(gc, spreadsheet_key):
    spreadsheet = gc.open_by_key(spreadsheet_key)
    sheet_objs = [s for s in spreadsheet.worksheets() if "REKAP" in s.title.upper()]
    rekap_list = []
    for s in sheet_objs:
        try:
            vals = s.get_all_values()
            if not vals or len(vals) < 2: continue
            header, data = vals[0], vals[1:]
            df = pd.DataFrame(data, columns=header)
            if '' in df.columns: df = df.drop(columns=[''])
            store_name_match = re.match(r"^(.*?) - REKAP", s.title, re.IGNORECASE)
            df['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
            rekap_list.append(df)
        except Exception: continue
    if not rekap_list: return pd.DataFrame()
    rekap_df = pd.concat(rekap_list, ignore_index=True)
    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]
    rekap_df.rename(columns={'NAMA': 'Nama Produk', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga'}, inplace=True)
    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Toko', 'Harga'], inplace=True)
    idx = rekap_df.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()
    return rekap_df.loc[idx].reset_index(drop=True)

def run_price_comparison_update(gc, spreadsheet_key, score_cutoff=88):
    placeholder = st.empty()
    with placeholder.container():
        st.info("Memulai pembaruan perbandingan harga...")
        prog = st.progress(0, text="0%")
    source_df = load_source_data_for_update(gc, spreadsheet_key)
    if source_df is None or source_df.empty:
        with placeholder.container(): st.error("Gagal memuat data sumber untuk update. Batal."); return
    my_store_name = "DB KLIK"
    my_store_df = source_df[source_df['Toko'] == my_store_name]
    competitor_df = source_df[source_df['Toko'] != my_store_name]
    if my_store_df.empty or competitor_df.empty:
        with placeholder.container(): st.warning("Data toko Anda atau kompetitor tidak cukup."); return
    competitor_products_list = competitor_df['Nama Produk'].unique().tolist()
    all_matches = []
    total = len(my_store_df)
    for i, (_, row) in enumerate(my_store_df.iterrows()):
        prog.progress(int((i / total) * 80), text=f"Mencocokkan produk {i+1}/{total}")
        matches = process.extract(row['Nama Produk'], competitor_products_list, scorer=fuzz.token_set_ratio, limit=5, score_cutoff=score_cutoff)
        for match_name, score, _ in matches:
            matched_rows = competitor_df[competitor_df['Nama Produk'] == match_name]
            for _, mrow in matched_rows.iterrows():
                all_matches.append({'Produk Toko Saya': row['Nama Produk'], 'Harga Toko Saya': int(row['Harga']), 'Produk Kompetitor': match_name, 'Harga Kompetitor': int(mrow['Harga']), 'Toko Kompetitor': mrow['Toko'], 'Skor Kemiripan': int(score), 'Tanggal_Update': datetime.now().strftime('%Y-%m-%d')})
    prog.progress(90, text="Menyimpan hasil...")
    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
        try:
            worksheet = spreadsheet.worksheet("HASIL_MATCHING")
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="HASIL_MATCHING", rows=1, cols=1)
        if all_matches:
            results_df = pd.DataFrame(all_matches)
            set_with_dataframe(worksheet, results_df, resize=True)
            with placeholder.container(): st.success(f"Selesai: {len(results_df)} baris hasil disimpan.")
        else:
            with placeholder.container(): st.warning("Tidak ditemukan pasangan produk yang cocok.")
    except Exception as e:
        with placeholder.container(): st.error(f"Gagal menyimpan hasil: {e}")

# ================================
# FUNGSI-FUNGSI PEMBANTU (UTILITY)
# ================================
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

# ================================
# APLIKASI UTAMA (MAIN APP)
# ================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor — v4.1")

SPREADSHEET_KEY = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
gc = connect_to_gsheets()

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if not st.session_state.data_loaded:
    _, col_center, _ = st.columns([2, 3, 2])
    with col_center:
        if st.button("Tarik Data & Mulai Analisis 🚀", type="primary"):
            df, db_df, matches_df = load_all_data(SPREADSHEET_KEY)
            if df is not None and not df.empty:
                st.session_state.df, st.session_state.db_df, st.session_state.matches_df = df, db_df, matches_df
                st.session_state.data_loaded = True
                st.rerun()
            else:
                st.error("Gagal memuat data. Periksa akses Google Sheets dan st.secrets.")
    st.info("👆 Klik tombol untuk menarik semua data yang diperlukan untuk analisis.")
    st.stop()

df = st.session_state.df
db_df = st.session_state.db_df if 'db_df' in st.session_state else pd.DataFrame()
matches_df = st.session_state.matches_df if 'matches_df' in st.session_state else pd.DataFrame()

# ================================
# SIDEBAR (Dari uji_coba)
# ================================
st.sidebar.header("Kontrol & Filter")
min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
if len(selected_date_range) != 2: st.sidebar.warning("Pilih 2 tanggal."); st.stop()
start_date, end_date = selected_date_range
accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)

# Logika Pembaruan Otomatis
latest_source_date = df['Tanggal'].max().date()
last_destination_update = datetime(1970, 1, 1).date()
if not matches_df.empty and 'Tanggal_Update' in matches_df.columns:
    matches_df['Tanggal_Update'] = pd.to_datetime(matches_df['Tanggal_Update'], errors='coerce')
    if not matches_df['Tanggal_Update'].isna().all():
        last_destination_update = matches_df['Tanggal_Update'].max().date()
st.sidebar.info(f"Data Sumber Terbaru: **{latest_source_date.strftime('%d %b %Y')}**")
st.sidebar.info(f"Perbandingan Terakhir: **{last_destination_update.strftime('%d %b %Y')}**")
if latest_source_date > last_destination_update:
    st.sidebar.warning("Data sumber lebih baru dari hasil perbandingan.")
    if st.sidebar.button("Perbarui Sekarang 🚀", type="primary"):
        run_price_comparison_update(gc, SPREADSHEET_KEY, score_cutoff=accuracy_cutoff)
        _, _, new_matches_df = load_all_data(SPREADSHEET_KEY)
        st.session_state.matches_df = new_matches_df
        st.success("Pembaruan selesai."); st.rerun()
else:
    st.sidebar.success("Data perbandingan sudah terbaru.")
if st.sidebar.button("Jalankan Pembaruan Manual", type="secondary"):
    run_price_comparison_update(gc, SPREADSHEET_KEY, score_cutoff=accuracy_cutoff)
    _, _, new_matches_df = load_all_data(SPREADSHEET_KEY)
    st.session_state.matches_df = new_matches_df
    st.success("Pembaruan manual selesai."); st.rerun()

st.sidebar.divider()
df_filtered_export = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)]
st.sidebar.header("Ekspor & Info")
st.sidebar.info(f"Baris data dalam rentang: **{len(df_filtered_export)}**")
csv_data = convert_df_for_download(df_filtered_export)
st.sidebar.download_button("📥 Unduh CSV (Filter)", data=csv_data, file_name=f'analisis_{start_date}_{end_date}.csv', mime='text/csv')

# ================================
# PERSIAPAN DATA UNTUK TABS (Gabungan dari v3 dan uji_coba)
# ================================
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty: st.error("Tidak ada data di rentang tanggal ini."); st.stop()

df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
my_store_name = "DB KLIK"
main_store_df = df_filtered[df_filtered['Toko'] == my_store_name]
competitor_df = df_filtered[df_filtered['Toko'] != my_store_name]

# Variabel tambahan yang dibutuhkan oleh tab dari v3
latest_entries_weekly = df_filtered.loc[df_filtered.groupby(['Minggu', 'Toko', 'Nama Produk'])['Tanggal'].idxmax()]
latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
main_store_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == my_store_name]
competitor_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] != my_store_name]

# ================================
# TABS (ANALISIS DIAMBIL DARI V3 & DIMODIFIKASI)
# ================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["⭐ Analisis Toko Saya", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

with tab1:
    st.header(f"Analisis Kinerja Toko: {my_store_name}")
    
    section_counter = 1

    st.subheader(f"{section_counter}. Analisis Kategori Terlaris (Berdasarkan Omzet)")
    section_counter += 1
    
    if 'KATEGORI' in main_store_latest_overall.columns:
        main_store_cat = main_store_latest_overall.copy()
        main_store_cat['KATEGORI'] = main_store_cat['KATEGORI'].replace('', 'Lainnya').fillna('Lainnya')
        
        category_sales = main_store_cat.groupby('KATEGORI')['Omzet'].sum().reset_index()
        
        if not category_sales.empty:
            cat_sales_sorted = category_sales.sort_values('Omzet', ascending=False).head(10)
            fig_cat = px.bar(cat_sales_sorted, x='KATEGORI', y='Omzet', title='Top 10 Kategori Berdasarkan Omzet', text_auto='.2s')
            st.plotly_chart(fig_cat, use_container_width=True)

            # --- POIN 1: Penambahan Tabel untuk Bar Chart ---
            st.markdown("##### Rincian Data Omzet per Kategori")
            table_cat_sales = cat_sales_sorted.copy()
            table_cat_sales['Omzet'] = table_cat_sales['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
            st.dataframe(table_cat_sales, use_container_width=True, hide_index=True)
            # --- AKHIR PERUBAHAN POIN 1 ---

            st.markdown("---")
            st.subheader("Lihat Produk Terlaris per Kategori")
            
            category_list = category_sales.sort_values('Omzet', ascending=False)['KATEGORI'].tolist()
            
            selected_category = st.selectbox(
                "Pilih Kategori untuk melihat produk terlaris:",
                options=category_list
            )

            if selected_category:
                products_in_category = main_store_cat[main_store_cat['KATEGORI'] == selected_category].copy()
                top_products_in_category = products_in_category.sort_values('Terjual per Bulan', ascending=False)

                if top_products_in_category.empty:
                    st.info(f"Tidak ada produk terlaris untuk kategori '{selected_category}'.")
                else:
                    # --- POIN 2: Penambahan Kolom SKU ---
                    columns_to_display = ['Nama Produk', 'SKU', 'Harga', 'Terjual per Bulan', 'Omzet']
                    if 'SKU' not in top_products_in_category.columns:
                        top_products_in_category['SKU'] = 'N/A' # Fallback jika SKU tidak ada
                    
                    display_table = top_products_in_category[columns_to_display].copy()
                    display_table['Harga'] = display_table['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                    display_table['Omzet'] = display_table['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                    
                    st.dataframe(display_table, use_container_width=True, hide_index=True)
                    # --- AKHIR PERUBAHAN POIN 2 ---
        else:
            st.info("Tidak ada data omzet per kategori untuk ditampilkan.")
    else:
        st.warning("Kolom 'KATEGORI' tidak ditemukan pada data toko Anda. Analisis ini dilewati.")

    st.subheader(f"{section_counter}. Produk Terlaris")
    section_counter += 1
    top_products = main_store_latest_overall.sort_values('Terjual per Bulan', ascending=False).head(15).copy()
    top_products['Harga_rp'] = top_products['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
    top_products['Omzet_rp'] = top_products['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
    
    # --- POIN 9: Penambahan Kolom SKU ---
    display_cols_top = ['Nama Produk', 'SKU', 'Harga_rp', 'Omzet_rp', 'Terjual per Bulan']
    if 'SKU' not in top_products.columns:
        top_products['SKU'] = 'N/A' # Fallback
        
    display_df_top = top_products[display_cols_top].rename(
        columns={'Harga_rp': 'Harga', 'Omzet_rp': 'Omzet'}
    )
    st.dataframe(display_df_top, use_container_width=True, hide_index=True)
    # --- AKHIR PERUBAHAN POIN 9 ---

    st.subheader(f"{section_counter}. Distribusi Omzet Brand")
    section_counter += 1
    brand_omzet_main = main_store_latest_overall.groupby('Brand')['Omzet'].sum().reset_index()
    if not brand_omzet_main.empty:
        fig_brand_pie = px.pie(brand_omzet_main.sort_values('Omzet', ascending=False).head(7), 
                            names='Brand', values='Omzet', title='Distribusi Omzet Top 7 Brand (Snapshot Terakhir)')
        
        # --- POIN 3: Modifikasi Tampilan Pie Chart ---
        fig_brand_pie.update_traces(
            textposition='outside',
            texttemplate='%{label}<br><b>Rp %{value:,.0f}</b><br>(%{percent})',
            insidetextfont=dict(color='white')
        )
        fig_brand_pie.update_layout(showlegend=False)
        # --- AKHIR PERUBAHAN POIN 3 ---
        
        st.plotly_chart(fig_brand_pie, use_container_width=True)
    else:
        st.info("Tidak ada data omzet brand.")

    st.subheader(f"{section_counter}. Ringkasan Kinerja Mingguan (WoW Growth)")
    section_counter += 1
    main_store_latest_weekly = main_store_df.loc[main_store_df.groupby(['Minggu', 'Nama Produk'])['Tanggal'].idxmax()]
    weekly_summary_tab1 = main_store_latest_weekly.groupby('Minggu').agg(
        Omzet=('Omzet', 'sum'), Penjualan_Unit=('Terjual per Bulan', 'sum')
    ).reset_index().sort_values('Minggu')
    weekly_summary_tab1['Pertumbuhan Omzet (WoW)'] = weekly_summary_tab1['Omzet'].pct_change().apply(format_wow_growth)
    weekly_summary_tab1['Omzet'] = weekly_summary_tab1['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
    st.dataframe(
        weekly_summary_tab1[['Minggu', 'Omzet', 'Penjualan_Unit', 'Pertumbuhan Omzet (WoW)']].style.applymap(
            style_wow_growth, subset=['Pertumbuhan Omzet (WoW)']
        ), use_container_width=True, hide_index=True
    )

with tab2:
    st.header(f"Perbandingan Produk '{my_store_name}' dengan Kompetitor")
    st.info("Perbandingan menggunakan data produk terbaru dari toko Anda.")
    
    df_main_store_unfiltered = df[df['Toko'] == my_store_name]
    latest_date_in_db_klik = df_main_store_unfiltered['Tanggal'].max()
    latest_products_df = df_main_store_unfiltered[df_main_store_unfiltered['Tanggal'] == latest_date_in_db_klik].copy()
    
    brand_list = sorted(latest_products_df['Brand'].unique())
    selected_brand = st.selectbox("Filter berdasarkan Brand:", ["Semua Brand"] + brand_list, key="brand_select_compare")
    
    if selected_brand != "Semua Brand":
        products_to_show_df = latest_products_df[latest_products_df['Brand'] == selected_brand]
    else:
        products_to_show_df = latest_products_df
        
    product_list = sorted(products_to_show_df['Nama Produk'].unique())
    selected_product = st.selectbox("Pilih produk dari toko Anda:", product_list, key="product_select_compare")

    if selected_product:
        product_info_list = latest_products_df[latest_products_df['Nama Produk'] == selected_product]
        
        if not product_info_list.empty:
            product_info = product_info_list.iloc[0]
            st.markdown(f"**Produk Pilihan Anda:** *{product_info['Nama Produk']}*")
            
            all_occurrences = df_filtered[df_filtered['Nama Produk'] == selected_product]
            matches_for_product = matches_df[
                (matches_df['Produk Toko Saya'] == selected_product) &
                (matches_df['Skor Kemiripan'] >= accuracy_cutoff)
            ].sort_values(by='Skor Kemiripan', ascending=False)

            col1, col2, col3 = st.columns(3)
            
            # Metrik 1: Harga Rata-rata
            if not all_occurrences.empty:
                avg_price = all_occurrences['Harga'].mean()
                col1.metric("Harga Rata-Rata (Semua Toko)", f"Rp {int(avg_price):,}")
            else:
                col1.metric("Harga Rata-Rata (Semua Toko)", "N/A")
                
            # --- POIN 1 (Revisi): Logika Baru untuk Perbandingan Status ---
            total_competitor_stores = len(competitor_df['Toko'].unique())
            
            # Cari status dari produk yang cocok di data kompetitor terbaru
            matched_product_names = matches_for_product['Produk Kompetitor'].unique()
            matched_products_details = competitor_latest_overall[
                competitor_latest_overall['Nama Produk'].isin(matched_product_names)
            ]
            
            ready_count = matched_products_details[matched_products_details['Status'] == 'Tersedia']['Toko'].nunique()
            oot_count = total_competitor_stores - ready_count
            
            col2.metric(
                "Status di Kompetitor", 
                f"Ready: {ready_count} | Habis: {oot_count}", 
                help=f"Berdasarkan {total_competitor_stores} total toko kompetitor yang dipantau."
            )
            # --- AKHIR PERUBAHAN POIN 1 (Revisi) ---

            # --- POIN 1 (Revisi): Menampilkan Omzet di Metrik ---
            if not all_occurrences.empty:
                top_store_row = all_occurrences.loc[all_occurrences['Omzet'].idxmax()]
                top_store_name = top_store_row['Toko']
                top_omzet = top_store_row['Omzet']
                col3.metric("Toko Omzet Tertinggi", f"{top_store_name}", f"Rp {int(top_omzet):,}")
            else:
                 col3.metric("Toko Omzet Tertinggi", "N/A")
            # --- AKHIR PERUBAHAN POIN 1 (Revisi) ---
            
            st.divider()
            
            # --- POIN 2 (Revisi): Memperbarui Tabel Perbandingan ---
            st.subheader("Perbandingan di Toko Kompetitor (Hasil Matching Terakhir)")
            if matches_for_product.empty:
                st.warning("Tidak ditemukan kecocokan di 'HASIL_MATCHING' dengan filter akurasi Anda.")
            else:
                comparison_data = []
                my_price = int(product_info['Harga'])

                for _, match in matches_for_product.iterrows():
                    comp_price = int(match['Harga Kompetitor'])
                    price_diff = comp_price - my_price
                    
                    # Tambahan info lebih murah/mahal
                    if price_diff > 0:
                        diff_text = f" (Lebih Mahal)"
                    elif price_diff < 0:
                        diff_text = f" (Lebih Murah)"
                    else:
                        diff_text = f" (Sama)"

                    # Ambil data 'Terjual per Bulan' dan 'Omzet' dari data kompetitor terbaru
                    comp_details = competitor_latest_overall[
                        (competitor_latest_overall['Nama Produk'] == match['Produk Kompetitor']) &
                        (competitor_latest_overall['Toko'] == match['Toko Kompetitor'])
                    ]
                    
                    terjual = comp_details['Terjual per Bulan'].iloc[0] if not comp_details.empty else 0
                    omzet = comp_details['Omzet'].iloc[0] if not comp_details.empty else 0
                    
                    comparison_data.append({
                        'Toko Kompetitor': match['Toko Kompetitor'],
                        'Harga Kompetitor': f"Rp {comp_price:,}",
                        'Selisih Harga': f"Rp {price_diff:,}{diff_text}",
                        'Terjual per Bulan': int(terjual) if terjual else 0,
                        'Omzet': f"Rp {int(omzet):,}" if omzet else "Rp 0",
                        'Skor Kemiripan (%)': int(match['Skor Kemiripan'])
                    })
                
                comparison_df = pd.DataFrame(comparison_data)
                # Mengatur urutan kolom agar lebih rapi
                ordered_cols = [
                    'Toko Kompetitor', 'Harga Kompetitor', 'Selisih Harga', 
                    'Terjual per Bulan', 'Omzet', 'Skor Kemiripan (%)'
                ]
                st.dataframe(comparison_df[ordered_cols], use_container_width=True, hide_index=True)
            # --- AKHIR PERUBAHAN POIN 2 (Revisi) ---
            
with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    if competitor_df.empty:
        st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
    else:
        competitor_list = sorted(competitor_df['Toko'].unique())
        for competitor_store in competitor_list:
            with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                single_competitor_df = competitor_latest_overall[competitor_latest_overall['Toko'] == competitor_store]
                brand_analysis = single_competitor_df.groupby('Brand').agg(
                    Total_Omzet=('Omzet', 'sum'), 
                    Total_Unit_Terjual=('Terjual per Bulan', 'sum')
                ).reset_index().sort_values("Total_Omzet", ascending=False)
                
                if not brand_analysis.empty:
                    # --- POIN 10: Format Rupiah pada Kolom Total_Omzet ---
                    display_brand_analysis = brand_analysis.head(10).copy()
                    display_brand_analysis['Total_Omzet'] = display_brand_analysis['Total_Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                    st.dataframe(display_brand_analysis, use_container_width=True, hide_index=True)
                    # --- AKHIR PERUBAHAN POIN 10 ---

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
        omzet_pivot = df_filtered.pivot_table(index='Toko', columns='Tanggal', values='Omzet', aggfunc='sum').fillna(0)
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
                        new_products_df['Harga_fmt'] = new_products_df['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'Harga_fmt', 'Stok', 'Brand']].rename(columns={'Harga_fmt':'Harga'}), use_container_width=True, hide_index=True)

