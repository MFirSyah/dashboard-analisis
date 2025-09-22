# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI DEPLOYMENT (FINAL)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi ini menggabungkan semua fitur dari file 'uji_coba' ke dalam
#  struktur yang dioptimalkan untuk hosting di Streamlit Community Cloud.
# ===================================================================================

import streamlit as st
import pandas as pd
from rapidfuzz import process, fuzz
import plotly.express as px
import re
import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe

# ================================
# KONFIGURASI HALAMAN
# ================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis Penjualan")

# ================================
# FUNGSI KONEKSI GOOGLE SHEETS
# ================================
@st.cache_resource(show_spinner="Menghubungkan ke Google Sheets...")
def connect_to_gsheets():
    """
    Membuat koneksi aman ke Google Sheets menggunakan st.secrets.
    """
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
    return gc

# ================================
# FUNGSI MEMUAT SEMUA DATA
# ================================
@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_all_data(spreadsheet_key):
    """
    Memuat semua data rekap DAN data perbandingan dari 'HASIL_MATCHING'.
    """
    gc = connect_to_gsheets()
    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
    except Exception as e:
        st.error(f"GAGAL KONEKSI/OPEN SPREADSHEET: {e}")
        return None, None

    # Daftar sheet rekap
    sheet_names = [
        "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS", "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS", "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS", "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS", "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
        "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS"
    ]
    rekap_list_df = []

    # Memuat data rekap
    for sheet_name in sheet_names:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            all_values = worksheet.get_all_values()
            if not all_values or len(all_values) < 2: continue
            header, data = all_values[0], all_values[1:]
            df_sheet = pd.DataFrame(data, columns=header)
            if '' in df_sheet.columns: df_sheet = df_sheet.drop(columns=[''])
            
            store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
            toko_name = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
            df_sheet['Toko'] = toko_name
            if 'Status' not in df_sheet.columns:
                df_sheet['Status'] = 'Tersedia' if "READY" in sheet_name.upper() else 'Habis'
            rekap_list_df.append(df_sheet)
        except gspread.exceptions.WorksheetNotFound: continue
        except Exception as e: st.warning(f"Gagal baca sheet '{sheet_name}': {e}"); continue

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang berhasil dimuat."); return None, None
    
    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]
    final_rename = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'}
    rekap_df.rename(columns=final_rename, inplace=True)

    # Pembersihan data
    for col, dtype in {'Nama Produk': str, 'Harga': 'numeric', 'Terjual per Bulan': 'numeric'}.items():
        if col in rekap_df.columns:
            if dtype == 'numeric':
                rekap_df[col] = pd.to_numeric(rekap_df[col].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
            else:
                rekap_df[col] = rekap_df[col].astype(str).str.strip()
    
    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Terjual per Bulan'] = rekap_df['Terjual per Bulan'].fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga', 'Toko'], inplace=True)
    rekap_df['Omzet'] = (rekap_df['Harga'].fillna(0) * rekap_df['Terjual per Bulan']).astype(int)

    # Memuat data perbandingan yang sudah ada
    matches_df = pd.DataFrame()
    try:
        matches_sheet = spreadsheet.worksheet("HASIL_MATCHING")
        matches_df = pd.DataFrame(matches_sheet.get_all_records())
        if not matches_df.empty:
            matches_df.columns = [str(c).strip() for c in matches_df.columns]
    except gspread.exceptions.WorksheetNotFound:
        st.warning("Worksheet 'HASIL_MATCHING' tidak ditemukan. Perbandingan harga tidak akan tersedia.")
    except Exception as e:
        st.warning(f"Gagal memuat 'HASIL_MATCHING': {e}")

    return rekap_df.sort_values('Tanggal'), matches_df

# ================================
# FUNGSI UNTUK PROSES UPDATE HARGA
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
        prog = st.progress(0, text="Memuat data sumber...")
    source_df = load_source_data_for_update(gc, spreadsheet_key)
    if source_df is None or source_df.empty:
        with placeholder.container(): st.error("Gagal memuat data sumber. Batal."); return
    
    my_store_name = "DB KLIK"
    my_store_df = source_df[source_df['Toko'] == my_store_name]
    competitor_df = source_df[source_df['Toko'] != my_store_name]
    if my_store_df.empty or competitor_df.empty:
        with placeholder.container(): st.warning("Data toko Anda atau kompetitor tidak cukup."); return
        
    competitor_products_list = competitor_df['Nama Produk'].unique().tolist()
    all_matches, total = [], len(my_store_df)
    for i, (_, row) in enumerate(my_store_df.iterrows()):
        prog.progress(int((i / total) * 80), text=f"Mencocokkan produk {i+1}/{total}...")
        matches = process.extract(row['Nama Produk'], competitor_products_list, scorer=fuzz.token_set_ratio, limit=5, score_cutoff=score_cutoff)
        for match_name, score, _ in matches:
            matched_rows = competitor_df[competitor_df['Nama Produk'] == match_name]
            for _, mrow in matched_rows.iterrows():
                all_matches.append({'Produk Toko Saya': row['Nama Produk'], 'Harga Toko Saya': int(row['Harga']), 'Produk Kompetitor': match_name, 'Harga Kompetitor': int(mrow['Harga']), 'Toko Kompetitor': mrow['Toko'], 'Skor Kemiripan': int(score), 'Tanggal_Update': datetime.now().strftime('%Y-%m-%d')})
    
    prog.progress(90, text="Menyimpan hasil ke Google Sheets...")
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
            prog.progress(100)
            with placeholder.container(): st.success(f"Pembaruan selesai! {len(results_df)} baris hasil disimpan.")
        else:
            with placeholder.container(): st.warning("Tidak ditemukan pasangan produk yang cocok dengan cutoff saat ini.")
    except Exception as e:
        with placeholder.container(): st.error(f"Gagal menyimpan hasil: {e}")

# ================================
# APLIKASI UTAMA
# ================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

SPREADSHEET_KEY = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if not st.session_state.data_loaded:
    _, col_center, _ = st.columns([2, 3, 2])
    with col_center:
        if st.button("Tarik Data & Mulai Analisis 🚀", type="primary"):
            df, matches_df = load_all_data(SPREADSHEET_KEY)
            if df is not None and not df.empty:
                st.session_state.df = df
                st.session_state.matches_df = matches_df
                st.session_state.data_loaded = True
                st.rerun()
            else:
                st.error("Gagal memuat data. Periksa koneksi atau KEY spreadsheet.")
    st.info("👆 Klik tombol untuk menarik semua data yang diperlukan untuk analisis.")
    st.stop()

# Ambil data dari state
df = st.session_state.df
matches_df = st.session_state.matches_df

# ================================
# SIDEBAR
# ================================
st.sidebar.header("Kontrol & Filter")
min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
if len(selected_date_range) != 2: st.sidebar.warning("Pilih 2 tanggal."); st.stop()
start_date, end_date = selected_date_range

accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Tampilan (%)", 80, 100, 91, 1)

st.sidebar.divider()
st.sidebar.header("Pembaruan Data Perbandingan")
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

if st.sidebar.button("Perbarui Perbandingan Harga Sekarang 🚀", type="primary"):
    gc = connect_to_gsheets()
    run_price_comparison_update(gc, SPREADSHEET_KEY, score_cutoff=88) # Cutoff saat update bisa di-hardcode
    # Reload data to get the new matches
    st.cache_data.clear()
    df, matches_df = load_all_data(SPREADSHEET_KEY)
    st.session_state.df = df
    st.session_state.matches_df = matches_df
    st.rerun()


# ================================
# PERSIAPAN DATA UNTUK TABS
# ================================
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty: st.error("Tidak ada data di rentang tanggal ini."); st.stop()

df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
my_store_name = "DB KLIK"
main_store_df = df_filtered[df_filtered['Toko'] == my_store_name]
competitor_df = df_filtered[df_filtered['Toko'] != my_store_name]

# Mengambil data snapshot terakhir untuk analisis
latest_entries_weekly = df_filtered.loc[df_filtered.groupby(['Minggu', 'Toko', 'Nama Produk'])['Tanggal'].idxmax()]
latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
main_store_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == my_store_name]

# ================================
# TABS
# ================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "⭐ Analisis Toko Saya", 
    "⚖️ Perbandingan Harga", 
    "🏆 Analisis Brand Kompetitor",
    "📦 Status Stok",
    "📈 Kinerja Penjualan",
    "🆕 Produk Baru"
])

with tab1:
    st.header(f"Analisis Kinerja Toko: {my_store_name}")
    st.subheader("Analisis Kategori Terlaris (Berdasarkan Omzet)")
    if 'KATEGORI' in main_store_latest_overall.columns:
        main_store_cat = main_store_latest_overall.copy()
        main_store_cat['KATEGORI'] = main_store_cat['KATEGORI'].replace('', 'Lainnya').fillna('Lainnya')
        category_sales = main_store_cat.groupby('KATEGORI')['Omzet'].sum().reset_index()
        
        if not category_sales.empty:
            cat_sales_sorted = category_sales.sort_values('Omzet', ascending=False).head(10)
            fig_cat = px.bar(cat_sales_sorted, x='KATEGORI', y='Omzet', title='Top 10 Kategori Berdasarkan Omzet', text_auto='.2s')
            st.plotly_chart(fig_cat, use_container_width=True)

            st.subheader("Lihat Produk Terlaris per Kategori")
            category_list = category_sales.sort_values('Omzet', ascending=False)['KATEGORI'].tolist()
            selected_category = st.selectbox("Pilih Kategori:", options=category_list)
            if selected_category:
                products_in_category = main_store_cat[main_store_cat['KATEGORI'] == selected_category]
                top_products_in_category = products_in_category.sort_values('Terjual per Bulan', ascending=False)
                display_table = top_products_in_category[['Nama Produk', 'Harga', 'Terjual per Bulan', 'Omzet']].copy()
                display_table['Harga'] = display_table['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                display_table['Omzet'] = display_table['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                st.dataframe(display_table, use_container_width=True, hide_index=True)
    else:
        st.warning("Kolom 'KATEGORI' tidak ditemukan pada data toko Anda.")

with tab2:
    st.header(f"Perbandingan Produk '{my_store_name}' dengan Kompetitor")
    st.info("Hasil perbandingan diambil dari worksheet 'HASIL_MATCHING'. Gunakan sidebar untuk memperbarui.")
    
    if matches_df.empty:
        st.warning("Data perbandingan tidak tersedia. Coba jalankan pembaruan dari sidebar.")
    else:
        product_list = sorted(main_store_df['Nama Produk'].unique())
        selected_product = st.selectbox("Pilih produk dari toko Anda:", product_list)
        
        if selected_product:
            product_info_list = main_store_latest_overall[main_store_latest_overall['Nama Produk'] == selected_product]
            if not product_info_list.empty:
                product_info = product_info_list.iloc[0]
                my_price = product_info['Harga']

                # Memeriksa apakah harga produk saya valid sebelum ditampilkan
                if pd.notna(my_price):
                    st.metric(f"Harga di {my_store_name}", f"Rp {int(my_price):,}")
                else:
                    st.metric(f"Harga di {my_store_name}", "Harga tidak tersedia")
                st.divider()

                matches_for_product = matches_df[
                    (matches_df['Produk Toko Saya'] == selected_product) &
                    (matches_df['Skor Kemiripan'] >= accuracy_cutoff)
                ].sort_values(by='Skor Kemiripan', ascending=False)

                st.subheader("Perbandingan di Toko Kompetitor (Hasil Matching Terakhir)")
                if matches_for_product.empty:
                    st.warning("Tidak ditemukan kecocokan di 'HASIL_MATCHING' dengan filter akurasi Anda.")
                else:
                    for _, match in matches_for_product.iterrows():
                        competitor_price = match['Harga Kompetitor']
                        
                        # Menyiapkan nilai default untuk ditampilkan jika ada data harga yang kosong
                        delta_txt = "N/A"
                        competitor_price_display = "Harga tidak tersedia"
                        
                        # Hanya hitung selisih jika kedua harga valid
                        if pd.notna(my_price) and pd.notna(competitor_price):
                            price_diff = int(competitor_price) - int(my_price)
                            delta_txt = f"Rp {price_diff:,} {'(Lebih Mahal)' if price_diff > 0 else '(Lebih Murah)' if price_diff < 0 else ''}"

                        # Format harga kompetitor jika valid
                        if pd.notna(competitor_price):
                            competitor_price_display = f"Rp {int(competitor_price):,}"

                        with st.container(border=True):
                            st.markdown(f"**{match['Toko Kompetitor']}** (Kemiripan: {int(match['Skor Kemiripan'])}%)")
                            st.markdown(f"*{match['Produk Kompetitor']}*")
                            c1, c2 = st.columns(2)
                            c1.metric("Harga Kompetitor", competitor_price_display, delta=delta_txt)
                            update_date = pd.to_datetime(match['Tanggal_Update']).strftime('%d %b %Y') if 'Tanggal_Update' in match and pd.notna(match['Tanggal_Update']) else 'N/A'
                            c2.metric("Tanggal Update", update_date)

with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    competitor_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] != my_store_name]
    if competitor_latest_overall.empty:
        st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
    else:
        competitor_list = sorted(competitor_df['Toko'].unique())
        for competitor_store in competitor_list:
            with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                single_competitor_df = competitor_latest_overall[competitor_latest_overall['Toko'] == competitor_store]
                brand_analysis = single_competitor_df.groupby('Brand').agg(Total_Omzet=('Omzet', 'sum')).reset_index().sort_values("Total_Omzet", ascending=False)
                
                if not brand_analysis.empty:
                    st.dataframe(brand_analysis.head(10), use_container_width=True, hide_index=True)
                    fig_pie_comp = px.pie(brand_analysis.head(7), names='Brand', values='Total_Omzet', title=f'Distribusi Omzet Top 7 Brand di {competitor_store}')
                    st.plotly_chart(fig_pie_comp, use_container_width=True)
                else:
                    st.info("Tidak ada data brand untuk toko ini.")

with tab4:
    st.header("Analisis Status Stok Produk")
    st.subheader("Tren Status Stok Mingguan per Toko")
    stock_trends = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
    if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
    if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
    stock_trends_melted = stock_trends.melt(id_vars=['Minggu', 'Toko'], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
    
    fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
    st.plotly_chart(fig_stock_trends, use_container_width=True)
    with st.expander("Lihat Tabel Data Stok"):
        st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)

with tab5:
    st.header("Analisis Kinerja Penjualan")
    st.subheader("Perbandingan Omzet Mingguan Antar Toko")
    all_stores_latest_per_week = latest_entries_weekly.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
    fig_weekly_omzet = px.line(all_stores_latest_per_week, x='Minggu', y='Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
    st.plotly_chart(fig_weekly_omzet, use_container_width=True)
    
    with st.expander("Lihat Rincian Omzet per Tanggal (Pivot)"):
        omzet_pivot = df_filtered.pivot_table(index='Toko', columns='Tanggal', values='Omzet', aggfunc='sum').fillna(0)
        omzet_pivot.columns = [col.strftime('%d-%b') for col in omzet_pivot.columns]
        st.dataframe(omzet_pivot)

with tab6:
    st.header("Analisis Produk Baru Mingguan")
    weeks = sorted(df_filtered['Minggu'].unique())
    if len(weeks) < 2:
        st.warning("Butuh setidaknya 2 minggu data dalam rentang yang dipilih untuk melakukan perbandingan.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            week_before = st.selectbox("Pilih Minggu Pembanding:", options=weeks)
        with col2:
            week_after = st.selectbox("Pilih Minggu Penentu:", options=weeks, index=len(weeks)-1)

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
                        new_products_df = df_filtered[(df_filtered['Nama Produk'].isin(new_products)) & (df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)].copy()
                        new_products_df['Harga'] = new_products_df['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'Harga', 'Stok', 'Brand']].drop_duplicates(), hide_index=True)


