# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 3 (SIMPLES + UPDATE OTOMATIS)
#  Dibuat oleh: Firman & Asisten AI Gemini (merge versi lama + fitur otomatis dari versi baru)
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
# PAGE CONFIG
# ================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis v3")

# ================================
# HELPERS: CONNECT TO GSUITES / GSHEETS
# ================================
@st.cache_resource(show_spinner="Menghubungkan ke Google Sheets...")
def connect_to_gsheets():
    """
    Buat koneksi aman ke Google Sheets menggunakan st.secrets (service account fields).
    Pastikan st.secrets berisi kunci GCP sesuai format.
    """
    creds_dict = {
        "type": st.secrets["gcp_type"],
        "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"],
        # private_key_raw disimpan tanpa baris baru; replace -> \n
        "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
        "client_email": st.secrets["gcp_client_email"],
        "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"],
        "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    gc = gspread.service_account_from_dict(creds_dict)
    return gc

# ================================
# LOAD ALL DATA (Sederhana tapi robust)
# ================================
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_all_data(spreadsheet_key): # PERUBAHAN 1: Hapus 'gc' dari argumen
    """
    Memuat semua REKAP sheet + DATABASE + HASIL_MATCHING (jika ada).
    Menyerupai alur versi lama, tapi memberi kolom konsisten.
    """
    gc = connect_to_gsheets() # PERUBAHAN 2: Panggil koneksi di dalam fungsi

    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
    except Exception as e:
        st.error(f"GAGAL KONEKSI/OPEN SPREADSHEET: {e}")
        return None, None, None

    # list sheet yang akan dicoba (mirip kedua versi)
    sheet_names = [
        "DATABASE", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
        "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
        "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS",
        "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
        "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS"
    ]

    rekap_list_df = []
    database_df = pd.DataFrame()

    for sheet_name in sheet_names:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            all_values = worksheet.get_all_values()
            if not all_values or len(all_values) < 2:
                # kosong -> skip
                continue
            header = all_values[0]
            data = all_values[1:]
            df_sheet = pd.DataFrame(data, columns=header)
            # drop stray empty column name ''
            if '' in df_sheet.columns:
                df_sheet = df_sheet.drop(columns=[''])
            if "DATABASE" in sheet_name.upper():
                database_df = df_sheet
            elif "REKAP" in sheet_name.upper():
                # tentukan status dari nama sheet READY/HABIS bila belum ada kolom
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                toko_name = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
                df_sheet['Toko'] = toko_name
                if 'Status' not in df_sheet.columns:
                    df_sheet['Status'] = 'Tersedia' if "READY" in sheet_name.upper() else 'Habis'
                rekap_list_df.append(df_sheet)
        except gspread.exceptions.WorksheetNotFound:
            # abaikan jika tidak ada; tetap lanjut
            continue
        except Exception as e:
            # log kecil tapi lanjutkan
            st.warning(f"Gagal baca sheet '{sheet_name}': {e}")
            continue

    if not rekap_list_df:
        st.error("Tidak ada data REKAP yang berhasil dimuat dari spreadsheet.")
        return None, None, None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)

    # standardisasi nama kolom
    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]
    final_rename = {
        'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal',
        'HARGA': 'Harga', 'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'
    }
    rekap_df.rename(columns=final_rename, inplace=True)

    # sanitasi & typing
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    if 'Tanggal' in rekap_df.columns:
        rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    if 'Harga' in rekap_df.columns:
        rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    if 'Terjual per Bulan' in rekap_df.columns:
        rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)

    # Drop rows penting kosong
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga', 'Toko'], inplace=True)

    # Brand fallback: ambil kata pertama jika tidak ada kolom Brand
    if 'Brand' not in rekap_df.columns or rekap_df['Brand'].isnull().all():
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()

    # Omzet jika terjual tersedia
    if 'Terjual per Bulan' in rekap_df.columns:
        rekap_df['Omzet'] = (rekap_df['Harga'].fillna(0) * rekap_df['Terjual per Bulan'].fillna(0)).astype(int)
    else:
        rekap_df['Omzet'] = 0

    # load hasil matching (opsional)
    matches_df = pd.DataFrame()
    try:
        matches_sheet = spreadsheet.worksheet("HASIL_MATCHING")
        matches_df = pd.DataFrame(matches_sheet.get_all_records())
        # standardisasi kolom jika ada
        if not matches_df.empty:
            matches_df.columns = [str(c).strip() for c in matches_df.columns]
    except gspread.exceptions.WorksheetNotFound:
        # sheet belum ada -> wajar
        matches_df = pd.DataFrame()
    except Exception as e:
        st.warning(f"Gagal memuat sheet 'HASIL_MATCHING': {e}")
        matches_df = pd.DataFrame()

    return rekap_df, database_df, matches_df

# ================================
# LOAD SOURCE DATA FOR UPDATE (ringkas)
# ================================
def load_source_data_for_update(gc, spreadsheet_key):
    """
    Versi ringan: ambil semua sheet REKAP yang ada, preprocessing dasar,
    lalu ambil snapshot terbaru per (Toko, Nama Produk).
    Digunakan untuk proses pembaruan otomatis/manual.
    """
    spreadsheet = gc.open_by_key(spreadsheet_key)
    # cari worksheet yang mengandung 'REKAP'
    sheet_objs = [s for s in spreadsheet.worksheets() if "REKAP" in s.title.upper()]
    rekap_list = []
    for s in sheet_objs:
        try:
            vals = s.get_all_values()
            if not vals or len(vals) < 2:
                continue
            header = vals[0]
            data = vals[1:]
            df = pd.DataFrame(data, columns=header)
            if '' in df.columns:
                df = df.drop(columns=[''])
            store_name_match = re.match(r"^(.*?) - REKAP", s.title, re.IGNORECASE)
            df['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
            if 'Status' not in df.columns:
                df['Status'] = 'Tersedia' if "READY" in s.title.upper() else 'Habis'
            rekap_list.append(df)
        except Exception:
            continue

    if not rekap_list:
        return pd.DataFrame()

    rekap_df = pd.concat(rekap_list, ignore_index=True)
    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]
    rename_map = {'NAMA': 'Nama Produk', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'TOKO': 'Toko'}
    rekap_df.rename(columns=rename_map, inplace=True)

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Toko', 'Harga'], inplace=True)

    # ambil snapshot terbaru per toko+produk
    idx = rekap_df.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()
    return rekap_df.loc[idx].reset_index(drop=True)

# ================================
# RUN PRICE COMPARISON UPDATE (simpel)
# ================================
def run_price_comparison_update(gc, spreadsheet_key, score_cutoff=88):
    """
    Proses fuzzy matching dari produk toko kita (DB KLIK) terhadap semua competitor,
    menyimpan hasil ke sheet 'HASIL_MATCHING'.
    UI: menampilkan progress sederhana via placeholder.
    """
    placeholder = st.empty()
    with placeholder.container():
        st.info("Memulai pembaruan perbandingan harga...")
        prog = st.progress(0, text="0%")

    # Muat data sumber ringkas
    source_df = load_source_data_for_update(gc, spreadsheet_key)
    if source_df is None or source_df.empty:
        with placeholder.container():
            st.error("Gagal memuat data sumber untuk update. Batal.")
        return

    # Pisah toko kita vs competitor
    my_store_name = "DB KLIK"
    my_store_df = source_df[source_df['Toko'] == my_store_name].copy()
    competitor_df = source_df[source_df['Toko'] != my_store_name].copy()

    if my_store_df.empty or competitor_df.empty:
        with placeholder.container():
            st.warning("Data toko Anda atau kompetitor tidak mencukupi untuk proses matching.")
        return

    competitor_products_list = competitor_df['Nama Produk'].unique().tolist()
    all_matches = []
    total = len(my_store_df)

    for i, (idx, row) in enumerate(my_store_df.iterrows()):
        prog.progress(int((i / total) * 80), text=f"Mencocokkan produk {i+1}/{total}")
        # ambil top N matches di atas cutoff
        matches = process.extract(row['Nama Produk'], competitor_products_list, scorer=fuzz.token_set_ratio, limit=5, score_cutoff=score_cutoff)
        for match_name, score, _ in matches:
            matched_rows = competitor_df[competitor_df['Nama Produk'] == match_name]
            for _, mrow in matched_rows.iterrows():
                all_matches.append({
                    'Produk Toko Saya': row['Nama Produk'],
                    'Harga Toko Saya': int(row['Harga']),
                    'Produk Kompetitor': match_name,
                    'Harga Kompetitor': int(mrow['Harga']),
                    'Toko Kompetitor': mrow['Toko'],
                    'Skor Kemiripan': int(score),
                    'Tanggal_Update': datetime.now().strftime('%Y-%m-%d')
                })

    prog.progress(90, text="Menyimpan hasil ke Google Sheets...")

    # simpan ke sheet HASIL_MATCHING
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
            prog.progress(100, text="Selesai. Hasil tersimpan di 'HASIL_MATCHING'.")
            with placeholder.container():
                st.success(f"Selesai: {len(results_df)} baris hasil disimpan ke 'HASIL_MATCHING'.")
        else:
            with placeholder.container():
                st.warning("Tidak ditemukan pasangan produk yang cocok dengan cutoff saat ini.")
    except Exception as e:
        with placeholder.container():
            st.error(f"Gagal menyimpan hasil: {e}")

# ================================
# UTILITY: small helpers dari versi lama
# ================================
def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'):
        return "N/A"
    elif pct_change > 0.001:
        return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001:
        return f"▼ {pct_change:.1%}"
    else:
        return f"▬ 0.0%"

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
# MAIN APP
# ================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor — v3 (Simple + Auto Update)")

SPREADSHEET_KEY = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"  # ganti sesuai milik Anda

# inisialisasi koneksi
gc = connect_to_gsheets()

# state untuk trigger update
if 'update_triggered' not in st.session_state:
    st.session_state.update_triggered = False

# tombol utama untuk tarik data / mulai analisis (mirip versi lama)
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if not st.session_state.data_loaded:
    col1, col2, col3 = st.columns([2, 3, 2])
    with col2:
        if st.button("Tarik Data & Mulai Analisis 🚀", key="load_data_v3", type="primary"):
            df, db_df, matches_df = load_all_data(SPREADSHEET_KEY)
            if df is not None and not df.empty:
                st.session_state.df = df
                st.session_state.db_df = db_df
                st.session_state.matches_df = matches_df
                st.session_state.data_loaded = True
                st.rerun()
            else:
                st.error("Gagal memuat data. Periksa akses Google Sheets dan st.secrets.")
    st.info("👆 Klik tombol untuk menarik data (semua sheet REKAP + DATABASE + HASIL_MATCHING jika ada).")
    st.stop()

# ambil dari state
df = st.session_state.df
db_df = st.session_state.db_df
matches_df = st.session_state.matches_df if 'matches_df' in st.session_state else pd.DataFrame()

# SIDEBAR: kontrol & update logic
st.sidebar.header("Kontrol & Filter")

# tanggal range
min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
if len(selected_date_range) != 2:
    st.sidebar.warning("Pilih 2 tanggal (start & end)."); st.stop()
start_date, end_date = selected_date_range

# accuracy cutoff untuk filter tampilan (bukan saat update)
accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)

# Cek tanggal terakhir source vs destination (HASIL_MATCHING)
latest_source_date = df['Tanggal'].max().date()
last_destination_update = datetime(1970,1,1).date()
if not matches_df.empty and 'Tanggal_Update' in matches_df.columns:
    try:
        matches_df['Tanggal_Update'] = pd.to_datetime(matches_df['Tanggal_Update'], errors='coerce')
        if not matches_df['Tanggal_Update'].isna().all():
            last_destination_update = matches_df['Tanggal_Update'].max().date()
    except Exception:
        last_destination_update = datetime(1970,1,1).date()

st.sidebar.info(f"Data Sumber Terbaru: **{latest_source_date.strftime('%d %b %Y')}**")
st.sidebar.info(f"Perbandingan Terakhir: **{last_destination_update.strftime('%d %b %Y')}**")

if latest_source_date > last_destination_update:
    st.sidebar.warning("Data sumber lebih baru dari hasil perbandingan.")
    if st.sidebar.button("Perbarui Sekarang 🚀", type="primary"):
        st.session_state.update_triggered = True
        # jalankan update langsung (tidak perlu rerun yang rumit)
        run_price_comparison_update(gc, SPREADSHEET_KEY, score_cutoff=accuracy_cutoff)
        # setelah selesai, reload matches_df agar UI menampilkan hasil terbaru
        _, _, new_matches_df = load_all_data(gc, SPREADSHEET_KEY)
        st.session_state.matches_df = new_matches_df
        st.success("Perbaruan selesai dan data perbandingan diperbarui di memori aplikasi.")
        st.rerun()
else:
    st.sidebar.success("Data perbandingan tampak sudah terbaru.")

if st.sidebar.button("Jalankan Pembaruan Manual (force)", type="secondary"):
    run_price_comparison_update(gc, SPREADSHEET_KEY, score_cutoff=accuracy_cutoff)
    _, _, new_matches_df = load_all_data(gc, SPREADSHEET_KEY)
    st.session_state.matches_df = new_matches_df
    st.success("Perbaruan manual selesai.")
    st.rerun()

st.sidebar.divider()
st.sidebar.header("Ekspor & Info")
st.sidebar.info(f"Baris data dalam rentang: **{len(df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)])}**")
csv_data = convert_df_for_download(df)
st.sidebar.download_button("📥 Unduh CSV (Full)", data=csv_data, file_name=f'analisis_full_{start_date}_{end_date}.csv', mime='text/csv')

# FILTER DATA untuk tampilan
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error("Tidak ada data di rentang tanggal ini."); st.stop()

df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
my_store_name = "DB KLIK"
main_store_df = df_filtered[df_filtered['Toko'] == my_store_name].copy()
competitor_df = df_filtered[df_filtered['Toko'] != my_store_name].copy()

# TABS (mirip versi lama)
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["⭐ Analisis Toko Saya", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

with tab1:
    st.header(f"Analisis Kinerja: {my_store_name}")
    # contoh ringkas: top products & brand pie
    # top products
    latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
    main_store_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == my_store_name]

    st.subheader("Produk Terlaris (Top 15 berdasarkan Terjual per Bulan)")
    if not main_store_latest_overall.empty:
        top_products = main_store_latest_overall.sort_values('Terjual per Bulan', ascending=False).head(15).copy()
        top_products['Harga_fmt'] = top_products['Harga'].apply(lambda x: f"Rp {int(x):,}")
        top_products['Omzet_fmt'] = top_products['Omzet'].apply(lambda x: f"Rp {int(x):,}")
        st.dataframe(top_products[['Nama Produk','Harga_fmt','Omzet_fmt','Terjual per Bulan']].rename(columns={'Harga_fmt':'Harga','Omzet_fmt':'Omzet'}), use_container_width=True, hide_index=True)
    else:
        st.info("Tidak ada data produk untuk toko Anda pada rentang tanggal ini.")

    st.subheader("Distribusi Omzet Brand (Snapshot Terakhir)")
    brand_omzet_main = main_store_latest_overall.groupby('Brand')['Omzet'].sum().reset_index().sort_values('Omzet', ascending=False)
    if not brand_omzet_main.empty:
        fig = px.pie(brand_omzet_main.head(7), names='Brand', values='Omzet', title='Top 7 Brand (Omzet)')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Tidak ada data omzet brand.")

with tab2:
    st.header(f"Perbandingan Produk '{my_store_name}' dengan Kompetitor")
    st.info("Hasil perbandingan diambil dari worksheet 'HASIL_MATCHING' jika tersedia. Gunakan tombol sidebar untuk memperbarui.")
    if 'matches_df' not in st.session_state or st.session_state.matches_df is None or st.session_state.matches_df.empty:
        st.warning("Data perbandingan belum tersedia. Jalankan pembaruan dari sidebar.")
    else:
        matches_df = st.session_state.matches_df
        product_list = sorted(main_store_df['Nama Produk'].unique())
        selected_product = st.selectbox("Pilih produk dari toko Anda:", product_list)
        if selected_product:
            product_info_list = main_store_latest_overall[main_store_latest_overall['Nama Produk'] == selected_product]
            if not product_info_list.empty:
                product_info = product_info_list.iloc[0]
                col1, col2, col3 = st.columns(3)
                col1.metric(f"Harga di {my_store_name}", f"Rp {int(product_info['Harga']):,}")
                col2.metric("Status", product_info.get('Status','N/A'))
                col3.metric("Stok", product_info.get('Stok','N/A'))
                st.markdown("---")
                # filter matches
                matches_for_product = matches_df[
                    (matches_df['Produk Toko Saya'] == selected_product) &
                    (matches_df['Skor Kemiripan'] >= accuracy_cutoff)
                ].sort_values(by='Skor Kemiripan', ascending=False)
                if matches_for_product.empty:
                    st.warning("Tidak ditemukan kecocokan pada HASIL_MATCHING dengan filter akurasi Anda.")
                else:
                    for _, match in matches_for_product.iterrows():
                        price_diff = int(match['Harga Kompetitor']) - int(product_info['Harga'])
                        delta_txt = f"Rp {price_diff:,} "
                        delta_txt += "(Lebih Mahal)" if price_diff > 0 else "(Lebih Murah)" if price_diff < 0 else "(Sama Harga)"
                        with st.container():
                            st.markdown(f"**{match['Toko Kompetitor']}** — Kemiripan: {int(match['Skor Kemiripan'])}%")
                            st.markdown(f"*{match['Produk Kompetitor']}*")
                            c1, c2 = st.columns(2)
                            c1.metric("Harga Kompetitor", f"Rp {int(match['Harga Kompetitor']):,}", delta=delta_txt)
                            c2.metric("Tanggal Update", match.get('Tanggal_Update','N/A'))

with tab3:
    st.header("Analisis Brand Kompetitor (Snapshot Terakhir)")
    latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
    competitor_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] != my_store_name]
    if competitor_latest_overall.empty:
        st.warning("Tidak ada data kompetitor.")
    else:
        brand_analysis = competitor_latest_overall.groupby(['Toko','Brand']).agg(Total_Omzet=('Omzet','sum')).reset_index().sort_values('Total_Omzet', ascending=False)
        st.dataframe(brand_analysis, use_container_width=True, hide_index=True)

with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    stock_trends = df_filtered.groupby(['Minggu','Toko','Status']).size().unstack(fill_value=0).reset_index()
    if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
    if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
    melted = stock_trends.melt(id_vars=['Minggu','Toko'], value_vars=['Tersedia','Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
    fig = px.line(melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Stok: Tersedia vs Habis per Minggu')
    st.plotly_chart(fig, use_container_width=True)

with tab5:
    st.header("Analisis Kinerja Penjualan (Semua Toko)")
    latest_weekly = df_filtered.loc[df_filtered.groupby(['Minggu','Toko','Nama Produk'])['Tanggal'].idxmax()]
    all_stores_week = latest_weekly.groupby(['Minggu','Toko'])['Omzet'].sum().reset_index()
    fig_week = px.line(all_stores_week, x='Minggu', y='Omzet', color='Toko', markers=True, title='Omzet Mingguan Antar Toko')
    st.plotly_chart(fig_week, use_container_width=True)

with tab6:
    st.header("Analisis Produk Baru Mingguan")
    weeks = sorted(df_filtered['Minggu'].unique())
    if len(weeks) < 2:
        st.info("Perlu minimal 2 minggu data untuk analisis produk baru.")
    else:
        col1, col2 = st.columns(2)
        week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0)
        week_after = col2.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)
        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            stores = sorted(df_filtered['Toko'].unique())
            for s in stores:
                with st.expander(f"Produk Baru di {s}"):
                    set_before = set(df_filtered[(df_filtered['Toko']==s) & (df_filtered['Minggu']==week_before) & (df_filtered['Status']=='Tersedia')]['Nama Produk'])
                    set_after = set(df_filtered[(df_filtered['Toko']==s) & (df_filtered['Minggu']==week_after) & (df_filtered['Status']=='Tersedia')]['Nama Produk'])
                    new_products = set_after - set_before
                    if not new_products:
                        st.write("Tidak ada produk baru.")
                    else:
                        st.write(f"Ditemukan {len(new_products)} produk baru.")
                        new_df = df_filtered[df_filtered['Nama Produk'].isin(new_products) & (df_filtered['Toko']==s)]
                        new_df['Harga_fmt'] = new_df['Harga'].apply(lambda x: f"Rp {int(x):,}")
                        st.dataframe(new_df[['Nama Produk','Harga_fmt','Stok','Brand']].rename(columns={'Harga_fmt':'Harga'}), use_container_width=True, hide_index=True)


