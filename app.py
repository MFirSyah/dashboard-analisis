# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 5.3 (FINAL & TERINTEGRASI)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi ini mengintegrasikan kode 6 tab analisis ke dalam kerangka kerja baru,
#  memperbaiki masalah indentasi, dan menambahkan definisi variabel yang diperlukan.
# ===================================================================================

import streamlit as st
import pandas as pd
from rapidfuzz import process, fuzz
import plotly.express as px
import re
import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe
import numpy as np # Diperlukan untuk penanganan numerik

# ===============================
# KONFIGURASI HALAMAN
# ===============================
st.set_page_config(layout="wide", page_title="Dashboard Analisis v5.3")

# ===============================
# FUNGSI BANTUAN
# ===============================
def format_wow_growth(value):
    if pd.isna(value):
        return "N/A"
    return f"{value:+.1%}"

def style_wow_growth(value):
    if isinstance(value, str):
        if value.startswith('+'):
            return 'color: green;'
        elif value.startswith('-'):
            return 'color: red;'
    return ''

# ===============================
# FUNGSI KONEKSI GOOGLE SHEETS
# ===============================
@st.cache_resource(show_spinner="Menghubungkan ke Google Sheets...")
def connect_to_gsheets():
    """
    Membuat koneksi aman ke Google Sheets menggunakan st.secrets.
    """
    creds_dict = {
        "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key"],
        "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    sa = gspread.service_account_from_dict(creds_dict)
    return sa

@st.cache_resource(show_spinner="Membuka Spreadsheet...")
def open_spreadsheet(_sa, spreadsheet_id):
    """
    Membuka spreadsheet berdasarkan ID.
    """
    return _sa.open_by_key(spreadsheet_id)

# ===============================
# FUNGSI PEMROSESAN DATA
# ===============================
@st.cache_data(ttl=600, show_spinner="Memuat dan memproses data dari file lokal...")
def load_and_process_data_local():
    """
    Memuat data dari file CSV lokal dan melakukan pemrosesan dasar.
    """
    portal_files = {
        "LOGITECH": ["DATA_REKAP.xlsx - LOGITECH - REKAP - READY.csv", "DATA_REKAP.xlsx - LOGITECH - REKAP - HABIS.csv"],
        "DB KLIK": ["DATA_REKAP.xlsx - DB KLIK - REKAP - READY.csv", "DATA_REKAP.xlsx - DB KLIK - REKAP - HABIS.csv"],
        "ABDITAMA": ["DATA_REKAP.xlsx - ABDITAMA - REKAP - READY.csv", "DATA_REKAP.xlsx - ABDITAMA - REKAP - HABIS.csv"],
        "LEVEL99": ["DATA_REKAP.xlsx - LEVEL99 - REKAP - READY.csv", "DATA_REKAP.xlsx - LEVEL99 - REKAP - HABIS.csv"],
        "IT SHOP": ["DATA_REKAP.xlsx - IT SHOP - REKAP - READY.csv", "DATA_REKAP.xlsx - IT SHOP - REKAP - HABIS.csv"],
        "JAYA PC": ["DATA_REKAP.xlsx - JAYA PC - REKAP - READY.csv", "DATA_REKAP.xlsx - JAYA PC - REKAP - HABIS.csv"],
        "MULTIFUNGSI": ["DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - READY.csv", "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - HABIS.csv"],
        "TECH ISLAND": ["DATA_REKAP.xlsx - TECH ISLAND - REKAP - READY.csv", "DATA_REKAP.xlsx - TECH ISLAND - REKAP - HABIS.csv"],
        "GG STORE": ["DATA_REKAP.xlsx - GG STORE - REKAP - READY.csv", "DATA_REKAP.xlsx - GG STORE - REKAP - HABIS.csv"],
        "SURYA MITRA": ["DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - RE.csv", "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - HA.csv"]
    }

    all_data = []
    for toko, files in portal_files.items():
        for i, file in enumerate(files):
            try:
                df_temp = pd.read_csv(file)
                df_temp['Toko'] = toko
                df_temp['Status'] = 'Tersedia' if i == 0 else 'Habis'
                if 'NAMA' in df_temp.columns:
                    df_temp.rename(columns={'NAMA': 'Nama Produk'}, inplace=True)
                all_data.append(df_temp)
            except FileNotFoundError:
                st.warning(f"File tidak ditemukan: {file}. Melewati...")
            except Exception as e:
                st.error(f"Gagal memuat {file}: {e}")

    if not all_data:
        st.error("Tidak ada data portal yang berhasil dimuat. Dashboard tidak dapat ditampilkan.")
        return pd.DataFrame(), pd.DataFrame()

    df_combined = pd.concat(all_data, ignore_index=True)

    df_combined['TANGGAL'] = pd.to_datetime(df_combined['TANGGAL'], errors='coerce')
    df_combined['Harga'] = pd.to_numeric(df_combined['HARGA'], errors='coerce')
    df_combined['Terjual'] = pd.to_numeric(df_combined['TERJUAL/BLN'], errors='coerce').fillna(0)
    df_combined['Nama Produk'] = df_combined['Nama Produk'].astype(str).str.strip()
    df_combined['BRAND'].fillna('TIDAK DIKETAHUI', inplace=True)
    
    df_combined.dropna(subset=['TANGGAL'], inplace=True)
    df_combined['Minggu'] = df_combined['TANGGAL'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
    df_combined['Bulan'] = df_combined['TANGGAL'].dt.to_period('M').apply(lambda r: r.start_time).dt.date

    try:
        df_matching = pd.read_csv("DATA_REKAP.xlsx - HASIL_MATCHING.csv")
    except FileNotFoundError:
        st.warning("File hasil matching tidak ditemukan.")
        df_matching = pd.DataFrame()

    return df_combined, df_matching

def clean_price(price):
    if isinstance(price, (int, float)):
        return price
    if isinstance(price, str):
        price_cleaned = re.sub(r'[^\d]', '', price)
        if price_cleaned:
            return pd.to_numeric(price_cleaned, errors='coerce')
    return np.nan

# ============================================
# FUNGSI UNTUK HALAMAN DAFTAR HPP PRODUK (FITUR BARU)
# ============================================
def display_hpp_analysis():
    st.title("Daftar Harga Pokok Penjualan (HPP) Produk")
    st.info("""
    Halaman ini membandingkan harga produk yang ditemukan di berbagai portal dengan database HPP internal.
    - **Prioritas HPP:** Sistem akan menggunakan **HPP (LATEST)** terlebih dahulu. Jika tidak tersedia, **HPP (AVERAGE)** akan digunakan.
    - **Pencocokan:** Produk dicocokkan menggunakan algoritma kemiripan teks yang canggih untuk akurasi tinggi.
    """)

    try:
        df_database = pd.read_csv("DATA_REKAP.xlsx - DATABASE.csv")
        st.success("File DATABASE.csv berhasil dimuat.")
    except FileNotFoundError:
        st.error("File 'DATA_REKAP.xlsx - DATABASE.csv' tidak ditemukan. Analisis HPP tidak dapat dilanjutkan.")
        return

    portal_files_ready = [
        ("LOGITECH", "DATA_REKAP.xlsx - LOGITECH - REKAP - READY.csv"),
        ("DB KLIK", "DATA_REKAP.xlsx - DB KLIK - REKAP - READY.csv"),
        ("ABDITAMA", "DATA_REKAP.xlsx - ABDITAMA - REKAP - READY.csv"),
        ("LEVEL99", "DATA_REKAP.xlsx - LEVEL99 - REKAP - READY.csv"),
        ("IT SHOP", "DATA_REKAP.xlsx - IT SHOP - REKAP - READY.csv"),
        ("JAYA PC", "DATA_REKAP.xlsx - JAYA PC - REKAP - READY.csv"),
        ("MULTIFUNGSI", "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - READY.csv"),
        ("TECH ISLAND", "DATA_REKAP.xlsx - TECH ISLAND - REKAP - READY.csv"),
        ("GG STORE", "DATA_REKAP.xlsx - GG STORE - REKAP - READY.csv"),
        ("SURYA MITRA", "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - RE.csv")
    ]
    
    all_portal_data = []
    for toko, file in portal_files_ready:
        try:
            df_temp = pd.read_csv(file)
            df_temp['NAMA_PORTAL'] = toko
            all_portal_data.append(df_temp)
        except FileNotFoundError:
            continue
    
    if not all_portal_data:
        st.warning("Tidak ada file data portal 'READY' yang ditemukan.")
        return

    df_portals = pd.concat(all_portal_data, ignore_index=True)
    df_portals.rename(columns={'NAMA': 'NAMA_PRODUK_PORTAL', 'HARGA': 'HARGA_PORTAL'}, inplace=True)

    below_hpp_list = []
    not_found_list = []

    df_database['NAMA_CLEAN'] = df_database['NAMA'].astype(str)
    db_product_list = df_database['NAMA_CLEAN'].tolist()

    df_database['HPP (LATEST)'] = pd.to_numeric(df_database['HPP (LATEST)'], errors='coerce')
    df_database['HPP (AVERAGE)'] = pd.to_numeric(df_database['HPP (AVERAGE)'], errors='coerce')
    df_portals['HARGA_PORTAL'] = pd.to_numeric(df_portals['HARGA_PORTAL'], errors='coerce')
    
    with st.spinner("Melakukan pencocokan dan analisis HPP... Ini mungkin memakan waktu beberapa saat."):
        for _, row in df_portals.iterrows():
            portal_product_name = row['NAMA_PRODUK_PORTAL']
            portal_price = row['HARGA_PORTAL']
            
            if pd.isna(portal_price) or not portal_product_name:
                continue

            match = process.extractOne(portal_product_name, db_product_list, scorer=fuzz.WRatio, score_cutoff=88)

            if match:
                matched_product_name, _, index = match
                db_row = df_database.iloc[index]
                
                hpp_latest = db_row['HPP (LATEST)']
                hpp_average = db_row['HPP (AVERAGE)']
                
                hpp_to_use = hpp_latest if pd.notna(hpp_latest) and hpp_latest > 0 else (hpp_average if pd.notna(hpp_average) and hpp_average > 0 else None)
                
                if hpp_to_use:
                    if portal_price < hpp_to_use:
                        selisih = portal_price - hpp_to_use
                        below_hpp_list.append({
                            "Tanggal": row['TANGGAL'], "Nama Portal": row['NAMA_PORTAL'],
                            "Nama Produk Portal": portal_product_name, "Nama Produk (DB)": matched_product_name,
                            "Harga Portal": portal_price, "HPP Digunakan": hpp_to_use,
                            "Selisih (Harga - HPP)": selisih, "Note": "Lebih Murah dari HPP"
                        })
                else:
                    not_found_list.append({
                        "Tanggal": row['TANGGAL'], "Nama Portal": row['NAMA_PORTAL'],
                        "Nama Produk Portal": portal_product_name, "Status": "Produk ditemukan di DB, tapi tidak memiliki data HPP"
                    })
            else:
                not_found_list.append({
                    "Tanggal": row['TANGGAL'], "Nama Portal": row['NAMA_PORTAL'],
                    "Nama Produk Portal": portal_product_name, "Status": "Produk tidak cocok dengan data di Database"
                })

    st.subheader("Tabel Produk dengan Harga di Bawah HPP")
    if below_hpp_list:
        df_below_hpp = pd.DataFrame(below_hpp_list)
        df_below_hpp_display = df_below_hpp.copy()
        for col in ["Harga Portal", "HPP Digunakan", "Selisih (Harga - HPP)"]:
            df_below_hpp_display[col] = df_below_hpp_display[col].apply(lambda x: f"Rp {int(x):,}".replace(",", "."))
        st.dataframe(df_below_hpp_display, use_container_width=True)
    else:
        st.write("Tidak ada produk yang ditemukan dengan harga di bawah HPP.")

    st.subheader("Tabel Produk Tidak Ditemukan atau Tanpa Data HPP")
    if not_found_list:
        df_not_found = pd.DataFrame(not_found_list)
        st.dataframe(df_not_found, use_container_width=True)
    else:
        st.write("Semua produk berhasil diproses dan memiliki data HPP.")

# ===============================
# MAIN APP EXECUTION
# ===============================
def main():
    df, df_matching = load_and_process_data_local()

    st.sidebar.title("Navigasi Utama")
    page = st.sidebar.radio("Pilih Halaman:", ("ANALISIS PENJUALAN", "DAFTAR HPP PRODUK"))

    if page == "ANALISIS PENJUALAN":
        if df.empty:
            st.error("Data penjualan tidak dapat dimuat. Halaman tidak bisa ditampilkan.")
            return

        # =======================================================================
        # PERSIAPAN DATA DAN FILTER UNTUK HALAMAN ANALISIS PENJUALAN
        # =======================================================================
        st.sidebar.header("Filter Toko Utama")
        all_stores_list = sorted(df['Toko'].unique())
        if not all_stores_list:
            st.error("Tidak ada data toko yang ditemukan.")
            return
            
        my_store_name = st.sidebar.selectbox("Pilih Toko Anda:", options=all_stores_list, index=0)

        st.sidebar.header("Filter Analisis Kompetitor")
        all_other_stores = [t for t in all_stores_list if t != my_store_name]
        selected_stores_competitor = st.sidebar.multiselect("Pilih Toko Kompetitor", options=all_other_stores, default=all_other_stores)

        all_brands = sorted(df['BRAND'].unique())
        selected_brands = st.sidebar.multiselect("Pilih Brand", options=all_brands, default=all_brands[:10])

        accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Matching (%)", 70, 100, 90)

        # PENYIAPAN DATAFRAME BERDASARKAN FILTER
        main_store_df = df[df['Toko'] == my_store_name].copy()
        competitor_df = df[df['Toko'].isin(selected_stores_competitor)].copy()
        
        stores_to_filter = [my_store_name] + selected_stores_competitor
        df_filtered = df[df['Toko'].isin(stores_to_filter) & df['BRAND'].isin(selected_brands)].copy()

        matches_df = df_matching

        latest_date_main = main_store_df['Tanggal'].max()
        main_store_latest_overall = main_store_df[main_store_df['Tanggal'] == latest_date_main].copy()

        if not competitor_df.empty:
            latest_date_competitor = competitor_df['Tanggal'].max()
            competitor_latest_overall = competitor_df[competitor_df['Tanggal'] == latest_date_competitor].copy()
        else:
            competitor_latest_overall = pd.DataFrame()

        latest_entries_weekly = df_filtered.loc[df_filtered.groupby(['Minggu', 'Toko', 'Nama Produk'])['Tanggal'].idxmax()].copy()

        # Menghitung Omzet dan standarisasi nama kolom
        for temp_df in [df_filtered, main_store_latest_overall, competitor_latest_overall, main_store_df, competitor_df, latest_entries_weekly]:
            if not temp_df.empty:
                temp_df['Omzet'] = temp_df['Harga'] * temp_df['Terjual']
                if 'Terjual' in temp_df.columns and 'Terjual per Bulan' not in temp_df.columns:
                    temp_df.rename(columns={'Terjual': 'Terjual per Bulan'}, inplace=True)
                if 'BRAND' in temp_df.columns and 'Brand' not in temp_df.columns:
                    temp_df.rename(columns={'BRAND': 'Brand'}, inplace=True)
        
        # =======================================================================
        # KODE 6 TAB ANALISIS ANDA DIMULAI DI SINI
        # =======================================================================
        st.title("Dashboard Analisis Penjualan & Kompetitor")

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

                    st.markdown("##### Rincian Data Omzet per Kategori")
                    table_cat_sales = cat_sales_sorted.copy()
                    table_cat_sales['Omzet'] = table_cat_sales['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                    st.dataframe(table_cat_sales, use_container_width=True, hide_index=True)

                    st.markdown("---")
                    st.subheader("Lihat Produk Terlaris per Kategori")
                    category_list = category_sales.sort_values('Omzet', ascending=False)['KATEGORI'].tolist()
                    selected_category = st.selectbox("Pilih Kategori untuk melihat produk terlaris:", options=category_list)

                    if selected_category:
                        products_in_category = main_store_cat[main_store_cat['KATEGORI'] == selected_category].copy()
                        top_products_in_category = products_in_category.sort_values('Terjual per Bulan', ascending=False)

                        if top_products_in_category.empty:
                            st.info(f"Tidak ada produk terlaris untuk kategori '{selected_category}'.")
                        else:
                            columns_to_display = ['Nama Produk', 'SKU', 'Harga', 'Terjual per Bulan', 'Omzet']
                            if 'SKU' not in top_products_in_category.columns:
                                top_products_in_category['SKU'] = 'N/A'
                            
                            display_table = top_products_in_category[columns_to_display].copy()
                            display_table['Harga'] = display_table['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                            display_table['Omzet'] = display_table['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                            st.dataframe(display_table, use_container_width=True, hide_index=True)
                else:
                    st.info("Tidak ada data omzet per kategori untuk ditampilkan.")
            else:
                st.warning("Kolom 'KATEGORI' tidak ditemukan pada data toko Anda. Analisis ini dilewati.")

            st.subheader(f"{section_counter}. Produk Terlaris")
            section_counter += 1
            top_products = main_store_latest_overall.sort_values('Terjual per Bulan', ascending=False).head(15).copy()
            top_products['Harga_rp'] = top_products['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
            top_products['Omzet_rp'] = top_products['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
            
            display_cols_top = ['Nama Produk', 'SKU', 'Harga_rp', 'Omzet_rp', 'Terjual per Bulan']
            if 'SKU' not in top_products.columns:
                top_products['SKU'] = 'N/A'
            
            display_df_top = top_products[display_cols_top].rename(columns={'Harga_rp': 'Harga', 'Omzet_rp': 'Omzet'})
            st.dataframe(display_df_top, use_container_width=True, hide_index=True)

            st.subheader(f"{section_counter}. Distribusi Omzet Brand")
            section_counter += 1
            brand_omzet_main = main_store_latest_overall.groupby('Brand')['Omzet'].sum().reset_index()
            if not brand_omzet_main.empty:
                fig_brand_pie = px.pie(brand_omzet_main.sort_values('Omzet', ascending=False).head(7), 
                                       names='Brand', values='Omzet', title='Distribusi Omzet Top 7 Brand (Snapshot Terakhir)')
                fig_brand_pie.update_traces(textposition='outside', texttemplate='%{label}<br><b>Rp %{value:,.0f}</b><br>(%{percent})', insidetextfont=dict(color='white'))
                fig_brand_pie.update_layout(showlegend=False)
                st.plotly_chart(fig_brand_pie, use_container_width=True)
            else:
                st.info("Tidak ada data omzet brand.")

            st.subheader(f"{section_counter}. Ringkasan Kinerja Mingguan (WoW Growth)")
            section_counter += 1
            main_store_latest_weekly = main_store_df.loc[main_store_df.groupby(['Minggu', 'Nama Produk'])['Tanggal'].idxmax()]
            weekly_summary_tab1 = main_store_latest_weekly.groupby('Minggu').agg(Omzet=('Omzet', 'sum'), Penjualan_Unit=('Terjual per Bulan', 'sum')).reset_index().sort_values('Minggu')
            weekly_summary_tab1['Pertumbuhan Omzet (WoW)'] = weekly_summary_tab1['Omzet'].pct_change().apply(format_wow_growth)
            weekly_summary_tab1['Omzet'] = weekly_summary_tab1['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
            st.dataframe(weekly_summary_tab1[['Minggu', 'Omzet', 'Penjualan_Unit', 'Pertumbuhan Omzet (WoW)']].style.applymap(style_wow_growth, subset=['Pertumbuhan Omzet (WoW)']), use_container_width=True, hide_index=True)

        with tab2:
            st.header(f"Perbandingan Produk '{my_store_name}' dengan Kompetitor")
            st.info("Perbandingan menggunakan data produk terbaru dari toko Anda.")
            
            latest_products_df = main_store_latest_overall.copy()
            
            brand_list = sorted(latest_products_df['Brand'].unique())
            selected_brand_tab2 = st.selectbox("Filter berdasarkan Brand:", ["Semua Brand"] + brand_list, key="brand_select_compare")
            
            products_to_show_df = latest_products_df[latest_products_df['Brand'] == selected_brand_tab2] if selected_brand_tab2 != "Semua Brand" else latest_products_df
                
            product_list = sorted(products_to_show_df['Nama Produk'].unique())
            if not product_list:
                st.warning(f"Tidak ada produk untuk brand '{selected_brand_tab2}' pada snapshot terakhir.")
            else:
                selected_product = st.selectbox("Pilih produk dari toko Anda:", product_list, key="product_select_compare")

                if selected_product:
                    product_info = latest_products_df[latest_products_df['Nama Produk'] == selected_product].iloc[0]
                    st.markdown(f"**Produk Pilihan Anda:** *{product_info['Nama Produk']}*")
                    
                    matches_for_product = matches_df[(matches_df['Produk Toko Saya'] == selected_product) & (matches_df['Skor Kemiripan'] >= accuracy_cutoff)].sort_values(by='Skor Kemiripan', ascending=False)

                    col1, col2, col3 = st.columns(3)
                    
                    all_occurrences = df_filtered[df_filtered['Nama Produk'] == selected_product]
                    avg_price = all_occurrences['Harga'].mean()
                    col1.metric("Harga Rata-Rata (Semua Toko)", f"Rp {int(avg_price):,}" if pd.notna(avg_price) else "N/A")
                    
                    total_competitor_stores = len(competitor_df['Toko'].unique())
                    matched_product_names = matches_for_product['Produk Kompetitor'].unique()
                    matched_products_details = competitor_latest_overall[competitor_latest_overall['Nama Produk'].isin(matched_product_names)]
                    
                    ready_count = matched_products_details[matched_products_details['Status'] == 'Tersedia']['Toko'].nunique()
                    oot_count = total_competitor_stores - ready_count
                    
                    col2.metric("Status di Kompetitor", f"Ready: {ready_count} | Habis: {oot_count}", help=f"Berdasarkan {total_competitor_stores} total toko kompetitor yang dipantau.")

                    if not all_occurrences.empty:
                        top_store_row = all_occurrences.loc[all_occurrences['Omzet'].idxmax()]
                        top_store_name = top_store_row['Toko']
                        top_omzet = top_store_row['Omzet']
                        col3.metric("Toko Omzet Tertinggi", f"{top_store_name}", f"Rp {int(top_omzet):,}")
                    else:
                        col3.metric("Toko Omzet Tertinggi", "N/A")
                    
                    st.divider()
                    
                    st.subheader("Perbandingan di Toko Kompetitor (Hasil Matching Terakhir)")
                    if matches_for_product.empty:
                        st.warning("Tidak ditemukan kecocokan di 'HASIL_MATCHING' dengan filter akurasi Anda.")
                    else:
                        comparison_data = []
                        my_price = int(product_info['Harga'])
                        for _, match in matches_for_product.iterrows():
                            comp_price = int(match['Harga Kompetitor'])
                            price_diff = comp_price - my_price
                            diff_text = " (Lebih Mahal)" if price_diff > 0 else (" (Lebih Murah)" if price_diff < 0 else " (Sama)")
                            
                            comp_details = competitor_latest_overall[(competitor_latest_overall['Nama Produk'] == match['Produk Kompetitor']) & (competitor_latest_overall['Toko'] == match['Toko Kompetitor'])]
                            terjual = comp_details['Terjual per Bulan'].iloc[0] if not comp_details.empty else 0
                            omzet = comp_details['Omzet'].iloc[0] if not comp_details.empty else 0
                            
                            comparison_data.append({
                                'Toko Kompetitor': match['Toko Kompetitor'], 'Harga Kompetitor': f"Rp {comp_price:,}",
                                'Selisih Harga': f"Rp {price_diff:,}{diff_text}", 'Terjual per Bulan': int(terjual),
                                'Omzet': f"Rp {int(omzet):,}", 'Skor Kemiripan (%)': int(match['Skor Kemiripan'])
                            })
                        
                        comparison_df = pd.DataFrame(comparison_data)
                        ordered_cols = ['Toko Kompetitor', 'Harga Kompetitor', 'Selisih Harga', 'Terjual per Bulan', 'Omzet', 'Skor Kemiripan (%)']
                        st.dataframe(comparison_df[ordered_cols], use_container_width=True, hide_index=True)

        with tab3:
            st.header("Analisis Brand di Toko Kompetitor")
            if competitor_df.empty:
                st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
            else:
                competitor_list = sorted(competitor_df['Toko'].unique())
                for competitor_store in competitor_list:
                    with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                        single_competitor_df = competitor_latest_overall[competitor_latest_overall['Toko'] == competitor_store]
                        brand_analysis = single_competitor_df.groupby('Brand').agg(Total_Omzet=('Omzet', 'sum'), Total_Unit_Terjual=('Terjual per Bulan', 'sum')).reset_index().sort_values("Total_Omzet", ascending=False)
                        
                        if not brand_analysis.empty:
                            display_brand_analysis = brand_analysis.head(10).copy()
                            display_brand_analysis['Total_Omzet'] = display_brand_analysis['Total_Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                            st.dataframe(display_brand_analysis, use_container_width=True, hide_index=True)

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
                    all_stores_filtered = sorted(df_filtered['Toko'].unique())
                    for store in all_stores_filtered:
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
                                
                                # Penanganan jika kolom 'Stok' tidak ada
                                cols_to_display_new = ['Nama Produk', 'Harga_fmt', 'Brand']
                                if 'Stok' in new_products_df.columns:
                                    cols_to_display_new.append('Stok')
                                    
                                st.dataframe(new_products_df[cols_to_display_new].rename(columns={'Harga_fmt':'Harga'}), use_container_width=True, hide_index=True)

    elif page == "DAFTAR HPP PRODUK":
        display_hpp_analysis()

if __name__ == "__main__":
    main()

