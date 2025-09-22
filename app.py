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

# ===============================
# KONFIGURASI HALAMAN
# ===============================
st.set_page_config(layout="wide", page_title="Dashboard Analisis Penjualan")

# ===============================
# FUNGSI KONEKSI & MEMUAT DATA
# ===============================
@st.cache_resource(show_spinner="Menghubungkan ke Google Sheets...")
def connect_to_gsheets():
    creds_dict = st.secrets["gcp_service_account"]
    gc = gspread.service_account_from_dict(creds_dict)
    return gc

@st.cache_data(ttl=600, show_spinner="Memuat data sumber...")
def load_source_data(_gc, spreadsheet_id):
    spreadsheet = _gc.open_by_key(spreadsheet_id)
    worksheet_list = [
        "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS",
        "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
        "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
        "LOGITECH - REKAP - READY", "LOGITECH - REKAP - HABIS"
    ]
    all_data = []
    for ws_name in worksheet_list:
        try:
            worksheet = spreadsheet.worksheet(ws_name)
            df = pd.DataFrame(worksheet.get_all_records())
            df['Toko'] = ws_name.split(' - ')[0]
            df['Status'] = 'Tersedia' if 'READY' in ws_name or 'RE' in ws_name else 'Habis'
            all_data.append(df)
        except gspread.WorksheetNotFound:
            st.warning(f"Worksheet '{ws_name}' tidak ditemukan.")
    if not all_data:
        st.error("Tidak ada data yang berhasil dimuat. Periksa nama worksheet Anda.")
        return pd.DataFrame()
    df_combined = pd.concat(all_data, ignore_index=True)
    df_combined['Tanggal'] = pd.to_datetime(df_combined['TANGGAL'], errors='coerce')
    df_combined.rename(columns={'NAMA': 'Nama Produk', 'HARGA': 'Harga', 'TERJUAL/BLN': 'Terjual/Bln'}, inplace=True)
    df_combined['Harga'] = pd.to_numeric(df_combined['Harga'], errors='coerce').fillna(0)
    df_combined['Terjual/Bln'] = pd.to_numeric(df_combined['Terjual/Bln'], errors='coerce').fillna(0)
    df_combined['Omzet'] = df_combined['Harga'] * df_combined['Terjual/Bln']
    df_combined['Minggu'] = df_combined['Tanggal'].dt.strftime('W%U-%Y')
    return df_combined

@st.cache_data(ttl=600, show_spinner="Memuat hasil perbandingan harga...")
def load_matching_results(_gc, spreadsheet_id):
    try:
        spreadsheet = _gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("HASIL_MATCHING")
        df = pd.DataFrame(worksheet.get_all_records())
        # Pastikan kolom harga adalah numerik
        df['Harga DBKlik'] = pd.to_numeric(df['Harga DBKlik'], errors='coerce').fillna(0)
        df['Harga Kompetitor'] = pd.to_numeric(df['Harga Kompetitor'], errors='coerce').fillna(0)
        return df
    except gspread.WorksheetNotFound:
        st.error("Worksheet 'HASIL_MATCHING' tidak ditemukan di spreadsheet tujuan.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Gagal memuat 'HASIL_MATCHING': {e}")
        return pd.DataFrame()


# ===============================
# FUNGSI-FUNGSI BANTUAN
# ===============================
def run_fuzzy_matching(df_dbklik, df_competitor, progress_bar):
    dbklik_products = df_dbklik['Nama Produk'].tolist()
    competitor_products = df_competitor['Nama Produk'].tolist()
    
    matches = []
    total_products = len(dbklik_products)
    
    for i, product in enumerate(dbklik_products):
        # Cari 5 kandidat terbaik
        best_matches = process.extract(product, competitor_products, scorer=fuzz.WRatio, limit=5)
        
        # Ambil informasi produk DBKlik saat ini
        dbklik_product_info = df_dbklik.iloc[i]

        for match_name, score, _ in best_matches:
            if score >= 85: # Ambang batas skor
                competitor_info = df_competitor[df_competitor['Nama Produk'] == match_name].iloc[0]
                matches.append({
                    'Tanggal Analisis': datetime.now().strftime('%Y-%m-%d'),
                    'Produk DBKlik': product,
                    'SKU DBKlik': dbklik_product_info.get('SKU', ''),
                    'Harga DBKlik': dbklik_product_info['Harga'],
                    'Produk Kompetitor': match_name,
                    'Harga Kompetitor': competitor_info['Harga'],
                    'Toko Kompetitor': competitor_info['Toko'],
                    'Skor Kemiripan (%)': score,
                    'Brand': dbklik_product_info.get('BRAND', '')
                })
        
        # Update progress bar
        progress_bar.progress((i + 1) / total_products, text=f"Mencocokkan produk: {i+1}/{total_products}")

    return pd.DataFrame(matches)

# ===============================
# MAIN APP
# ===============================
gc = connect_to_gsheets()

# --- Muat Data ---
SOURCE_SPREADSHEET_ID = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
DESTINATION_SPREADSHEET_ID = "1W2DiEUWQEBgZdaeQM-BV_z507rdcBJYqupc6sgzTFd4"

df_sumber = load_source_data(gc, SOURCE_SPREADSHEET_ID)
df_hasil_matching = load_matching_results(gc, DESTINATION_SPREADSHEET_ID)


# --- Sidebar ---
st.sidebar.title("Navigasi")
selected_tab = st.sidebar.radio("Pilih Halaman Analisis:", 
    ["Analisis Toko Saya", "Perbandingan Harga", "Analisis Brand Kompetitor", "Analisis Stok", "Kinerja Penjualan", "Produk Baru"])

st.sidebar.header("Pembaruan Data")
if st.sidebar.button("Update Perbandingan Harga"):
    with st.spinner("Memulai proses pembaruan... Ini mungkin memakan waktu beberapa menit."):
        st.sidebar.info("Langkah 1: Mengambil data terbaru dari sumber...")
        df_latest = load_source_data.clear() # Hapus cache untuk ambil data baru
        df_latest = load_source_data(gc, SOURCE_SPREADSHEET_ID)

        df_dbklik_latest = df_latest[(df_latest['Toko'] == 'DB KLIK') & (df_latest['Status'] == 'Tersedia')].copy()
        df_competitor_latest = df_latest[(df_latest['Toko'] != 'DB KLIK') & (df_latest['Status'] == 'Tersedia')].copy()
        
        st.sidebar.info(f"Langkah 2: Menjalankan pencocokan untuk {len(df_dbklik_latest)} produk DB KLIK...")
        progress_bar = st.sidebar.progress(0, text="Memulai pencocokan...")
        
        new_matches_df = run_fuzzy_matching(df_dbklik_latest, df_competitor_latest, progress_bar)
        
        progress_bar.progress(1.0, text="Pencocokan selesai.")

        if not new_matches_df.empty:
            st.sidebar.info("Langkah 3: Mengunggah hasil baru ke Google Sheets...")
            try:
                dest_spreadsheet = gc.open_by_key(DESTINATION_SPREADSHEET_ID)
                dest_ws = dest_spreadsheet.worksheet("HASIL_MATCHING")
                dest_ws.clear()
                set_with_dataframe(dest_ws, new_matches_df)
                st.sidebar.success("Pembaruan berhasil! Muat ulang halaman untuk melihat data terbaru.")
                # Clear cache hasil matching agar data baru dimuat
                load_matching_results.clear()
            except Exception as e:
                st.sidebar.error(f"Gagal mengunggah hasil: {e}")
        else:
            st.sidebar.warning("Tidak ditemukan kecocokan baru untuk diunggah.")


# =================================================================
# --- TAB 1: ANALISIS TOKO SAYA (DB KLIK) ---
# =================================================================
if selected_tab == "Analisis Toko Saya":
    st.header("Analisis Kinerja Toko: DB KLIK")
    
    df_dbklik = df_sumber[df_sumber['Toko'] == 'DB KLIK'].copy()

    if df_dbklik.empty:
        st.warning("Tidak ada data ditemukan untuk DB KLIK.")
    else:
        # Analisis Kategori Terlaris
        st.subheader("Kategori Terlaris Berdasarkan Omzet")
        category_sales = df_dbklik.groupby('KATEGORI')['Omzet'].sum().nlargest(10).sort_values(ascending=False)
        
        fig_cat = px.bar(category_sales, x=category_sales.values, y=category_sales.index, orientation='h', 
                         labels={'x': 'Total Omzet (Rp)', 'y': 'Kategori'},
                         title="Top 10 Kategori Paling Laris", text_auto='.2s')
        fig_cat.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_cat, use_container_width=True)

        # Detail Produk Terlaris per Kategori
        st.subheader("Detail Produk Terlaris per Kategori")
        top_categories = category_sales.index.tolist()
        selected_category = st.selectbox("Pilih kategori untuk melihat detail produk:", top_categories)

        if selected_category:
            product_details = df_dbklik[df_dbklik['KATEGORI'] == selected_category].sort_values('Omzet', ascending=False).head(20)
            product_details_display = product_details[['Nama Produk', 'Harga', 'Terjual/Bln', 'Omzet']].copy()
            product_details_display.rename(columns={'Terjual/Bln': 'Terjual/Bln Terbaru'}, inplace=True)
            
            # Formatting
            for col in ['Harga', 'Omzet']:
                product_details_display[col] = product_details_display[col].apply(lambda x: f"Rp {int(x):,}")

            st.dataframe(product_details_display, use_container_width=True)

# =================================================================
# --- TAB 2: PERBANDINGAN HARGA --- (LOGIKA DIPERBAIKI)
# =================================================================
elif selected_tab == "Perbandingan Harga":
    st.header("Perbandingan Harga dengan Kompetitor")

    if df_hasil_matching.empty:
        st.warning("Data perbandingan tidak tersedia. Silakan jalankan 'Update Perbandingan Harga' di sidebar.")
    else:
        # 1. Dropdown untuk memilih produk dari DB KLIK
        dbklik_products_list = sorted(df_hasil_matching['Produk DBKlik'].unique())
        selected_product = st.selectbox("Pilih produk DB Klik untuk dibandingkan:", dbklik_products_list)

        if selected_product:
            # 2. Filter DataFrame `df_hasil_matching` berdasarkan produk yang dipilih
            comparison_df = df_hasil_matching[df_hasil_matching['Produk DBKlik'] == selected_product].copy()

            if comparison_df.empty:
                st.info("Tidak ditemukan data perbandingan untuk produk ini.")
            else:
                # 3. Tampilkan informasi produk kita
                my_product_info = comparison_df.iloc[0]
                my_price = my_product_info['Harga DBKlik']
                
                st.subheader(f"Produk Anda: {my_product_info['Produk DBKlik']}")
                st.metric(label="Harga Jual Anda (DB Klik)", value=f"Rp {int(my_price):,}")

                # 4. Tampilkan tabel perbandingan
                st.subheader("Ditemukan pada Kompetitor:")
                
                # Siapkan data untuk ditampilkan
                display_cols = comparison_df[['Produk Kompetitor', 'Harga Kompetitor', 'Toko Kompetitor', 'Skor Kemiripan (%)']].copy()
                display_cols.sort_values(by='Skor Kemiripan (%)', ascending=False, inplace=True)
                
                # Hitung selisih harga
                display_cols['Selisih Harga'] = display_cols['Harga Kompetitor'] - my_price
                
                # Formatting
                display_cols['Harga Kompetitor'] = display_cols['Harga Kompetitor'].apply(lambda x: f"Rp {int(x):,}")
                display_cols['Selisih Harga'] = display_cols['Selisih Harga'].apply(
                    lambda x: f"Rp {int(x):,}" if x >= 0 else f"-Rp {int(abs(x)):,}"
                )

                st.dataframe(display_cols, use_container_width=True)


# =================================================================
# --- TAB 3: ANALISIS BRAND KOMPETITOR ---
# =================================================================
elif selected_tab == "Analisis Brand Kompetitor":
    st.header("Analisis Brand di Toko Kompetitor")
    df_competitors = df_sumber[df_sumber['Toko'] != 'DB KLIK'].copy()

    if df_competitors.empty:
        st.warning("Tidak ada data kompetitor yang ditemukan.")
    else:
        stores = sorted(df_competitors['Toko'].unique())
        for store in stores:
            with st.expander(f"Analisis Brand di Toko: {store}"):
                store_df = df_competitors[df_competitors['Toko'] == store]
                brand_sales = store_df.groupby('BRAND')['Omzet'].sum().nlargest(10)

                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Top 10 Brand Terlaris (Omzet)")
                    st.dataframe(brand_sales.map('{:,.0f}'.format), use_container_width=True)
                with col2:
                    st.subheader("Distribusi Omzet Brand")
                    fig = px.pie(brand_sales, values='Omzet', names=brand_sales.index, 
                                 title=f"Distribusi Omzet 10 Brand Teratas di {store}")
                    st.plotly_chart(fig, use_container_width=True)

# =================================================================
# --- TAB 4: ANALISIS STOK ---
# =================================================================
elif selected_tab == "Analisis Stok":
    st.header("Analisis Status Stok Produk")
    
    status_counts = df_sumber.groupby(['Toko', 'Status']).size().unstack(fill_value=0)
    st.dataframe(status_counts, use_container_width=True)

    fig = px.bar(status_counts, barmode='group',
                 labels={'value': 'Jumlah Produk', 'Toko': 'Toko'},
                 title="Perbandingan Jumlah Produk Tersedia vs Habis per Toko")
    st.plotly_chart(fig, use_container_width=True)


# =================================================================
# --- TAB 5: KINERJA PENJUALAN ---
# =================================================================
elif selected_tab == "Kinerja Penjualan":
    st.header("Analisis Kinerja Penjualan Mingguan")
    
    weekly_sales = df_sumber.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
    weekly_sales_sorted = weekly_sales.sort_values(by='Minggu')

    fig = px.line(weekly_sales_sorted, x='Minggu', y='Omzet', color='Toko',
                  title="Tren Omzet Mingguan per Toko",
                  labels={'Omzet': 'Total Omzet (Rp)', 'Minggu': 'Minggu'})
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Detail Omzet Harian per Toko")
    pivot_df = df_sumber.pivot_table(index='Tanggal', columns='Toko', values='Omzet', aggfunc='sum', fill_value=0)
    pivot_df.index = pivot_df.index.strftime('%Y-%m-%d')
    st.dataframe(pivot_df.style.format("Rp {:,.0f}"), use_container_width=True)


# =================================================================
# --- TAB 6: PRODUK BARU ---
# =================================================================
elif selected_tab == "Produk Baru":
    st.header("Deteksi Produk Baru")
    
    weeks = sorted(df_sumber['Minggu'].unique())
    col1, col2 = st.columns(2)
    with col1:
        week_before = st.selectbox("Pilih Minggu Pembanding:", weeks, index=max(0, len(weeks)-2))
    with col2:
        week_after = st.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)

    if week_before >= week_after:
        st.error("Minggu Penentu harus setelah Minggu Pembanding.")
    else:
        all_stores = sorted(df_sumber['Toko'].unique())
        for store in all_stores:
            with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                products_before = set(df_sumber[(df_sumber['Toko'] == store) & (df_sumber['Minggu'] == week_before) & (df_sumber['Status'] == 'Tersedia')]['Nama Produk'])
                products_after = set(df_sumber[(df_sumber['Toko'] == store) & (df_sumber['Minggu'] == week_after) & (df_sumber['Status'] == 'Tersedia')]['Nama Produk'])
                new_products = products_after - products_before
                
                if not new_products:
                    st.write("Tidak ada produk baru yang terdeteksi.")
                else:
                    st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                    new_products_df = df_sumber[df_sumber['Nama Produk'].isin(new_products) & (df_sumber['Toko'] == store) & (df_sumber['Minggu'] == week_after)].copy()
                    new_products_df['Harga_fmt'] = new_products_df['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                    st.dataframe(new_products_df[['Nama Produk', 'Harga_fmt', 'Terjual/Bln']], use_container_width=True)




