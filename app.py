# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 5 (PERBAIKAN SESUAI REQUEST)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi ini mengimplementasikan 10 poin perbaikan yang diminta.
# ===================================================================================

import streamlit as st
import pandas as pd
from rapidfuzz import process, fuzz
import plotly.express as px
import plotly.graph_objects as go
import re
from datetime import datetime

# ===============================
# KONFIGURASI HALAMAN
# ===============================
st.set_page_config(layout="wide", page_title="Dashboard Analisis v5")

# ===============================
# FUNGSI-FUNGSI BANTUAN
# ===============================

def format_rupiah(nilai):
    """Format angka menjadi format mata uang Rupiah tanpa desimal."""
    if pd.isna(nilai) or not isinstance(nilai, (int, float)):
        return "Rp 0"
    # Menggunakan f-string dengan format locale-agnostic untuk ribuan
    return f"Rp {int(nilai):,}".replace(",", ".")

def clean_text(text):
    """Membersihkan teks untuk matching."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

@st.cache_data
def load_and_prepare_data():
    """Memuat semua data dari file CSV dan mempersiapkannya."""
    # Daftar semua file yang akan dimuat
    files_to_load = [
        "DATA_REKAP.xlsx - ABDITAMA - REKAP - HABIS.csv", "DATA_REKAP.xlsx - ABDITAMA - REKAP - READY.csv",
        "DATA_REKAP.xlsx - DB KLIK - REKAP - HABIS.csv", "DATA_REKAP.xlsx - DB KLIK - REKAP - READY.csv",
        "DATA_REKAP.xlsx - GG STORE - REKAP - HABIS.csv", "DATA_REKAP.xlsx - GG STORE - REKAP - READY.csv",
        "DATA_REKAP.xlsx - IT SHOP - REKAP - HABIS.csv", "DATA_REKAP.xlsx - IT SHOP - REKAP - READY.csv",
        "DATA_REKAP.xlsx - JAYA PC - REKAP - HABIS.csv", "DATA_REKAP.xlsx - JAYA PC - REKAP - READY.csv",
        "DATA_REKAP.xlsx - LEVEL99 - REKAP - HABIS.csv", "DATA_REKAP.xlsx - LEVEL99 - REKAP - READY.csv",
        "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - HABIS.csv", "DATA_REKAP.xlsx - MULTIFUNGSI - REKAP - READY.csv",
        "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - HA.csv", "DATA_REKAP.xlsx - SURYA MITRA ONLINE - REKAP - RE.csv",
        "DATA_REKAP.xlsx - TECH ISLAND - REKAP - HABIS.csv", "DATA_REKAP.xlsx - TECH ISLAND - REKAP - READY.csv",
        "DATA_REKAP.xlsx - LOGITECH - REKAP - HABIS.csv", "DATA_REKAP.xlsx - LOGITECH - REKAP - READY.csv"
    ]

    all_data = []
    for f in files_to_load:
        try:
            toko = f.split(' - ')[1].strip()
            status = 'Habis' if 'HABIS' in f or 'HA.csv' in f else 'Tersedia'

            df_toko = pd.read_csv(f)
            df_toko.rename(columns={
                'NAMA': 'Nama Produk',
                'HARGA': 'Harga',
                'TERJUAL/BLN': 'Terjual per Bulan',
                'BRAND': 'Brand',
                'TANGGAL': 'Tanggal'
            }, inplace=True)

            df_toko['Toko'] = toko
            df_toko['Status'] = status
            all_data.append(df_toko)
        except Exception as e:
            # Lewati file jika ada error saat memuat
            continue

    df_gabungan = pd.concat(all_data, ignore_index=True)

    # Konversi dan pembersihan data
    df_gabungan['Tanggal'] = pd.to_datetime(df_gabungan['Tanggal'], errors='coerce')
    df_gabungan.dropna(subset=['Tanggal'], inplace=True) # Hapus baris dengan tanggal tidak valid
    df_gabungan['Harga'] = pd.to_numeric(df_gabungan['Harga'], errors='coerce').fillna(0)
    df_gabungan['Terjual per Bulan'] = pd.to_numeric(df_gabungan['Terjual per Bulan'], errors='coerce').fillna(0)
    df_gabungan['Omzet'] = df_gabungan['Harga'] * df_gabungan['Terjual per Bulan']
    df_gabungan['Nama Produk'] = df_gabungan['Nama Produk'].astype(str)
    
    # Tambahkan kolom Minggu dan Bulan
    df_gabungan['Minggu'] = df_gabungan['Tanggal'].dt.strftime('W%U-%Y')
    df_gabungan['Bulan'] = df_gabungan['Tanggal'].dt.to_period('M').strftime('%Y-%m')

    # Load data matching terakhir
    df_matching = pd.read_csv("DATA_REKAP.xlsx - HASIL_MATCHING.csv")

    return df_gabungan, df_matching

# ===============================
#  MEMUAT DATA UTAMA
# ===============================
df_gabungan, df_matching = load_and_prepare_data()

# ===============================
#  UI STREAMLIT - TABS
# ===============================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")
if not df_gabungan.empty:
    st.write(f"Data terakhir diperbarui pada: **{df_gabungan['Tanggal'].max().strftime('%d %B %Y')}**")
else:
    st.error("Gagal memuat data. Silakan periksa file CSV Anda.")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Analisis DB KLIK",
    "⚔️ Analisis Kompetitor",
    "🏢 Analisis Performa Toko",
    "🏷️ Analisis Performa Brand",
    "🆕 Analisis Produk Baru",
    "🔄 Uji Coba Matching"
])

# ===================================================================================
#  TAB 1: ANALISIS DB KLIK
# ===================================================================================
with tab1:
    st.header("Analisis Performa Toko DB KLIK")

    df_db_klik = df_gabungan[df_gabungan['Toko'] == 'DB KLIK'].copy()
    if df_db_klik.empty:
        st.warning("Tidak ada data untuk toko 'DB KLIK'.")
    else:
        df_db_klik.sort_values('Tanggal', ascending=False, inplace=True)
        unique_months_dbklik = sorted(df_db_klik['Bulan'].unique(), reverse=True)
        selected_month_dbklik = st.selectbox("Pilih Bulan Analisis:", unique_months_dbklik, key='dbklik_month_selector')
        df_db_klik_filtered = df_db_klik[df_db_klik['Bulan'] == selected_month_dbklik]

        # --- 1. Analisis Kategori Terlaris ---
        st.subheader("1. Analisis Kategori Terlaris (Berdasarkan Omzet)")
        if 'KATEGORI' in df_db_klik_filtered.columns and not df_db_klik_filtered.empty:
            df_db_klik_filtered['KATEGORI'].fillna('Tidak Diketahui', inplace=True)
            omzet_per_kategori = df_db_klik_filtered.groupby('KATEGORI')['Omzet'].sum().sort_values(ascending=False)

            fig_kategori = px.bar(omzet_per_kategori, x=omzet_per_kategori.values, y=omzet_per_kategori.index, orientation='h',
                                  title=f"Total Omzet per Kategori - Bulan {selected_month_dbklik}", labels={'x': 'Total Omzet (Rp)', 'y': 'Kategori'},
                                  text=omzet_per_kategori.apply(lambda x: format_rupiah(x)))
            fig_kategori.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_kategori, use_container_width=True)

            # >> PERMINTAAN 1: Tabel dari nilai asli bar
            with st.expander("Lihat Tabel Data Omzet per Kategori"):
                tabel_omzet_kategori = omzet_per_kategori.reset_index()
                tabel_omzet_kategori.columns = ['Kategori', 'Total Omzet']
                tabel_omzet_kategori['Total Omzet'] = tabel_omzet_kategori['Total Omzet'].apply(format_rupiah)
                st.dataframe(tabel_omzet_kategori, use_container_width=True, hide_index=True)

            with st.expander("Lihat Produk Terlaris per Kategori"):
                kategori_pilihan = st.selectbox("Pilih Kategori:", options=['Semua'] + list(omzet_per_kategori.index))
                produk_terlaris = df_db_klik_filtered if kategori_pilihan == 'Semua' else df_db_klik_filtered[df_db_klik_filtered['KATEGORI'] == kategori_pilihan]
                
                # >> PERMINTAAN 2: Tambah kolom SKU
                produk_terlaris_display = produk_terlaris.sort_values('Omzet', ascending=False).head(20)
                display_cols = ['Nama Produk', 'SKU', 'Harga', 'Terjual per Bulan', 'Omzet']
                if 'SKU' not in produk_terlaris_display.columns:
                    produk_terlaris_display['SKU'] = 'N/A'
                produk_terlaris_display = produk_terlaris_display[display_cols]
                produk_terlaris_display['Harga'] = produk_terlaris_display['Harga'].apply(format_rupiah)
                produk_terlaris_display['Omzet'] = produk_terlaris_display['Omzet'].apply(format_rupiah)
                st.dataframe(produk_terlaris_display, use_container_width=True, hide_index=True)
        else:
            st.warning("Kolom 'KATEGORI' tidak ditemukan atau tidak ada data untuk bulan terpilih.")
        
        st.divider()

        # --- 2. Analisis Produk Terlaris ---
        st.subheader("2. Produk Terlaris")
        if not df_db_klik_filtered.empty:
            top_n_produk = st.slider("Jumlah produk teratas:", 5, 50, 10, key='dbklik_top_n')
            produk_terlaris_dbklik = df_db_klik_filtered.sort_values('Omzet', ascending=False).head(top_n_produk)
            fig_produk = px.bar(produk_terlaris_dbklik, x='Omzet', y='Nama Produk', orientation='h', title=f"Top {top_n_produk} Produk Terlaris - Bulan {selected_month_dbklik}",
                                text=produk_terlaris_dbklik['Omzet'].apply(format_rupiah))
            fig_produk.update_layout(yaxis={'categoryorder':'total ascending'}, height=max(400, top_n_produk * 35), yaxis_title=None, xaxis_title="Omzet (Rp)")
            st.plotly_chart(fig_produk, use_container_width=True)

        st.divider()

        # --- 3. Distribusi Omzet Brand ---
        st.subheader("3. Distribusi Omzet Brand")
        if not df_db_klik_filtered.empty:
            omzet_per_brand = df_db_klik_filtered.groupby('Brand')['Omzet'].sum().sort_values(ascending=False)
            top_n_brand = st.slider("Jumlah brand teratas:", 5, 20, 10, key='dbklik_top_brand')
            top_brands = omzet_per_brand.head(top_n_brand)
            if len(omzet_per_brand) > top_n_brand:
                other_omzet = omzet_per_brand.iloc[top_n_brand:].sum()
                top_brands['Lainnya'] = other_omzet

            # >> PERMINTAAN 3: Nilai di luar, persen di dalam
            fig_brand_pie = go.Figure(data=[go.Pie(labels=top_brands.index, values=top_brands.values, hole=.3, textinfo='percent', insidetextorientation='radial')])
            fig_brand_pie.update_traces(textposition='outside', texttemplate=[f"{label}<br><b>{format_rupiah(value)}</b>" for label, value in top_brands.items()])
            fig_brand_pie.update_layout(title_text=f"Distribusi Omzet Top {top_n_brand} Brand - Bulan {selected_month_dbklik}", showlegend=False, uniformtext_minsize=10)
            st.plotly_chart(fig_brand_pie, use_container_width=True)


# ===================================================================================
#  TAB 2: ANALISIS KOMPETITOR
# ===================================================================================
with tab2:
    st.header("Analisis Kompetitor Terhadap DB KLIK")

    df_db_klik = df_gabungan[df_gabungan['Toko'] == 'DB KLIK'].copy()
    df_kompetitor = df_gabungan[df_gabungan['Toko'] != 'DB KLIK'].copy()

    if df_db_klik.empty or df_kompetitor.empty:
        st.warning("Data 'DB KLIK' atau data kompetitor tidak ditemukan.")
    else:
        # >> PERMINTAAN 4: Gunakan data tanggal paling baru saja
        latest_date_dbklik = df_db_klik['Tanggal'].max()
        df_db_klik_latest = df_db_klik[df_db_klik['Tanggal'] == latest_date_dbklik]
        latest_date_competitor = df_kompetitor['Tanggal'].max()
        df_kompetitor_latest = df_kompetitor[df_kompetitor['Tanggal'] == latest_date_competitor]
        df_all_latest = pd.concat([df_db_klik_latest, df_kompetitor_latest])

        st.subheader("1. Perbandingan Produk")
        st.info(f"Perbandingan dilakukan berdasarkan data terbaru DB KLIK ({latest_date_dbklik.strftime('%d %b %Y')}) dan Kompetitor ({latest_date_competitor.strftime('%d %b %Y')}).")

        # >> PERMINTAAN 5: Tambahkan filter brand
        unique_brands_latest = sorted(df_db_klik_latest['Brand'].dropna().unique())
        selected_brand_competitor = st.selectbox("Filter berdasarkan Brand:", options=['Semua Brand'] + unique_brands_latest)
        
        df_db_klik_latest_filtered = df_db_klik_latest[df_db_klik_latest['Brand'] == selected_brand_competitor] if selected_brand_competitor != 'Semua Brand' else df_db_klik_latest
        
        produk_unik_db_klik = sorted(df_db_klik_latest_filtered['Nama Produk'].unique())
        
        if not produk_unik_db_klik:
            st.warning("Tidak ada produk DB KLIK yang ditemukan untuk filter yang dipilih.")
        else:
            selected_product_competitor = st.selectbox("Pilih Produk DB KLIK untuk dibandingkan:", options=produk_unik_db_klik)
            
            st.markdown("---")
            st.subheader(f"Detail Perbandingan untuk: **{selected_product_competitor}**")
            
            # >> PERMINTAAN 6: Ganti informasi yang ditampilkan
            df_product_comparison = df_all_latest[df_all_latest['Nama Produk'] == selected_product_competitor]
            
            if not df_product_comparison.empty:
                harga_rata_rata = df_product_comparison['Harga'].mean()
                toko_omzet_tertinggi_row = df_product_comparison.loc[df_product_comparison['Omzet'].idxmax()]
                toko_omzet_tertinggi = f"{toko_omzet_tertinggi_row['Toko']} ({format_rupiah(toko_omzet_tertinggi_row['Omzet'])})"

                col1, col2, col3 = st.columns(3)
                col1.metric(label="Harga Rata-Rata", value=format_rupiah(harga_rata_rata))
                col3.metric(label="Toko Omzet Tertinggi", value=toko_omzet_tertinggi_row['Toko'], help=f"Omzet: {format_rupiah(toko_omzet_tertinggi_row['Omzet'])}")
                
                with col2:
                    st.write("**Perbandingan Status**")
                    status_html = ""
                    for _, row in df_product_comparison.iterrows():
                        color = "green" if row['Status'] == 'Tersedia' else "#E966A0"
                        status_html += f"<li><b>{row['Toko']}:</b> <span style='color:{color}; font-weight:bold;'>{row['Status']}</span></li>"
                    st.markdown(f"<ul>{status_html}</ul>", unsafe_allow_html=True)
            
                # >> PERMINTAAN 7: Tren Harga Historis dihapus
                # >> PERMINTAAN 8: Tabel perbandingan hasil matching
                st.subheader("Perbandingan di Toko Kompetitor (Hasil Matching Terakhir)")
                matching_result = df_matching[df_matching['Produk Toko Saya'] == selected_product_competitor]

                if not matching_result.empty:
                    comparison_data = []
                    db_klik_data_row = df_db_klik_latest[df_db_klik_latest['Nama Produk'] == selected_product_competitor].iloc[0]
                    comparison_data.append({'Toko': 'DB KLIK (Toko Saya)', 'Nama Produk': db_klik_data_row['Nama Produk'], 'Harga': format_rupiah(db_klik_data_row['Harga']), 'Status': db_klik_data_row['Status']})
                    
                    for _, row in matching_result.iterrows():
                        comp_info = df_kompetitor_latest[(df_kompetitor_latest['Toko'] == row['Toko Kompetitor']) & (df_kompetitor_latest['Nama Produk'] == row['Produk Kompetitor'])]
                        status_kompetitor, harga_kompetitor = (comp_info.iloc[0]['Status'], comp_info.iloc[0]['Harga']) if not comp_info.empty else ("Tidak Ditemukan", row['Harga Kompetitor'])
                        comparison_data.append({'Toko': row['Toko Kompetitor'], 'Nama Produk': row['Produk Kompetitor'], 'Harga': format_rupiah(harga_kompetitor), 'Status': status_kompetitor})

                    st.dataframe(pd.DataFrame(comparison_data), hide_index=True, use_container_width=True)
                else:
                    st.info("Tidak ditemukan produk yang cocok di toko kompetitor pada data matching terakhir.")

        st.divider()

        # >> PERMINTAAN 9: Tambahkan kolom SKU pada tabel produk terlaris kompetitor
        st.subheader("2. Produk Terlaris di Toko Kompetitor")
        unique_months_comp = sorted(df_kompetitor['Bulan'].unique(), reverse=True)
        selected_month_comp = st.selectbox("Pilih Bulan Analisis:", unique_months_comp, key='comp_month_selector')
        df_kompetitor_filtered = df_kompetitor[df_kompetitor['Bulan'] == selected_month_comp]
        
        toko_kompetitor_list = sorted(df_kompetitor_filtered['Toko'].unique())
        selected_toko = st.selectbox("Pilih Toko Kompetitor:", options=toko_kompetitor_list)

        if selected_toko:
            df_selected_toko = df_kompetitor_filtered[df_kompetitor_filtered['Toko'] == selected_toko]
            produk_terlaris_kompetitor = df_selected_toko.sort_values('Omzet', ascending=False).head(10)
            if 'SKU' not in produk_terlaris_kompetitor.columns:
                produk_terlaris_kompetitor['SKU'] = 'N/A'
            
            display_cols = ['Nama Produk', 'SKU', 'Harga', 'Terjual per Bulan', 'Omzet']
            produk_terlaris_kompetitor_display = produk_terlaris_kompetitor[display_cols]
            produk_terlaris_kompetitor_display['Harga'] = produk_terlaris_kompetitor_display['Harga'].apply(format_rupiah)
            produk_terlaris_kompetitor_display['Omzet'] = produk_terlaris_kompetitor_display['Omzet'].apply(format_rupiah)
            st.dataframe(produk_terlaris_kompetitor_display, use_container_width=True, hide_index=True)


# ===================================================================================
#  TAB 3: ANALISIS PERFORMA TOKO
# ===================================================================================
with tab3:
    st.header("Analisis Performa Antar Toko")
    
    unique_months_all = sorted(df_gabungan['Bulan'].unique(), reverse=True)
    selected_month_all = st.selectbox("Pilih Bulan Analisis:", unique_months_all, key='all_stores_month_selector')
    df_all_filtered = df_gabungan[df_gabungan['Bulan'] == selected_month_all]
    
    st.subheader("1. Perbandingan Omzet Total per Toko")
    omzet_per_toko = df_all_filtered.groupby('Toko')['Omzet'].sum().sort_values(ascending=False)
    fig_omzet_toko = px.bar(omzet_per_toko, x=omzet_per_toko.index, y=omzet_per_toko.values, title=f"Total Omzet per Toko - Bulan {selected_month_all}",
                            labels={'x': 'Toko', 'y': 'Total Omzet (Rp)'}, text=omzet_per_toko.apply(format_rupiah))
    st.plotly_chart(fig_omzet_toko, use_container_width=True)

    st.subheader("2. Analisis Brand di Toko Kompetitor")
    df_kompetitor_all_filtered = df_all_filtered[df_all_filtered['Toko'] != 'DB KLIK']
    pivot_brand_toko = df_kompetitor_all_filtered.pivot_table(index='Brand', columns='Toko', values='Omzet', aggfunc='sum', fill_value=0)
    pivot_brand_toko['Total_Omzet'] = pivot_brand_toko.sum(axis=1)
    pivot_brand_toko_sorted = pivot_brand_toko.sort_values('Total_Omzet', ascending=False)
    
    st.write("Top 20 Brand dengan Omzet Tertinggi di Seluruh Toko Kompetitor")
    pivot_display = pivot_brand_toko_sorted.head(20).copy()
    
    # >> PERMINTAAN 10: Format Total_Omzet menjadi Rupiah
    for col in pivot_display.columns:
        if col != 'Total_Omzet':
            pivot_display[col] = pivot_display[col].apply(lambda x: format_rupiah(x) if x > 0 else "-")
    pivot_display['Total_Omzet'] = pivot_display['Total_Omzet'].apply(format_rupiah)
    st.dataframe(pivot_display, use_container_width=True)

# ===================================================================================
#  TAB 4: ANALISIS PERFORMA BRAND
# ===================================================================================
with tab4:
    st.header("Analisis Performa Brand Lintas Toko")
    
    unique_brands_all = sorted(df_gabungan['Brand'].dropna().unique())
    selected_brand_analysis = st.selectbox("Pilih Brand untuk dianalisis:", options=unique_brands_all)
    
    if selected_brand_analysis:
        df_brand_filtered = df_gabungan[df_gabungan['Brand'] == selected_brand_analysis]
        unique_months_brand = sorted(df_brand_filtered['Bulan'].unique(), reverse=True)
        if unique_months_brand:
            selected_month_brand = st.selectbox("Pilih Bulan:", options=unique_months_brand, key='brand_month_selector')
            df_brand_filtered_monthly = df_brand_filtered[df_brand_filtered['Bulan'] == selected_month_brand]

            st.subheader(f"Distribusi Omzet Brand '{selected_brand_analysis}' per Toko")
            omzet_brand_per_toko = df_brand_filtered_monthly.groupby('Toko')['Omzet'].sum().sort_values(ascending=False)
            fig_brand_dist = px.pie(omzet_brand_per_toko, names=omzet_brand_per_toko.index, values=omzet_brand_per_toko.values,
                                    title=f"Pangsa Pasar Omzet '{selected_brand_analysis}' - Bulan {selected_month_brand}", hole=0.3)
            fig_brand_dist.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_brand_dist, use_container_width=True)
            
            st.subheader(f"Produk Terlaris dari '{selected_brand_analysis}' di Semua Toko")
            top_produk_brand = df_brand_filtered_monthly.sort_values('Omzet', ascending=False).head(10)
            top_produk_brand_display = top_produk_brand[['Nama Produk', 'Toko', 'Harga', 'Terjual per Bulan', 'Omzet']].copy()
            top_produk_brand_display['Harga'] = top_produk_brand_display['Harga'].apply(format_rupiah)
            top_produk_brand_display['Omzet'] = top_produk_brand_display['Omzet'].apply(format_rupiah)
            st.dataframe(top_produk_brand_display, use_container_width=True, hide_index=True)

# ===================================================================================
#  TAB 5: ANALISIS PRODUK BARU
# ===================================================================================
with tab5:
    st.header("Analisis Produk Baru yang Muncul")
    st.info("Fitur ini membandingkan daftar produk yang 'Tersedia' antara dua minggu untuk mendeteksi produk baru.")

    toko_list_new = ['Semua Toko'] + sorted(df_gabungan['Toko'].unique())
    selected_toko_new = st.selectbox("Pilih Toko:", toko_list_new, key='new_prod_toko')
    df_filtered_new = df_gabungan if selected_toko_new == 'Semua Toko' else df_gabungan[df_gabungan['Toko'] == selected_toko_new]
    
    weeks = sorted(df_filtered_new['Minggu'].unique())
    
    if len(weeks) < 2:
        st.warning("Data tidak cukup untuk perbandingan (kurang dari 2 minggu).")
    else:
        col1, col2 = st.columns(2)
        week_before = col1.selectbox("Minggu Pembanding (Lama):", weeks, index=0)
        week_after = col2.selectbox("Minggu Penentu (Baru):", weeks, index=len(weeks)-1)

        if st.button("Bandingkan Minggu"):
            if week_before >= week_after:
                st.error("Minggu Penentu harus setelah Minggu Pembanding.")
            else:
                all_stores_to_check = [selected_toko_new] if selected_toko_new != 'Semua Toko' else sorted(df_filtered_new['Toko'].unique())
                for store in all_stores_to_check:
                    with st.expander(f"Produk Baru di Toko: **{store}**", expanded=True):
                        products_before = set(df_filtered_new[(df_filtered_new['Toko'] == store) & (df_filtered_new['Minggu'] == week_before) & (df_filtered_new['Status'] == 'Tersedia')]['Nama Produk'])
                        products_after = set(df_filtered_new[(df_filtered_new['Toko'] == store) & (df_filtered_new['Minggu'] == week_after) & (df_filtered_new['Status'] == 'Tersedia')]['Nama Produk'])
                        new_products = products_after - products_before
                        
                        if not new_products:
                            st.write("Tidak ada produk baru yang terdeteksi.")
                        else:
                            st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                            new_products_df = df_filtered_new[df_filtered_new['Nama Produk'].isin(new_products) & (df_filtered_new['Toko'] == store) & (df_filtered_new['Minggu'] == week_after)].copy()
                            new_products_df['Harga'] = new_products_df['Harga'].apply(format_rupiah)
                            display_cols = ['Nama Produk', 'Harga', 'Brand']
                            if 'KATEGORI' in new_products_df.columns:
                                display_cols.append('KATEGORI')
                            st.dataframe(new_products_df[display_cols], hide_index=True, use_container_width=True)

# ===================================================================================
#  TAB 6: UJI COBA MATCHING
# ===================================================================================
with tab6:
    st.header("Uji Coba Matching Produk")
    st.info("Fitur ini membantu mencocokkan produk DB KLIK dengan produk kompetitor menggunakan pencocokan teks.")

    df_db_klik_latest_match = df_gabungan[(df_gabungan['Toko'] == 'DB KLIK') & (df_gabungan['Tanggal'] == df_gabungan['Tanggal'].max())]
    df_kompetitor_latest_match = df_gabungan[(df_gabungan['Toko'] != 'DB KLIK') & (df_gabungan['Tanggal'] == df_gabungan['Tanggal'].max())]

    if not df_db_klik_latest_match.empty and not df_kompetitor_latest_match.empty:
        product_list_db_klik = sorted(df_db_klik_latest_match['Nama Produk'].unique())
        selected_product_match = st.selectbox("Pilih produk DB KLIK:", product_list_db_klik)

        competitor_list_match = sorted(df_kompetitor_latest_match['Toko'].unique())
        selected_competitor_match = st.selectbox("Pilih toko kompetitor:", competitor_list_match)
        
        min_score = st.slider("Tingkat kemiripan minimum (%):", 0, 100, 85)

        if st.button("Cari Produk Serupa"):
            product_list_competitor = df_kompetitor_latest_match[df_kompetitor_latest_match['Toko'] == selected_competitor_match]['Nama Produk'].tolist()
            
            cleaned_query = clean_text(selected_product_match)
            cleaned_choices = {clean_text(p): p for p in product_list_competitor}
            
            matches = process.extract(cleaned_query, cleaned_choices.keys(), scorer=fuzz.WRatio, limit=5, score_cutoff=min_score)
            
            st.subheader("Hasil Pencocokan:")
            if not matches:
                st.warning("Tidak ditemukan produk yang cukup mirip.")
            else:
                match_results = []
                for (cleaned_match, score, _) in matches:
                    original_name = cleaned_choices[cleaned_match]
                    product_info = df_kompetitor_latest_match[(df_kompetitor_latest_match['Toko'] == selected_competitor_match) & (df_kompetitor_latest_match['Nama Produk'] == original_name)].iloc[0]
                    match_results.append({"Produk Kompetitor": original_name, "Skor Kemiripan": f"{score:.1f}%", "Harga": format_rupiah(product_info['Harga']), "Status": product_info['Status']})
                st.dataframe(pd.DataFrame(match_results), hide_index=True, use_container_width=True)
    else:
        st.error("Data terbaru untuk DB KLIK atau kompetitor tidak tersedia untuk matching.")
