# KODE FINAL LENGKAP - app.py (Perbaikan Otentikasi gspread)

import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import re
import gspread
from gspread_pandas import Spread

st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# --- FUNGSI-FUNGSI UTAMA ---

# Fungsi ini mengambil data dari Google Sheets dan memprosesnya
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...", ttl=600)
def load_data_from_gsheets():
    try:
        # --- PERUBAHAN OTENTIKASI DIMULAI DI SINI ---
        # Perbaikan: Menggunakan gspread untuk otentikasi dari dict, lalu memberikannya ke gspread-pandas
        spreadsheet_name = "DATA REKAP"
        
        # 1. Otentikasi menggunakan gspread langsung dari st.secrets
        gc = gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        
        # 2. Buka spreadsheet menggunakan koneksi yang sudah terotentikasi
        spreadsheet = gc.open(spreadsheet_name)
        
        # 3. Buat objek Spread dari gspread-pandas menggunakan spreadsheet yang sudah dibuka
        spread = Spread(spread=spreadsheet)
        # --- AKHIR DARI PERUBAHAN OTENTIKASI ---

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Spreadsheet dengan nama '{spreadsheet_name}' tidak ditemukan. Pastikan nama sudah benar dan sudah dibagikan ke email service account.")
        return pd.DataFrame(), pd.DataFrame(), None
    except Exception as e:
        st.error(f"Terjadi kesalahan saat menghubungkan ke Google Sheets: {e}")
        st.info("Pastikan Anda sudah mengatur 'Secrets' di Streamlit Cloud dengan benar dan format penamaan sheet sudah sesuai.")
        return pd.DataFrame(), pd.DataFrame(), None

    rekap_list_df = []
    database_df = pd.DataFrame()
    my_store_name = None

    # Loop melalui semua sheet di dalam file (tidak ada perubahan di bagian ini)
    for sheet in spread.sheets:
        sheet_title = sheet.title
        
        is_database_file = "DATABASE" in sheet_title.upper()
        is_rekap_file = "REKAP" in sheet_title.upper()

        if is_database_file:
            database_df = spread.sheet_to_df(index=None, sheet=sheet)
            database_df.columns = [str(col).strip().upper() for col in database_df.columns]
            db_store_name_match = re.match(r"^(.*?) - DATABASE", sheet_title, re.IGNORECASE)
            if db_store_name_match:
                my_store_name = db_store_name_match.group(1).strip()
        
        elif is_rekap_file:
            df_sheet = spread.sheet_to_df(index=None, sheet=sheet, header_rows=1)
            if df_sheet.empty:
                continue

            df_sheet.columns = [str(col).strip().upper() for col in df_sheet.columns]
            
            store_name_match = re.match(r"^(.*?) - REKAP", sheet_title, re.IGNORECASE)
            store_name = store_name_match.group(1).strip() if store_name_match else "Toko Tidak Dikenal"
            df_sheet['Toko'] = store_name

            if "READY" in sheet_title.upper():
                df_sheet['Status'] = 'Tersedia'
            elif "HABIS" in sheet_title.upper():
                df_sheet['Status'] = 'Habis'
            else:
                df_sheet['Status'] = 'Tidak Diketahui'
            
            rekap_list_df.append(df_sheet)

    if not rekap_list_df:
        return pd.DataFrame(), pd.DataFrame(), None

    # --- Bagian pemrosesan data (tidak ada perubahan) ---
    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    column_mapping = {'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga'}
    rekap_df.rename(columns=column_mapping, inplace=True)
    
    required_cols = ['Tanggal', 'Nama Produk', 'Harga', 'Terjual per Bulan']
    for col in required_cols:
        if col not in rekap_df.columns:
            st.error(f"Kolom krusial '{col}' tidak ditemukan di sheet REKAP. Pastikan nama kolom sudah benar.")
            return pd.DataFrame(), pd.DataFrame(), None

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce')
    def clean_price(price):
        try:
            price_str = str(price).replace('Rp', '').replace('.', '').replace(',', '').strip()
            return pd.to_numeric(price_str, errors='coerce')
        except: return None
    rekap_df['Harga'] = rekap_df['Harga'].apply(clean_price)
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0).astype(int)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)
    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True)
    return rekap_df.sort_values('Tanggal'), database_df, my_store_name

# --- SEMUA FUNGSI LAINNYA DI BAWAH INI TIDAK ADA PERUBAHAN SAMA SEKALI ---

def get_smart_matches(query_product_info, competitor_df, limit=5, score_cutoff=90):
    query_name = query_product_info['Nama Produk']
    query_lower = query_name.lower()
    query_brand = query_lower.split()[0]
    identifiers = set(re.findall(r'\b([a-z0-9]*[a-z][0-9][a-z0-9]*|[a-z0-9]*[0-9][a-z][a-z0-9]*)\b', query_lower))
    all_caps_words = set(re.findall(r'\b[A-Z]{3,}\b', query_name))
    identifiers.update({word.lower() for word in all_caps_words})
    if not identifiers:
        identifiers = {word for word in query_lower.split() if len(word) > 4 and word.isalnum()}
    competitor_product_list = competitor_df['Nama Produk'].tolist()
    candidates = process.extract(query_name, competitor_product_list, limit=20, scorer=fuzz.token_set_ratio)
    smart_results = []
    for candidate_name, score in candidates:
        if len(smart_results) >= limit: break
        if score < score_cutoff: continue
        candidate_lower = candidate_name.lower()
        if query_brand not in candidate_lower: continue
        if not any(identifier in candidate_lower for identifier in identifiers): continue
        smart_results.append((candidate_name, score))
    return smart_results

@st.cache_data(show_spinner="Menganalisis produk yang memiliki kemiripan di toko lain...")
def find_products_with_matches(_main_store_df, _competitor_df):
    if _main_store_df.empty or _competitor_df.empty:
        return set()
    products_with_matches_set = set()
    latest_date = _main_store_df['Tanggal'].max()
    _main_store_df_latest = _main_store_df[_main_store_df['Tanggal'] == latest_date]
    for _, product_row in _main_store_df_latest.iterrows():
        matches = get_smart_matches(product_row, _competitor_df, limit=1, score_cutoff=90)
        if matches:
            products_with_matches_set.add(product_row['Nama Produk'])
    return products_with_matches_set

def style_matched_products(row, matched_set):
    if row['Nama Produk'] in matched_set:
        return ['background-color: lightblue'] * len(row)
    else:
        return [''] * len(row)

def create_consolidated_weekly_summary(weekly_stats_list):
    if not weekly_stats_list: return pd.DataFrame()
    weekly_stats_list = sorted(weekly_stats_list, key=lambda x: x['start_date'])
    keterangan_labels = ["Total Omzet per Bulan", "Rata - Rata Omzet Per Hari", "Total Produk Terjual per Bulan", "Rata - Rata Terjual Per Hari", "Rata - Rata Harga Per Produk"]
    final_df = pd.DataFrame({'Keterangan': keterangan_labels})
    for i, stats in enumerate(weekly_stats_list):
        column_name = f"MINGGU {i + 1} (s/d {stats['start_date'].strftime('%d %b %Y')})"
        column_values = [f"Rp{stats['Total Omzet per Bulan']:,.2f}", f"Rp{stats['Rata - Rata Omzet Per Hari']:,.2f}", f"{stats['Total Produk Terjual per Bulan']:,}", f"{stats['Rata - Rata Terjual Per Hari']:,}", f"Rp{stats['Rata - Rata Harga Per Produk']:,.2f}"]
        final_df[column_name] = column_values
    return final_df

# --- INTERFACE DASHBOARD ---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

st.sidebar.header("Kontrol Analisis")

# Tombol untuk memulai analisis
if st.sidebar.button("Tarik Data & Mulai Analisis 🚀"):
    df, db_df, my_store_name_from_db = load_data_from_gsheets()

    if df.empty:
        st.error("Gagal memuat data REKAP dari Google Sheets. Pastikan format sheet dan nama kolom sudah benar.")
        st.stop()
    if db_df.empty:
        st.warning("Sheet DATABASE tidak ditemukan atau gagal dibaca. Tab 'Analisis Toko Saya' tidak akan tersedia.")
    if my_store_name_from_db is None and not db_df.empty:
        st.warning("Nama toko Anda tidak dapat diidentifikasi dari nama sheet DATABASE. Gunakan format 'NAMA TOKO - DATABASE'.")

    st.sidebar.header("Filter Data")
    all_stores_list = sorted(df['Toko'].unique())
    main_store_for_comp = st.sidebar.selectbox("Pilih Toko Utama untuk Perbandingan:", options=all_stores_list, index=0)
    min_date = df['Tanggal'].min().date()
    max_date = df['Tanggal'].max().date()
    start_date = st.sidebar.date_input("Tanggal Mulai", min_date, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("Tanggal Akhir", max_date, min_value=start_date, max_value=max_date)

    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date)
    df_filtered = df[(df['Tanggal'] >= start_datetime) & (df['Tanggal'] <= end_datetime)].copy()
    if df_filtered.empty:
        st.error("Tidak ada data pada rentang tanggal yang dipilih. Sesuaikan filter Anda.")
        st.stop()
        
    latest_date_in_range = df_filtered['Tanggal'].max()
    df_latest = df_filtered[df_filtered['Tanggal'] == latest_date_in_range].copy()
    main_store_df = df_filtered[df_filtered['Toko'] == main_store_for_comp].copy()
    competitor_df = df_filtered[df_filtered['Toko'] != main_store_for_comp].copy()
    
    TABS = []
    if not db_df.empty and my_store_name_from_db:
        TABS.append(f"⭐ Analisis Toko Saya ({my_store_name_from_db})")
    TABS.extend(["⚖️ Perbandingan Harga Kompetitor", "📦 Status Stok Produk", "💡 Rekomendasi Analisis", "📈 Kinerja Penjualan"])
    created_tabs = st.tabs(TABS)

    start_index = 0
    if not db_df.empty and my_store_name_from_db:
        my_store_tab = created_tabs[0]
        start_index = 1
        with my_store_tab:
            st.header(f"Analisis Kinerja Toko: {my_store_name_from_db}")
            my_store_rekap_df = df_filtered[df_filtered['Toko'] == my_store_name_from_db].copy()
            if my_store_rekap_df.empty:
                st.warning(f"Tidak ditemukan data rekap untuk toko '{my_store_name_from_db}' pada rentang tanggal terpilih.")
            else:
                st.subheader("Kategori Produk Terlaris")
                with st.spinner("Mencocokkan produk dengan kategori..."):
                    @st.cache_data
                    def fuzzy_merge_categories(_rekap_df, _database_df):
                        _rekap_df['Kategori'] = 'Tidak Diketahui'
                        if 'NAMA' not in _database_df.columns or 'KATEGORI' not in _database_df.columns:
                            st.error("File DATABASE harus memiliki kolom 'NAMA' dan 'KATEGORI'. Pastikan nama kolom sudah benar.")
                            return _rekap_df
                        db_product_list = _database_df['NAMA'].tolist()
                        for index, row in _rekap_df.iterrows():
                            match, score = process.extractOne(row['Nama Produk'], db_product_list, scorer=fuzz.token_set_ratio)
                            if score >= 95:
                                matched_category = _database_df[_database_df['NAMA'] == match]['KATEGORI'].iloc[0]
                                _rekap_df.loc[index, 'Kategori'] = matched_category
                        return _rekap_df
                    my_store_rekap_df = fuzzy_merge_categories(my_store_rekap_df, db_df)
                    category_sales = my_store_rekap_df.groupby('Kategori')['Terjual per Bulan'].sum().reset_index().sort_values(by='Terjual per Bulan', ascending=False)
                    fig_cat = px.bar(category_sales, x='Kategori', y='Terjual per Bulan', title='Total Penjualan per Kategori')
                    st.plotly_chart(fig_cat, use_container_width=True)
                st.divider()

                st.subheader("Produk Terlaris")
                top_products = my_store_rekap_df.sort_values(by='Terjual per Bulan', ascending=False).head(15)
                top_products_display = top_products[['Nama Produk', 'Terjual per Bulan', 'Omzet']].copy()
                top_products_display['Omzet'] = top_products_display['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
                st.dataframe(top_products_display, use_container_width=True)
                st.divider()

                st.subheader("Peta Kontribusi Omzet (Treemap)")
                omzet_per_produk = my_store_rekap_df.groupby('Nama Produk')['Omzet'].sum().reset_index()
                fig_treemap = px.treemap(omzet_per_produk, path=[px.Constant("Semua Produk"), 'Nama Produk'], values='Omzet', title='Kontribusi Omzet per Produk', hover_data={'Omzet':':,.0f'})
                fig_treemap.update_traces(textinfo="label+percent root")
                st.plotly_chart(fig_treemap, use_container_width=True)
                st.info("Arahkan kursor ke kotak untuk melihat detail. Ukuran kotak menunjukkan besarnya kontribusi omzet produk tersebut.")
                st.divider()

                st.subheader("Analisis Peluang yang Hilang (Penjualan vs. Stok Habis)")
                total_sales = my_store_rekap_df.groupby('Nama Produk')['Terjual per Bulan'].sum().reset_index()
                out_of_stock_products = my_store_rekap_df[my_store_rekap_df['Status'] == 'Habis']
                frequent_oos = out_of_stock_products['Nama Produk'].value_counts().reset_index()
                frequent_oos.columns = ['Nama Produk', 'Frekuensi Habis']
                merged_analysis_df = pd.merge(total_sales, frequent_oos, on='Nama Produk', how='outer').fillna(0)
                fig_scatter = px.scatter(merged_analysis_df, x='Terjual per Bulan', y='Frekuensi Habis', hover_data=['Nama Produk'], title='Korelasi Penjualan vs. Frekuensi Stok Habis', labels={'Terjual per Bulan': 'Total Unit Terjual (Semakin Laris ->)', 'Frekuensi Habis': 'Jumlah Stok Habis (Semakin Sering ->)'})
                st.plotly_chart(fig_scatter, use_container_width=True)
                st.warning("**Perhatian Khusus**: Produk di **pojok kanan atas** adalah produk terlaris Anda yang paling sering kehabisan stok. Ini adalah prioritas utama untuk perbaikan manajemen stok agar tidak kehilangan potensi omzet.")
                st.divider()

                st.subheader("Analisis Stok Mingguan")
                col1, col2 = st.columns(2)
                with col1:
                    my_store_rekap_df['Minggu'] = my_store_rekap_df['Tanggal'].dt.to_period('W-SUN').astype(str)
                    stock_weekly = my_store_rekap_df.groupby(['Minggu', 'Status']).size().unstack(fill_value=0)
                    if 'Tersedia' not in stock_weekly.columns: stock_weekly['Tersedia'] = 0
                    if 'Habis' not in stock_weekly.columns: stock_weekly['Habis'] = 0
                    fig_stock_week = px.line(stock_weekly, x=stock_weekly.index, y=['Tersedia', 'Habis'], title="Perbandingan Produk Tersedia vs Habis", markers=True, labels={'value': 'Jumlah Produk', 'Minggu': 'Minggu'})
                    st.plotly_chart(fig_stock_week, use_container_width=True)
                with col2:
                    frequent_oos_display = frequent_oos.head(10)
                    frequent_oos_display.rename(columns={'Nama Produk': 'Produk Paling Sering Habis', 'Frekuensi Habis': 'Jumlah Tercatat Habis'}, inplace=True)
                    st.dataframe(frequent_oos_display, use_container_width=True)

    with created_tabs[start_index]:
        st.header(f"Perbandingan Produk '{main_store_for_comp}'")
        st.subheader(f"Ringkasan Kinerja Mingguan untuk '{main_store_for_comp}'")
        if not main_store_df.empty:
            main_store_df_copy = main_store_df.copy()
            main_store_df_copy['Minggu'] = main_store_df_copy['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time)
            weekly_stats_list = []
            for week_start_date, group in main_store_df_copy.groupby('Minggu'):
                days_in_week_data = group['Tanggal'].nunique()
                if days_in_week_data == 0: continue
                total_omzet_mingguan = group['Omzet'].sum()
                avg_harga = group['Harga'].mean()
                stats = {'start_date': week_start_date, 'Total Omzet per Bulan': total_omzet_mingguan, 'Rata - Rata Omzet Per Hari': total_omzet_mingguan / 30, 'Total Produk Terjual per Bulan': int(group['Terjual per Bulan'].sum()), 'Rata - Rata Terjual Per Hari': int(group['Terjual per Bulan'].sum() / days_in_week_data), 'Rata - Rata Harga Per Produk': avg_harga if not pd.isna(avg_harga) else 0}
                weekly_stats_list.append(stats)
            if weekly_stats_list:
                consolidated_df = create_consolidated_weekly_summary(weekly_stats_list)
                st.dataframe(consolidated_df, hide_index=True, use_container_width=True)
        st.divider()

        st.header("Detail Perbandingan Produk")
        main_store_df_latest = main_store_df[main_store_df['Tanggal'] == latest_date_in_range].copy()
        competitor_df_latest = competitor_df[competitor_df['Tanggal'] == latest_date_in_range].copy()
        if not main_store_df_latest.empty and not competitor_df_latest.empty:
            matched_product_names = find_products_with_matches(main_store_df, competitor_df_latest)
            st.markdown(f"Ditemukan **{len(matched_product_names)}** dari **{len(main_store_df_latest)}** produk Anda yang juga dijual oleh kompetitor. Produk ini ditandai biru muda.")
            st.dataframe(main_store_df_latest[['Nama Produk', 'Harga', 'Status']].style.apply(style_matched_products, matched_set=matched_product_names, axis=1), use_container_width=True)
            st.divider()
            product_list = sorted(main_store_df_latest['Nama Produk'].unique())
            selected_product_name = st.selectbox("Pilih produk untuk dibandingkan:", options=product_list, index=None, placeholder="Ketik untuk mencari produk...")
            if selected_product_name:
                product_info = main_store_df_latest[main_store_df_latest['Nama Produk'] == selected_product_name].iloc[0]
                st.subheader(f"Produk Acuan: *{product_info['Nama Produk']}*")
                st.metric(label=f"Harga di {main_store_for_comp}", value=f"Rp {product_info['Harga']:,.0f}")
                st.subheader("Hasil Perbandingan di Toko Kompetitor")
                with st.spinner("Mencari produk yang mirip..."):
                    similar_products = get_smart_matches(product_info, competitor_df_latest, score_cutoff=85)
                if not similar_products:
                    st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                else:
                    for product, score in similar_products:
                        match_info = competitor_df_latest[competitor_df_latest['Nama Produk'] == product].iloc[0]
                        price_diff_rp = match_info['Harga'] - product_info['Harga']
                        delta_color = "normal" if price_diff_rp < 0 else "inverse"
                        col1, col2, col3 = st.columns([2,1,1])
                        with col1:
                            st.markdown(f"**Toko:** {match_info['Toko']}**\n\n*{match_info['Nama Produk']}*")
                            st.markdown(f"**Harga:** Rp {match_info['Harga']:,.0f}")
                        with col2:
                            st.metric(label="Perbedaan Harga", value=f"Rp {price_diff_rp:,.0f}", delta=f"{price_diff_rp:,.0f}", delta_color=delta_color)
                        with col3:
                            st.markdown(f"**Status Stok:** {match_info['Status']}")
                        st.divider()
        else:
            st.info("Tidak ada data toko utama atau kompetitor pada tanggal terbaru di rentang yang dipilih.")

    with created_tabs[start_index + 1]:
        st.header("Status Stok per Toko (pada hari terakhir data)")
        stock_status = df_latest.groupby(['Toko', 'Status']).size().unstack(fill_value=0)
        if 'Tersedia' not in stock_status.columns: stock_status['Tersedia'] = 0
        if 'Habis' not in stock_status.columns: stock_status['Habis'] = 0
        stock_status = stock_status[['Tersedia', 'Habis']]
        stock_status_melted = stock_status.reset_index().melt(id_vars='Toko', value_vars=['Tersedia', 'Habis'], var_name='Status Stok', value_name='Jumlah Produk')
        fig_stock = px.bar(stock_status_melted, x='Toko', y='Jumlah Produk', color='Status Stok', barmode='group', title="Perbandingan Produk Tersedia vs. Habis per Toko", labels={'Jumlah Produk': 'Jumlah Produk', 'Toko': 'Toko'}, color_discrete_map={'Tersedia': '#4CAF50', 'Habis': '#F44336'})
        st.plotly_chart(fig_stock, use_container_width=True)
        st.dataframe(stock_status, use_container_width=True)

    with created_tabs[start_index + 2]:
        st.header("💡 Rekomendasi Analisis Lanjutan")
        st.markdown("- **Produk Paling Laku:** Identifikasi produk dengan total `Terjual per Bulan` tertinggi di semua toko dalam rentang waktu yang dipilih.\n- **Analisis Kategori:** Jika data memiliki kategori produk, analisis kategori mana yang paling mendominangkan omzet.\n- **Peluang Stok:** Cari produk yang `Habis` di toko Anda tapi `Tersedia` di banyak kompetitor.")

    with created_tabs[start_index + 3]:
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        st.subheader("Grafik Omzet per Tanggal")
        omzet_over_time = df_filtered.groupby(['Tanggal', 'Toko'])['Omzet'].sum().reset_index()
        fig_omzet = px.line(omzet_over_time, x='Tanggal', y='Omzet', color='Toko', title="Perbandingan Omzet Harian Antar Toko", labels={'Omzet': 'Total Omzet (Rp)', 'Tanggal': 'Tanggal', 'Toko': 'Nama Toko'})
        st.plotly_chart(fig_omzet, use_container_width=True)

        st.subheader("Grafik Penjualan Unit per Minggu")
        df_filtered['Minggu_str'] = df_filtered['Tanggal'].dt.to_period('W-SUN').astype(str)
        sales_over_week = df_filtered.groupby(['Minggu_str', 'Toko'])['Terjual per Bulan'].sum().reset_index()
        fig_sales_weekly = px.line(sales_over_week, x='Minggu_str', y='Terjual per Bulan', color='Toko', title="Perbandingan Jumlah Unit Terjual per Minggu", labels={'Terjual per Bulan': 'Total Unit Terjual', 'Minggu_str': 'Minggu', 'Toko': 'Nama Toko'}, markers=True)
        st.plotly_chart(fig_sales_weekly, use_container_width=True)
        st.divider()
        
        st.header("Ringkasan Kinerja Mingguan per Toko")
        if not df_filtered.empty:
            store_groups = df_filtered.groupby('Toko')
            for store_name, store_df_group in store_groups:
                st.subheader(f"Ringkasan Kinerja Mingguan untuk '{store_name}'")
                store_df_copy = store_df_group.copy()
                store_df_copy['Minggu'] = store_df_copy['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time)
                weekly_stats_list = []
                for week_start_date, group in store_df_copy.groupby('Minggu'):
                    days_in_data = group['Tanggal'].nunique()
                    if days_in_data == 0: continue
                    total_omzet_mingguan = group['Omzet'].sum()
                    avg_harga = group['Harga'].mean()
                    stats = {'start_date': week_start_date, 'Total Omzet per Bulan': total_omzet_mingguan, 'Rata - Rata Omzet Per Hari': total_omzet_mingguan / 30, 'Total Produk Terjual per Bulan': int(group['Terjual per Bulan'].sum()), 'Rata - Rata Terjual Per Hari': int(group['Terjual per Bulan'].sum() / days_in_data), 'Rata - Rata Harga Per Produk': avg_harga if not pd.isna(avg_harga) else 0}
                    weekly_stats_list.append(stats)
                if weekly_stats_list:
                    consolidated_df = create_consolidated_weekly_summary(weekly_stats_list)
                    st.dataframe(consolidated_df, hide_index=True, use_container_width=True)
                st.divider()

else:
    st.info("👈 Klik tombol di sidebar untuk menarik data dan memulai analisis.")
    st.stop()
