import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import re
import gspread

st.set_page_config(layout="wide", page_title="Dashboard Analisis")

# ===================================================================================
# FUNGSI UTAMA LOAD DATA
# ===================================================================================
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    try:
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key("1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ")
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: {e}")
        return None, None, None

    rekap_list_df, database_df = [], pd.DataFrame()
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
    try:
        for sheet_name in sheet_names:
            sheet = spreadsheet.worksheet(sheet_name)
            all_values = sheet.get_all_values()
            if not all_values or len(all_values) < 2:
                continue
            header = all_values[0]
            data = all_values[1:]
            df_sheet = pd.DataFrame(data, columns=header)
            if '' in df_sheet.columns:
                df_sheet = df_sheet.drop(columns=[''])

            if "DATABASE" in sheet_name.upper():
                database_df = df_sheet
            elif "REKAP" in sheet_name.upper():
                if df_sheet.empty: continue
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                df_sheet['Toko'] = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
                df_sheet['Status'] = 'Tersedia' if "READY" in sheet_name.upper() else 'Habis'
                rekap_list_df.append(df_sheet)
    except Exception as e:
        st.error(f"Gagal memproses sheet: {e}.")
        return None, None, None

    if not rekap_list_df:
        return None, None, None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    for df in [database_df, rekap_df]:
        if not df.empty:
            df.columns = [str(col).strip().upper() for col in df.columns]

    rename_map = {'NAMA': 'Nama Produk','TERJUAL/BLN': 'Terjual per Bulan','TANGGAL': 'Tanggal','HARGA': 'Harga','BRAND': 'Brand','STOK': 'Stok','TOKO': 'Toko','STATUS': 'Status'}
    rekap_df.rename(columns=rename_map, inplace=True)
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    if 'Stok' not in rekap_df.columns: rekap_df['Stok'] = 'N/A'

    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)

    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Terjual per Bulan'] = rekap_df['Terjual per Bulan'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']
    rekap_df.drop_duplicates(subset=['Nama Produk', 'Toko', 'Tanggal'], inplace=True, keep='last')

    return rekap_df.sort_values('Tanggal'), database_df, spreadsheet

# ===================================================================================
# FUNGSI FUZZY UPDATE DAN LOAD HASIL
# ===================================================================================
def update_fuzzy_sheet(df, spreadsheet, sheet_name="hasil_fuzzy", score_cutoff=90):
    try:
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="10")

        main_store_latest = df[df['Toko'] == "DB KLIK"]
        competitor_latest = df[df['Toko'] != "DB KLIK"]
        latest_date = df['Tanggal'].max()
        main_store_latest = main_store_latest[main_store_latest['Tanggal'] == latest_date]
        competitor_latest = competitor_latest[competitor_latest['Tanggal'] == latest_date]

        results = []
        for _, row in main_store_latest.iterrows():
            product_name = row['Nama Produk']
            matches = process.extract(product_name, competitor_latest['Nama Produk'].tolist(), limit=5, scorer=fuzz.token_set_ratio)
            for match_name, score in matches:
                if score >= score_cutoff:
                    comp_row = competitor_latest[competitor_latest['Nama Produk'] == match_name].iloc[0]
                    results.append([
                        row['Nama Produk'], row['Harga'], row['Status'], row['Stok'],
                        comp_row['Toko'], comp_row['Nama Produk'], comp_row['Harga'], comp_row['Status'], comp_row['Stok'],
                        score
                    ])

        header = ["Produk DB KLIK","Harga DBK","Status DBK","Stok DBK","Toko Kompetitor","Produk Kompetitor","Harga Kompetitor","Status Kompetitor","Stok Kompetitor","Skor Similarity"]
        sheet.update([header] + results)
        st.success(f"✅ Sheet '{sheet_name}' berhasil diperbarui dengan {len(results)} baris.")
    except Exception as e:
        st.error(f"Gagal update sheet fuzzy: {e}")

@st.cache_data(show_spinner=False)
def load_fuzzy_results(spreadsheet, sheet_name="hasil_fuzzy"):
    try:
        sheet = spreadsheet.worksheet(sheet_name)
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    except gspread.exceptions.WorksheetNotFound:
        return pd.DataFrame()

# ===================================================================================
# MAIN APP
# ===================================================================================
st.title("📊 Dashboard Analisis Penjualan & Kompetitor")

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if not st.session_state.data_loaded:
    if st.button("Tarik Data & Mulai Analisis 🚀"):
        df, db_df, spreadsheet = load_data_from_gsheets()
        if df is not None:
            st.session_state.df = df
            st.session_state.db_df = db_df
            st.session_state.spreadsheet = spreadsheet
            st.session_state.data_loaded = True
            st.rerun()
    st.stop()

df = st.session_state.df

db_df = st.session_state.db_df

spreadsheet = st.session_state.spreadsheet

# Nama toko utama (default). Jika mau diganti, ubah di sini atau buat input di sidebar.
my_store_name = "DB KLIK"

# Sidebar tambahan tombol update fuzzy
accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)
if st.sidebar.button("🔄 Update Hasil Fuzzy Mingguan"):
    update_fuzzy_sheet(df, spreadsheet, "hasil_fuzzy", score_cutoff=accuracy_cutoff)

# ===================================================================================
# TAB LAYOUT
# ===================================================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["⭐ Analisis Toko Saya", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

with tab1:
    st.header(f"Analisis Kinerja Toko: {my_store_name}")
    
    section_counter = 1

    st.subheader(f"{section_counter}. Analisis Kategori Terlaris (Berdasarkan Omzet)")
    section_counter += 1
    if not db_df.empty and 'KATEGORI' in db_df.columns and 'NAMA' in db_df.columns:
        @st.cache_data
        def fuzzy_merge_categories(_rekap_df, _database_df):
            _rekap_df['Kategori'] = 'Lainnya'
            db_unique = _database_df.drop_duplicates(subset=['NAMA'])
            db_map = db_unique.set_index('NAMA')['KATEGORI']
            for index, row in _rekap_df.iterrows():
                if pd.notna(row['Nama Produk']):
                    match, score = process.extractOne(row['Nama Produk'], db_map.index, scorer=fuzz.token_set_ratio)
                    if score >= 80:
                        _rekap_df.loc[index, 'Kategori'] = db_map[match]
            return _rekap_df
        
        main_store_cat = fuzzy_merge_categories(main_store_latest_overall.copy(), db_df)
        category_sales = main_store_cat.groupby('Kategori')['Omzet'].sum().reset_index()
        
        if not category_sales.empty:
            col1, col2 = st.columns([1,2])
            sort_order_cat = col1.radio("Urutkan:", ["Omzet Tertinggi", "Omzet Terendah"], horizontal=True, key="cat_sort")
            top_n_cat = col2.number_input("Tampilkan Top:", 1, len(category_sales), min(10, len(category_sales)), key="cat_top_n")
            cat_sales_sorted = category_sales.sort_values('Omzet', ascending=(sort_order_cat == "Omzet Terendah")).head(top_n_cat)
            fig_cat = px.bar(cat_sales_sorted, x='Kategori', y='Omzet', title=f'Top {top_n_cat} Kategori Berdasarkan Omzet', text_auto='.2s')
            st.plotly_chart(fig_cat, use_container_width=True)

    st.subheader(f"{section_counter}. Produk Terlaris")
    section_counter += 1
    top_products = main_store_latest_overall.sort_values('Terjual per Bulan', ascending=False).head(15).copy()
    top_products['Harga_rp'] = top_products['Harga'].apply(lambda x: f"Rp {x:,.0f}")
    top_products['Omzet_rp'] = top_products['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
    top_products['Tanggal_fmt'] = top_products['Tanggal'].dt.strftime('%Y-%m-%d')
    
    display_df_top = top_products[['Nama Produk', 'Harga_rp', 'Omzet_rp', 'Tanggal_fmt']].rename(
        columns={'Harga_rp': 'Harga', 'Omzet_rp': 'Omzet', 'Tanggal_fmt': 'Update Terakhir'}
    )
    st.dataframe(display_df_top, use_container_width=True, hide_index=True)

    st.subheader(f"{section_counter}. Distribusi Omzet Brand")
    section_counter += 1
    brand_omzet_main = main_store_latest_overall.groupby('Brand')['Omzet'].sum().reset_index()
    if not brand_omzet_main.empty:
        fig_brand_pie = px.pie(brand_omzet_main.sort_values('Omzet', ascending=False).head(7), 
                            names='Brand', 
                            values='Omzet', 
                            title='Distribusi Omzet Top 7 Brand (Snapshot Terakhir)')
        fig_brand_pie.update_traces(
            textposition='outside', 
            texttemplate='<b>%{label}</b><br>%{percent}<br>Rp %{value:,.0f}'
        )
        st.plotly_chart(fig_brand_pie, use_container_width=True)
    else:
        st.info("Tidak ada data omzet brand.")

    st.subheader(f"{section_counter}. Ringkasan Kinerja Mingguan (WoW Growth)")
    section_counter += 1
    
    main_store_latest_weekly = main_store_df.loc[main_store_df.groupby(['Minggu', 'Nama Produk'])['Tanggal'].idxmax()]
    
    weekly_summary_tab1 = main_store_latest_weekly.groupby('Minggu').agg(
        Omzet=('Omzet', 'sum'),
        Penjualan_Unit=('Terjual per Bulan', 'sum')
    ).reset_index()
    
    weekly_summary_tab1.sort_values('Minggu', inplace=True) 
    
    weekly_summary_tab1['Pertumbuhan Omzet (WoW)'] = weekly_summary_tab1['Omzet'].pct_change().apply(format_wow_growth)
    weekly_summary_tab1['Omzet'] = weekly_summary_tab1['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
    st.dataframe(
        weekly_summary_tab1[['Minggu', 'Omzet', 'Penjualan_Unit', 'Pertumbuhan Omzet (WoW)']].style.apply(
            lambda s: s.map(style_wow_growth), subset=['Pertumbuhan Omzet (WoW)']
        ), 
        use_container_width=True, 
        hide_index=True
    )

with tab2:
    st.header("Perbandingan Produk DB KLIK dengan Kompetitor")
    fuzzy_results_df = load_fuzzy_results(spreadsheet, "hasil_fuzzy")
    if fuzzy_results_df.empty:
        st.info("Belum ada data fuzzy. Klik tombol update di sidebar.")
    else:
        selected_product = st.selectbox("Pilih produk dari toko Anda:", sorted(fuzzy_results_df["Produk DB KLIK"].unique()))
        if selected_product:
            subset = fuzzy_results_df[fuzzy_results_df["Produk DB KLIK"] == selected_product]
            st.dataframe(subset, use_container_width=True, hide_index=True)

with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    competitor_latest_overall = df.loc[df.groupby(['Toko','Nama Produk'])['Tanggal'].idxmax()]
    competitor_df = df[df['Toko'] != "DB KLIK"]
    if competitor_df.empty:
        st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
    else:
        competitor_list = sorted(competitor_df['Toko'].unique())
        for competitor_store in competitor_list:
            with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                single_comp = competitor_latest_overall[competitor_latest_overall['Toko']==competitor_store]
                brand_analysis = single_comp.groupby('Brand').agg(Total_Omzet=('Omzet','sum'),Total_Unit_Terjual=('Terjual per Bulan','sum')).reset_index().sort_values("Total_Omzet",ascending=False)
                st.dataframe(brand_analysis, use_container_width=True, hide_index=True)
                if not brand_analysis.empty:
                    fig = px.pie(brand_analysis.head(7), names='Brand', values='Total_Omzet', title=f'Top 7 Brand di {competitor_store}')
                    st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    df['Minggu'] = df['Tanggal'].dt.to_period('W-SUN').apply(lambda p:p.start_time).dt.date
    stock_trends = df.groupby(['Minggu','Toko','Status']).size().unstack(fill_value=0).reset_index()
    stock_trends_melted = stock_trends.melt(id_vars=['Minggu','Toko'], value_vars=['Tersedia','Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
    fig = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)

with tab5:
    st.header("Analisis Kinerja Penjualan (Semua Toko)")
    latest_entries_weekly = df.loc[df.groupby(['Minggu','Toko','Nama Produk'])['Tanggal'].idxmax()]
    omzet_weekly = latest_entries_weekly.groupby(['Minggu','Toko'])['Omzet'].sum().reset_index()
    fig = px.line(omzet_weekly, x='Minggu', y='Omzet', color='Toko', markers=True)
    st.plotly_chart(fig, use_container_width=True)
    st.subheader("Tabel Rincian Omzet per Tanggal")
    omzet_pivot = df.pivot_table(index='Toko',columns='Tanggal',values='Omzet',aggfunc='sum').fillna(0)
    omzet_pivot.reset_index(inplace=True)
    st.dataframe(omzet_pivot, use_container_width=True, hide_index=True)

with tab6:
    st.header("Analisis Produk Baru Mingguan")
    weeks = sorted(df['Minggu'].unique())
    if len(weeks)<2:
        st.info("Butuh setidaknya 2 minggu data.")
    else:
        col1,col2 = st.columns(2)
        week_before = col1.selectbox("Minggu Pembanding:", weeks, index=0)
        week_after = col2.selectbox("Minggu Penentu:", weeks, index=len(weeks)-1)
        if week_before>=week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            all_stores = sorted(df['Toko'].unique())
            for store in all_stores:
                with st.expander(f"Produk Baru di {store}"):
                    products_before = set(df[(df['Toko']==store)&(df['Minggu']==week_before)&(df['Status']=='Tersedia')]['Nama Produk'])
                    products_after = set(df[(df['Toko']==store)&(df['Minggu']==week_after)&(df['Status']=='Tersedia')]['Nama Produk'])
                    new_products = products_after-products_before
                    if not new_products:
                        st.write("Tidak ada produk baru.")
                    else:
                        new_df = df[df['Nama Produk'].isin(new_products)&(df['Toko']==store)&(df['Minggu']==week_after)]
                        st.dataframe(new_df[['Nama Produk','Harga','Stok','Brand']], use_container_width=True, hide_index=True)
