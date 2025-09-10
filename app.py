# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI FINAL V6 - ADAPTASI
#  Dibuat oleh: Firman & Asisten AI Gemini (adapted)
#  Catatan: Versi ini menggabungkan logic V6 (Google Sheets + kamus_brand)
#  dengan layout dan tab analisis yang diminta sebelumnya.
# ===================================================================================

import streamlit as st
import pandas as pd
from thefuzz import process, fuzz
import plotly.express as px
import plotly.graph_objects as go
import re
import gspread
from io import BytesIO

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis - DB KLIK")

# ===================================================================================
# FUNGSI-FUNGSI
# ===================================================================================
@st.cache_data(show_spinner="Mengambil data dari Google Sheets...")
def load_data_from_gsheets():
    try:
        creds_dict = {
            "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\n', '\\n'),
            "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        gc = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gc.open_by_key(st.secrets.get("gsheet_key"))
    except Exception as e:
        st.error(f"GAGAL KONEKSI KE GOOGLE SHEETS: {e}")
        return None, None, None

    sheet_names = [
        "DATABASE", "kamus_brand", "DB KLIK - REKAP - READY", "DB KLIK - REKAP - HABIS",
        "ABDITAMA - REKAP - READY", "ABDITAMA - REKAP - HABIS",
        "LEVEL99 - REKAP - READY", "LEVEL99 - REKAP - HABIS",
        "JAYA PC - REKAP - READY", "JAYA PC - REKAP - HABIS",
        "MULTIFUNGSI - REKAP - READY", "MULTIFUNGSI - REKAP - HABIS",
        "IT SHOP - REKAP - READY", "IT SHOP - REKAP - HABIS",
        "SURYA MITRA ONLINE - REKAP - READY", "SURYA MITRA ONLINE - REKAP - HABIS",
        "GG STORE - REKAP - READY", "GG STORE - REKAP - HABIS",
        "TECH ISLAND - REKAP - READY", "TECH ISLAND - REKAP - HABIS"
    ]

    rekap_list = []
    database_df = pd.DataFrame()
    kamus_brand_df = pd.DataFrame()

    try:
        for sheet_name in sheet_names:
            try:
                ws = spreadsheet.worksheet(sheet_name)
            except Exception:
                continue
            values = ws.get_all_values()
            if not values or len(values) < 2:
                continue
            header = values[0]
            data = values[1:]
            df = pd.DataFrame(data, columns=header)
            # drop empty unnamed
            df = df.loc[:, ~df.columns.str.strip().eq('')]

            if sheet_name.upper() == 'DATABASE':
                database_df = df
            elif sheet_name.lower().startswith('kamus'):
                kamus_brand_df = df
            elif 'REKAP' in sheet_name.upper():
                store_name_match = re.match(r"^(.*?) - REKAP", sheet_name, re.IGNORECASE)
                toko = store_name_match.group(1).strip() if store_name_match else sheet_name
                df['Toko'] = toko
                df['Status'] = 'Tersedia' if 'READY' in sheet_name.upper() else 'Habis'
                rekap_list.append(df)

    except Exception as e:
        st.error(f"Gagal memuat sheet: {e}")
        return None, None, None

    if not rekap_list:
        st.error('Tidak ada data REKAP yang valid di Google Sheets.')
        return None, None, None

    rekap_df = pd.concat(rekap_list, ignore_index=True)

    # Normalize columns
    def norm_cols(df):
        df = df.copy()
        df.columns = [str(c).strip().upper() for c in df.columns]
        return df

    database_df = norm_cols(database_df) if not database_df.empty else database_df
    kamus_brand_df = norm_cols(kamus_brand_df) if not kamus_brand_df.empty else kamus_brand_df
    rekap_df = norm_cols(rekap_df)

    # Rename to expected keys
    rename_map = {
        'NAMA': 'Nama Produk', 'NAMA PRODUK':'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TERJUAL':'Terjual per Bulan',
        'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'
    }
    rekap_df.rename(columns=lambda x: rename_map.get(x, x).strip(), inplace=True)

    # Ensure columns exist
    if 'Nama Produk' in rekap_df.columns:
        rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    else:
        st.error('Kolom Nama Produk tidak ditemukan di REKAP.'); return None, None, None

    if 'Tanggal' not in rekap_df.columns:
        st.error('Kolom Tanggal tidak ditemukan di REKAP.'); return None, None, None

    if 'Harga' not in rekap_df.columns:
        st.error('Kolom Harga tidak ditemukan di REKAP.'); return None, None, None

    if 'Terjual per Bulan' not in rekap_df.columns:
        rekap_df['Terjual per Bulan'] = 0

    # Clean types
    rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)
    rekap_df.dropna(subset=['Tanggal','Nama Produk','Harga'], inplace=True)

    rekap_df['Harga'] = rekap_df['Harga'].astype(int)
    rekap_df['Terjual per Bulan'] = rekap_df['Terjual per Bulan'].astype(int)
    rekap_df['Omzet'] = rekap_df['Harga'] * rekap_df['Terjual per Bulan']

    # Standardize brand using kamus_brand if present
    if not kamus_brand_df.empty and 'ALIAS' in kamus_brand_df.columns and 'BRAND_UTAMA' in kamus_brand_df.columns:
        kamus_brand_df.dropna(subset=['ALIAS','BRAND_UTAMA'], inplace=True)
        alias_map = {str(k).strip().upper(): str(v).strip().upper() for k,v in kamus_brand_df.set_index('ALIAS')['BRAND_UTAMA'].to_dict().items()}
        def std_brand(b):
            if pd.isna(b): return 'UNKNOWN'
            return alias_map.get(str(b).strip().upper(), str(b).strip().upper())
        rekap_df['Brand'] = rekap_df.get('Brand', '').apply(std_brand)
    else:
        if 'Brand' not in rekap_df.columns:
            rekap_df['Brand'] = rekap_df['Nama Produk'].str.split().str[0].str.upper()

    rekap_df.drop_duplicates(subset=['Nama Produk','Toko','Tanggal'], inplace=True, keep='last')

    return rekap_df.sort_values('Tanggal'), database_df, kamus_brand_df

@st.cache_data(show_spinner=False)
def get_smart_matches(query_name, competitor_list, score_cutoff=90):
    candidates = process.extract(query_name, competitor_list, limit=20, scorer=fuzz.token_set_ratio)
    return [c for c in candidates if c[1] >= score_cutoff][:5]


def format_wow_growth(pct):
    if pd.isna(pct) or pct==float('inf'):
        return 'N/A'
    if pct>0:
        return f'▲ {pct:.1%}'
    if pct<0:
        return f'▼ {abs(pct):.1%}'
    return '▬ 0.0%'

@st.cache_data
def convert_df_for_download(df):
    return df.to_csv(index=False).encode('utf-8')

@st.cache_data
def convert_df_to_json(df):
    return df.to_json(orient='records', indent=4).encode('utf-8')

# ===================================================================================
# UI - Load Data
# ===================================================================================
st.title('📊 Dashboard Analisis Penjualan & Kompetitor - Adaptasi')

if 'data_loaded' not in st.session_state:
    st.session_state['data_loaded'] = False

if not st.session_state['data_loaded']:
    c1, c2, c3 = st.columns([2,3,2])
    with c2:
        if st.button('Tarik Data & Mulai Analisis 🚀'):
            df, db_df, kamus_df = load_data_from_gsheets()
            if df is None:
                st.error('Gagal memuat data. Cek error di atas.')
            else:
                st.session_state.df = df
                st.session_state.db_df = db_df
                st.session_state.kamus_df = kamus_df
                st.session_state.data_loaded = True
                st.experimental_rerun()
    st.info('Klik tombol untuk tarik data dari Google Sheets (atau pastikan st.secrets terkonfigurasi).')
    st.stop()

# ===================================================================================
# Setelah data dimuat
# ===================================================================================
df = st.session_state.df
db_df = st.session_state.db_df
my_store_name = 'DB KLIK'

st.sidebar.header('Kontrol & Filter')
if st.sidebar.button('Hapus Cache & Tarik Ulang 🔄'):
    st.cache_data.clear()
    st.session_state.data_loaded = False
    st.experimental_rerun()

st.sidebar.divider()
min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
selected_date_range = st.sidebar.date_input('Rentang Tanggal', [min_date, max_date], min_value=min_date, max_value=max_date)
if len(selected_date_range) != 2:
    st.sidebar.warning('Pilih rentang tanggal yang valid.'); st.stop()
start_date, end_date = selected_date_range
accuracy_cutoff = st.sidebar.slider('Tingkat Akurasi Pencocokan (%)', 80, 100, 91)
show_top_bars = st.sidebar.slider('Jumlah bar kategori (Tab 1)', 3, 30, 10)

# filter
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error('Tidak ada data pada rentang tanggal yang dipilih.'); st.stop()

# weekly & latest snapshots
# latest per week per product (for trends)
df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
latest_entries_weekly = df_filtered.loc[df_filtered.groupby(['Minggu','Toko','Nama Produk'])['Tanggal'].idxmax()].copy()
# latest overall snapshot per product for aggregates
latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko','Nama Produk'])['Tanggal'].idxmax()].copy()

main_store_df = df_filtered[df_filtered['Toko']==my_store_name].copy()
competitor_df = df_filtered[df_filtered['Toko']!=my_store_name].copy()

main_latest_overall = latest_entries_overall[latest_entries_overall['Toko']==my_store_name].copy()
competitor_latest_overall = latest_entries_overall[latest_entries_overall['Toko']!=my_store_name].copy()

# Sidebar export
st.sidebar.header('Ekspor & Info Data')
st.sidebar.info(f"Baris yg diolah: {len(df_filtered)}")
st.sidebar.download_button('📥 Unduh CSV', data=convert_df_for_download(df_filtered), file_name=f'analisis_{start_date}_{end_date}.csv')
st.sidebar.download_button('📥 Unduh JSON', data=convert_df_to_json(df_filtered), file_name=f'analisis_{start_date}_{end_date}.json')

st.info('💡 Kalkulasi dasar: snapshot terakhir tiap produk digunakan untuk agregat (pie, ranking).')

# ===================================================================================
# Tabs: sesuai permintaan user (1..6)
# ===================================================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(['Kategori Terlaris','Banding Produk','Analisis Brand Kompetitor','Tren Status Stok Mingguan','Omzet Semua Toko (Timeline)','Produk Baru Mingguan'])

with tab1:
    st.header('1. Analisis Kategori Terlaris (Berdasarkan Omzet)')
    # Use fuzzy between main store products and database labels
    db_labels = db_df['NAMA'].unique().tolist() if (db_df is not None and not db_df.empty and 'NAMA' in db_df.columns) else []
    main_products = main_latest_overall['Nama Produk'].unique().tolist()
    label_map = {}
    if db_labels:
        for p in main_products:
            best = process.extractOne(p, db_labels, scorer=fuzz.token_sort_ratio)
            label_map[p] = best[0] if best and best[1]>=accuracy_cutoff else 'UNLABELED'
    else:
        label_map = {p:'UNLABELED' for p in main_products}

    main_latest_overall['Kategori'] = main_latest_overall['Nama Produk'].map(label_map).fillna('UNLABELED')
    cat_agg = main_latest_overall.groupby('Kategori').agg({'Omzet':'sum','Terjual per Bulan':'sum'}).reset_index().sort_values('Omzet',ascending=False)

    topn = st.number_input('Tampilkan berapa kategori', min_value=3, max_value=50, value=show_top_bars)
    display = cat_agg.head(int(topn))
    fig = px.bar(display, x='Kategori', y='Omzet', text='Omzet', title='Omzet per Kategori (DB KLIK)')
    fig.update_layout(yaxis_title='Omzet (Rp)')
    st.plotly_chart(fig, use_container_width=True)

    st.subheader('Tabel Produk Paling Laris dari Kategori Terpilih')
    sel_cat = st.selectbox('Pilih Kategori', options=cat_agg['Kategori'].tolist())
    if sel_cat:
        subset = main_latest_overall[main_latest_overall['Kategori']==sel_cat].sort_values('Omzet', ascending=False)
        table = subset[['Nama Produk','Harga','Terjual per Bulan','Status']].rename(columns={'Harga':'Harga pada tanggal terakhir','Terjual per Bulan':'Terjual/Bulan pada tanggal terakhir'})
        st.dataframe(table)

    st.markdown('---')
    st.subheader('Produk Terlaris (Global)')
    prod_agg = latest_entries_overall.groupby('Nama Produk').agg({'Harga':'last','Terjual per Bulan':'last','Omzet':'sum','Tanggal':'max'}).reset_index().sort_values('Omzet',ascending=False)

    # compute indicator (this week vs prev week) using weekly snapshots
    def indicator(prod_name):
        prod_week = latest_entries_weekly[latest_entries_weekly['Nama Produk']==prod_name]
        if prod_week.empty:
            return ' - '
        weeks = prod_week['Minggu'].sort_values().unique()
        if len(weeks)<2:
            return ' - '
        last_week = weeks[-1]
        prev_week = weeks[-2]
        s1 = prod_week[prod_week['Minggu']==last_week]['Omzet'].sum()
        s0 = prod_week[prod_week['Minggu']==prev_week]['Omzet'].sum()
        if s0==0:
            return ' - '
        pct = (s1 - s0)/s0
        return format_wow_growth(pct)

    prod_agg['Indikator'] = prod_agg['Nama Produk'].apply(indicator)
    prod_agg_display = prod_agg.rename(columns={'Nama Produk':'Nama Produk','Harga':'Harga pada tanggal terakhir','Terjual per Bulan':'Terjual/Bulan pada tanggal terakhir','Omzet':'Omzet','Tanggal':'Tanggal'})
    st.dataframe(prod_agg_display.head(200))

    st.markdown('---')
    st.subheader('Distribusi Omzet Brand (DB KLIK)')
    brand_agg = main_latest_overall.groupby('Brand').agg({'Omzet':'sum'}).reset_index().sort_values('Omzet',ascending=False)
    if not brand_agg.empty:
        figp = px.pie(brand_agg, names='Brand', values='Omzet', title='Distribusi Omzet per Brand (DB KLIK)', hover_data=['Omzet'])
        st.plotly_chart(figp, use_container_width=True)

    st.markdown('---')
    st.subheader('Ringkasan Kinerja Mingguan (WoW Growth)')
    weekly = latest_entries_weekly[latest_entries_weekly['Toko']==my_store_name].groupby('Minggu').agg({'Omzet':'sum','Nama Produk':'count'}).reset_index().rename(columns={'Nama Produk':'Penjualan Unit'})
    weekly['Growth'] = weekly['Omzet'].pct_change()
    weekly['Pertumbuhan Omzet'] = weekly['Growth'].apply(format_wow_growth)
    st.dataframe(weekly[['Minggu','Omzet','Penjualan Unit','Pertumbuhan Omzet']])

with tab2:
    st.header('2. Pilih Produk untuk Dibandingkan')
    prod_options = main_latest_overall['Nama Produk'].unique().tolist()
    sel_prod = st.selectbox('Pilih produk (DB KLIK)', options=prod_options)
    if sel_prod:
        st.subheader('Informasi Produk (DB KLIK)')
        pdf = latest_entries_overall[(latest_entries_overall['Nama Produk']==sel_prod) & (latest_entries_overall['Toko']==my_store_name)]
        if pdf.empty:
            st.write('Data tidak ditemukan')
        else:
            row = pdf.iloc[0]
            st.write('Harga:', row['Harga'])
            st.write('Status:', row.get('Status','-'))
            st.write('Stok:', row.get('Stok','Data tidak ditemukan'))
            # price history
            hist = df_filtered[(df_filtered['Nama Produk']==sel_prod) & (df_filtered['Toko']==my_store_name)].sort_values('Tanggal')
            if not hist.empty:
                fig = px.line(hist, x='Tanggal', y='Harga', title=f'Perubahan Harga - {sel_prod}', markers=True)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader('Hasil Fuzzy Kemiripan Produk (Toko lain)')
        competitor_products = competitor_latest_overall['Nama Produk'].unique().tolist()
        matches = get_smart_matches(sel_prod, competitor_products, score_cutoff=accuracy_cutoff)
        match_rows = []
        for m,score in [(x[0],x[1]) for x in matches]:
            mp = competitor_latest_overall[competitor_latest_overall['Nama Produk']==m]
            if mp.empty: continue
            latest = mp.iloc[0]
            price_db = row['Harga'] if 'row' in locals() else None
            price_m = latest['Harga']
            if pd.isna(price_db) or pd.isna(price_m):
                cmp = 'Data tidak lengkap'
            else:
                diff = price_m - price_db
                if diff<0: cmp = f'Lebih murah ({abs(diff)})'
                elif diff>0: cmp = f'Lebih mahal ({diff})'
                else: cmp = 'Produk Harga Sama'
            match_rows.append({'Produk':m,'Toko':latest['Toko'],'Harga':price_m,'Status':latest.get('Status','-'),'Stok':latest.get('Stok','N/A'),'Similarity':score,'Perbandingan Harga':cmp})
        st.dataframe(pd.DataFrame(match_rows))

        # combined price trend
        comp_names = [sel_prod] + [r['Produk'] for r in match_rows]
        plot_df = df_filtered[df_filtered['Nama Produk'].isin(comp_names)][['Tanggal','Nama Produk','Harga']].dropna()
        if not plot_df.empty:
            fig = px.line(plot_df, x='Tanggal', y='Harga', color='Nama Produk', title='Perbandingan Perubahan Harga')
            st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header('3. Analisis Brand di Toko Kompetitor')
    src_list = df_filtered['Toko'].unique().tolist()
    sel_src = st.selectbox('Pilih toko kompetitor', options=[s for s in src_list if s!=my_store_name], index=0)
    view = df_filtered[df_filtered['Toko']==sel_src].groupby('Brand').agg({'Omzet':'sum','Nama Produk':'count'}).reset_index().rename(columns={'Nama Produk':'Unit Terjual','Omzet':'Total Omzet'}).sort_values('Total Omzet',ascending=False)
    st.dataframe(view)
    if not view.empty:
        fig = px.pie(view, names='Brand', values='Total Omzet', title=f'Distribusi Omzet per Brand - {sel_src}', hover_data=['Total Omzet'])
        st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header('4. Tren Status Stok Mingguan per Toko')
    tmp = df_filtered.copy()
    tmp['Minggu'] = tmp['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
    status = tmp.groupby(['Minggu','Toko','Status']).size().unstack(fill_value=0).reset_index()
    if 'Tersedia' not in status.columns: status['Tersedia']=0
    if 'Habis' not in status.columns: status['Habis']=0
    status = status.rename(columns={'Tersedia':'Ready','Habis':'Habis'})
    st.dataframe(status[['Minggu','Toko','Ready','Habis']])
    fig = go.Figure()
    stores = status['Toko'].unique()
    for s in stores:
        df_s = status[status['Toko']==s]
        fig.add_trace(go.Scatter(x=df_s['Minggu'], y=df_s['Ready'], mode='lines+markers', name=f'{s} - Ready'))
        fig.add_trace(go.Scatter(x=df_s['Minggu'], y=df_s['Habis'], mode='lines+markers', name=f'{s} - Habis'))
    fig.update_layout(title='Tren Ready/Habis per Toko (Mingguan)', xaxis_title='Minggu', yaxis_title='Jumlah Produk')
    st.plotly_chart(fig, use_container_width=True)

with tab5:
    st.header('5. Tabel Omzet dari Semua Toko (timeline)')
    pivot = df_filtered.groupby(['Tanggal','Toko']).agg({'Omzet':'sum'}).reset_index()
    pivot_table = pivot.pivot(index='Tanggal', columns='Toko', values='Omzet').fillna(0)
    st.dataframe(pivot_table)
    fig = go.Figure()
    for col in pivot_table.columns:
        fig.add_trace(go.Scatter(x=pivot_table.index, y=pivot_table[col], mode='lines+markers', name=col))
    fig.update_layout(title='Omzet per Toko (Time Series)', xaxis_title='Tanggal', yaxis_title='Omzet (Rp)')
    st.plotly_chart(fig, use_container_width=True)

with tab6:
    st.header('6. Analisis Produk Baru Mingguan')
    weeks = sorted(df_filtered['Minggu'].unique())
    if len(weeks)<2:
        st.write('Data mingguan tidak cukup untuk analisis produk baru.')
    else:
        w1 = st.selectbox('Minggu Pembanding', options=weeks, index=0)
        w2 = st.selectbox('Minggu Target', options=weeks, index=min(1,len(weeks)-1))
        d1 = df_filtered[df_filtered['Minggu']==w1]
        d2 = df_filtered[df_filtered['Minggu']==w2]
        new_prods = set(d2['Nama Produk'].unique()) - set(d1['Nama Produk'].unique())
        rows = []
        for p in new_prods:
            tmp = d2[d2['Nama Produk']==p]
            last = tmp.loc[tmp['Tanggal'].idxmax()]
            rows.append({'Nama Produk':p,'Harga pada tanggal terakhir':last['Harga'],'Stok':last.get('Stok','N/A'),'Brand':last.get('Brand','N/A')})
        st.dataframe(pd.DataFrame(rows))

st.caption('Aplikasi ini telah diadaptasi. Jika ingin saya tambahkan fitur styling, export Excel khusus, atau optimasi performa (differential caching), beri tahu saya.')
