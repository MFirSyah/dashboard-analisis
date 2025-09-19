# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI SBERT
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Metode Koneksi: Aman & Stabil (gspread + st.secrets individual)
#  Peningkatan: Menggunakan Sentence-Transformers (SBERT) untuk perbandingan
#               produk yang akurat dan menyimpan hasil di Google Sheets.
# ===================================================================================

# ===================================================================================
# IMPORT LIBRARY
# ===================================================================================
import streamlit as st
import pandas as pd
import plotly.express as px
import re
import gspread
from gspread_dataframe import set_with_dataframe
from sentence_transformers import SentenceTransformer, util
import torch

# ===================================================================================
# KONFIGURASI HALAMAN
# ===================================================================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis DB Klik")

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

@st.cache_resource(show_spinner="Memuat model AI untuk analisis teks...")
def load_sbert_model():
    """
    Memuat model Sentence Transformer. Di-cache agar hanya dimuat sekali.
    """
    return SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets(spreadsheet_id):
    """
    Fungsi untuk memuat dan memproses data dari Google Sheets.
    """
    try:
        creds_dict = {
            "type": st.secrets["gcp_type"],
            "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"],
            "private_key": st.secrets["gcp_private_key"],
            "client_email": st.secrets["gcp_client_email"],
            "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"],
            "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        
        sa = gspread.service_account_from_dict(creds_dict)
        spreadsheet = sa.open_by_key(spreadsheet_id)
        
        all_data = []
        
        # Daftar worksheet kompetitor dan DB Klik (SEKARANG TERMASUK LOGITECH)
        worksheets_to_load = [ws.title for ws in spreadsheet.worksheets() if "REKAP" in ws.title]

        for sheet_name in worksheets_to_load:
            worksheet = spreadsheet.worksheet(sheet_name)
            df = pd.DataFrame(worksheet.get_all_records())
            
            # Ekstrak nama toko dari nama worksheet
            toko_name = sheet_name.split(' - ')[0].strip()
            df['Toko'] = toko_name
            
            status = "Tersedia" if "READY" in sheet_name or "RE" in sheet_name else "Habis"
            df['Status'] = status
            
            all_data.append(df)
            
        df_combined = pd.concat(all_data, ignore_index=True)
        
        # Coba muat hasil SBERT yang sudah ada
        try:
            hasil_fuzzy_ws = spreadsheet.worksheet("hasil_fuzzy")
            df_hasil_sbert = pd.DataFrame(hasil_fuzzy_ws.get_all_records())
        except gspread.exceptions.WorksheetNotFound:
            df_hasil_sbert = pd.DataFrame() # Buat dataframe kosong jika sheet tidak ada

        # Pra-pemrosesan data
        df_combined.rename(columns={'NAMA': 'Nama Produk'}, inplace=True)
        df_combined['HARGA'] = pd.to_numeric(df_combined['HARGA'], errors='coerce')
        df_combined['Terjual/Bln'] = pd.to_numeric(df_combined['Terjual/Bln'], errors='coerce')
        df_combined.dropna(subset=['HARGA', 'Nama Produk', 'Brand'], inplace=True)
        df_combined['TANGGAL'] = pd.to_datetime(df_combined['TANGGAL'], errors='coerce')
        df_combined['Minggu'] = df_combined['TANGGAL'].dt.strftime('%Y-%U')
        
        return df_combined, df_hasil_sbert, sa, spreadsheet
        
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheets: {e}")
        return pd.DataFrame(), pd.DataFrame(), None, None

def run_sbert_analysis(df_all, model):
    """
    Menjalankan analisis SBERT untuk membandingkan produk DB Klik dengan kompetitor.
    """
    st.info("Memulai analisis SBERT. Proses ini mungkin memakan beberapa menit...")
    
    df_dbklik = df_all[df_all['Toko'] == 'DB KLIK'].copy()
    df_kompetitor = df_all[df_all['Toko'] != 'DB KLIK'].copy()

    # Pastikan nama produk unik untuk efisiensi
    df_dbklik.drop_duplicates(subset=['Nama Produk'], inplace=True)
    df_kompetitor.drop_duplicates(subset=['Nama Produk', 'Toko'], inplace=True)

    dbklik_names = df_dbklik['Nama Produk'].tolist()
    kompetitor_names = df_kompetitor['Nama Produk'].tolist()

    st.write("Membuat 'kamus' produk kompetitor...")
    # Buat embeddings untuk semua produk kompetitor sekali jalan
    kompetitor_embeddings = model.encode(kompetitor_names, convert_to_tensor=True, show_progress_bar=True)
    
    all_matches = []
    
    progress_bar = st.progress(0, text="Menganalisis produk DB Klik...")
    total_products = len(df_dbklik)

    # Iterasi per produk DB Klik
    for i, row_dbklik in df_dbklik.iterrows():
        nama_dbklik = row_dbklik['Nama Produk']
        brand_dbklik = row_dbklik['Brand']
        
        # Filter cepat berdasarkan brand
        df_candidates = df_kompetitor[df_kompetitor['Brand'] == brand_dbklik]
        
        if not df_candidates.empty:
            candidate_names = df_candidates['Nama Produk'].tolist()
            
            # Ambil embeddings yang relevan
            indices = [kompetitor_names.index(name) for name in candidate_names]
            candidate_embeddings = kompetitor_embeddings[indices]
            
            # Hitung kemiripan
            query_embedding = model.encode(nama_dbklik, convert_to_tensor=True)
            cos_scores = util.cos_sim(query_embedding, candidate_embeddings)[0]
            
            # Cari skor tertinggi
            top_results = torch.topk(cos_scores, k=min(3, len(cos_scores)))
            
            for score, idx in zip(top_results[0], top_results[1]):
                if score.item() > 0.85: # Ambang batas kemiripan
                    match = df_candidates.iloc[idx.item()]
                    all_matches.append({
                        'Nama Produk DBKlik': nama_dbklik,
                        'HARGA DBKlik': row_dbklik['HARGA'],
                        'Brand DBKlik': brand_dbklik,
                        'Nama Produk Kompetitor': match['Nama Produk'],
                        'HARGA Kompetitor': match['HARGA'],
                        'Toko Kompetitor': match['Toko'],
                        'Skor Kemiripan': score.item(),
                        'TANGGAL Analisis': pd.Timestamp.now().strftime('%Y-%m-%d')
                    })
        
        # Update progress bar
        progress_bar.progress((i + 1) / total_products, text=f"Menganalisis: {nama_dbklik[:50]}...")

    progress_bar.empty()
    st.success("Analisis SBERT selesai!")
    return pd.DataFrame(all_matches)

def update_gsheet_with_results(spreadsheet, sheet_name, df_results):
    """
    Menulis ulang hasil analisis ke worksheet 'hasil_fuzzy'.
    """
    with st.spinner(f"Menyimpan {len(df_results)} hasil ke Google Sheets..."):
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()
            set_with_dataframe(worksheet, df_results)
            st.success(f"Berhasil menyimpan hasil ke worksheet '{sheet_name}'.")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1", cols="1")
            set_with_dataframe(worksheet, df_results)
            st.success(f"Worksheet '{sheet_name}' baru telah dibuat dan diisi data.")
        except Exception as e:
            st.error(f"Gagal menyimpan hasil ke Google Sheets: {e}")

# ===================================================================================
# MAIN SCRIPT
# ===================================================================================
# --- Judul Dashboard ---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor DB KLIK")
st.markdown("Versi ini menggunakan **SBERT** untuk perbandingan produk yang lebih akurat.")

# --- Load Data ---
spreadsheet_id = st.secrets.get("gcp_spreadsheet_id", "1aIu4dy2Qk9pWsoD4nR1S-Yw2WwPT9yX1Bv-3j-zG-C4")
df_all, df_hasil_sbert_cache, sa, spreadsheet = load_data_from_gsheets(spreadsheet_id)

if df_all is not None and not df_all.empty:
    
    model = load_sbert_model() # Muat model AI
    
    # --- Logika Pengecekan & Pembaruan Otomatis SBERT ---
    df_kompetitor_all = df_all[df_all['Toko'] != 'DB KLIK']
    TANGGAL_terbaru_kompetitor = df_kompetitor_all['TANGGAL'].max().strftime('%Y-%m-%d')
    
    TANGGAL_analisis_terakhir = "1970-01-01" # Default TANGGAL lama
    if not df_hasil_sbert_cache.empty and 'TANGGAL Analisis' in df_hasil_sbert_cache.columns:
        TANGGAL_analisis_terakhir = pd.to_datetime(df_hasil_sbert_cache['TANGGAL Analisis']).max().strftime('%Y-%m-%d')

    needs_update = TANGGAL_terbaru_kompetitor > TANGGAL_analisis_terakhir
    
    st.sidebar.title("⚙️ Opsi Analisis")
    if st.sidebar.button("Jalankan Ulang Analisis SBERT Manual"):
        needs_update = True
        st.sidebar.info("Tombol manual ditekan, analisis akan dijalankan ulang.")

    if needs_update:
        st.warning(f"Data kompetitor terdeteksi baru ({TANGGAL_terbaru_kompetitor}) atau pembaruan manual diminta. Menjalankan analisis SBERT...")
        df_hasil_sbert_baru = run_sbert_analysis(df_all, model)
        if not df_hasil_sbert_baru.empty:
            update_gsheet_with_results(spreadsheet, "hasil_fuzzy", df_hasil_sbert_baru)
            # Muat ulang data hasil untuk memastikan state konsisten
            _ , df_hasil_sbert_cache, _, _ = load_data_from_gsheets(spreadsheet_id)
            st.cache_data.clear() # Hapus cache agar data baru terbaca
    else:
        st.success(f"Data perbandingan produk sudah yang terbaru (Analisis terakhir: {TANGGAL_analisis_terakhir}).")
    
    # --- Tab Utama ---
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard Utama", "⚖️ Perbandingan HARGA (SBERT)", "📈 Analisis Tren", "🆕 Deteksi Produk Baru"])

    with tab1:
        st.header("Ringkasan Umum")
        # ... (Kode Dashboard Utama Anda tetap di sini, tidak diubah) ...
        # (Kode dari file asli Anda untuk tab1 akan ditempatkan di sini)
        total_produk = len(df_all)
        total_toko = df_all['Toko'].nunique()
        rata_HARGA = df_all['HARGA'].mean()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Produk Terdata", f"{total_produk:,}")
        col2.metric("Jumlah Toko", total_toko)
        col3.metric("Rata-rata HARGA Produk", f"Rp {rata_HARGA:,.0f}")
        
        st.subheader("Distribusi Produk per Toko")
        produk_per_toko = df_all['Toko'].value_counts().reset_index()
        produk_per_toko.columns = ['Toko', 'Jumlah Produk']
        fig = px.bar(produk_per_toko, x='Toko', y='Jumlah Produk', title="Jumlah Produk per Toko", text_auto=True)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.header("⚖️ Perbandingan HARGA Produk dengan Kompetitor (Metode SBERT)")
        
        if df_hasil_sbert_cache.empty:
            st.warning("Data hasil perbandingan belum tersedia. Jalankan analisis SBERT terlebih dahulu.")
        else:
            df_dbklik_list = df_all[df_all['Toko'] == 'DB KLIK'].sort_values('Nama Produk')
            dbklik_product_list = df_dbklik_list['Nama Produk'].unique()
            
            selected_product = st.selectbox(
                "Pilih produk DB Klik untuk dibandingkan:",
                options=dbklik_product_list,
                index=None,
                placeholder="Ketik untuk mencari produk..."
            )

            if selected_product:
                product_info = df_dbklik_list[df_dbklik_list['Nama Produk'] == selected_product].iloc[0]
                HARGA_dbklik = product_info['HARGA']

                st.subheader(f"Hasil Perbandingan untuk: **{selected_product}**")
                col1, col2 = st.columns(2)
                col1.metric("Brand", product_info['Brand'])
                col2.metric("HARGA di DB Klik", f"Rp {HARGA_dbklik:,.0f}")
                
                results = df_hasil_sbert_cache[df_hasil_sbert_cache['Nama Produk DBKlik'] == selected_product].copy()
                
                if not results.empty:
                    results['HARGA Kompetitor'] = pd.to_numeric(results['HARGA Kompetitor'])
                    results['Selisih HARGA'] = results['HARGA Kompetitor'] - HARGA_dbklik
                    
                    def format_selisih(selisih):
                        if selisih > 0:
                            return f"Lebih Mahal Rp {selisih:,.0f}"
                        elif selisih < 0:
                            return f"Lebih Murah Rp {-selisih:,.0f}"
                        else:
                            return "Sama"
                    
                    results['Keterangan'] = results['Selisih HARGA'].apply(format_selisih)
                    results['HARGA Kompetitor'] = results['HARGA Kompetitor'].apply(lambda x: f"Rp {x:,.0f}")
                    results['Skor Kemiripan'] = (pd.to_numeric(results['Skor Kemiripan']) * 100).apply(lambda x: f"{x:.2f}%")

                    st.dataframe(results[[
                        'Toko Kompetitor', 
                        'Nama Produk Kompetitor', 
                        'HARGA Kompetitor', 
                        'Keterangan',
                        'Skor Kemiripan'
                    ]], use_container_width=True)
                else:
                    st.info("Tidak ditemukan produk yang sangat mirip di toko kompetitor berdasarkan analisis SBERT.")


    with tab3:
        # ... (Kode Analisis Tren Anda tetap di sini, tidak diubah) ...
        # (Kode dari file asli Anda untuk tab3 akan ditempatkan di sini)
        st.header("Analisis Tren Penjualan")
        brand_list = sorted(df_all['Brand'].unique())
        selected_brand_tren = st.selectbox("Pilih Brand untuk melihat tren:", brand_list)
        
        if selected_brand_tren:
            df_tren = df_all[df_all['Brand'] == selected_brand_tren]
            tren_penjualan = df_tren.groupby('Minggu')['Terjual/Bln'].sum().reset_index()
            fig_tren = px.line(tren_penjualan, x='Minggu', y='Terjual/Bln', title=f'Tren Penjualan Mingguan untuk Brand: {selected_brand_tren}', markers=True)
            st.plotly_chart(fig_tren, use_container_width=True)


    with tab4:
        # ... (Kode Deteksi Produk Baru Anda tetap di sini, tidak diubah) ...
        # (Kode dari file asli Anda untuk tab4 akan ditempatkan di sini)
        st.header("Deteksi Produk Baru per Minggu")
        df_filtered = df_all.copy()
        weeks = sorted(df_filtered['Minggu'].unique())
        
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
                        new_products_df['HARGA_fmt'] = new_products_df['HARGA'].apply(lambda x: f"Rp {x:,.0f}")
                        st.dataframe(new_products_df[['Nama Produk', 'HARGA_fmt', 'Brand']], use_container_width=True)

else:
    st.error("Tidak dapat memuat data. Periksa koneksi atau konfigurasi Google Sheets Anda.")


