import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings

# Mengabaikan peringatan agar tampilan dashboard bersih
warnings.filterwarnings("ignore")

# 1. KONFIGURASI HALAMAN
st.set_page_config(page_title="Grand Mitra - Full Control Analytics", layout="wide")

# --- KONFIGURASI URL GITHUB (PASTIKAN MENGGUNAKAN LINK RAW) ---
URL_OMSET = "https://raw.githubusercontent.com/grandmitra/gmbdashboard/main/omset.parquet"
URL_STOK = "https://raw.githubusercontent.com/grandmitra/gmbdashboard/main/stok.parquet"

# 2. FUNGSI LOAD DATA (OPTIMASI PARQUET & ROBUST CLEANING)
@st.cache_data(ttl=3600)
def load_and_prepare_data():
    try:
        # Membaca file Parquet
        df_sales = pd.read_parquet(URL_OMSET)
        df_stock = pd.read_parquet(URL_STOK)
        
        # --- PEMBERSIHAN NAMA KOLOM ---
        # Menghapus spasi di awal/akhir dan mengubah semua jadi HURUF BESAR agar konsisten
        df_sales.columns = [c.strip().upper() for c in df_sales.columns]
        df_stock.columns = [c.strip().upper() for c in df_stock.columns]
        
    except Exception as e:
        st.error(f"Gagal mengambil data dari GitHub: {e}")
        return pd.DataFrame(), pd.DataFrame()

    if df_sales.empty: 
        return pd.DataFrame(), pd.DataFrame()

    # Pastikan ITEM_NO tetap string dan bersih dari angka desimal (.0)
    for df in [df_sales, df_stock]:
        if 'ITEM_NO' in df.columns:
            df['ITEM_NO'] = df['ITEM_NO'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 1. Mapping Hierarki (GROUP_NAME1-4 ke DEPT-BRAND)
    map_h = {'GROUP_NAME1': 'DEPT', 'GROUP_NAME2': 'DIV', 'GROUP_NAME3': 'KAT', 'GROUP_NAME4': 'BRAND'}
    for old, new in map_h.items():
        if old in df_stock.columns:
            df_stock[new] = df_stock[old].fillna('UNKNOWN').astype(str).str.strip()
        else:
            df_stock[new] = 'UNKNOWN'

    # 2. Sinkronisasi Balance & Aging
    if 'BALANCE_QTY' in df_stock.columns:
        df_stock['BALANCE_QTY'] = pd.to_numeric(df_stock['BALANCE_QTY'], errors='coerce').fillna(0)
    else:
        df_stock['BALANCE_QTY'] = 0

    if 'AGING' in df_stock.columns:
        df_stock['AGING'] = pd.to_numeric(df_stock['AGING'], errors='coerce').fillna(0).astype(int)
    else:
        df_stock['AGING'] = 0

    # 3. Merge Hierarki dari Stok ke Sales
    stok_ref = df_stock[['ITEM_NO', 'DEPT', 'DIV', 'KAT', 'BRAND']].drop_duplicates('ITEM_NO')
    for c in ['DEPT', 'DIV', 'KAT', 'BRAND']:
        if c in df_sales.columns: 
            df_sales = df_sales.drop(columns=[c])
    
    df_sales = pd.merge(df_sales, stok_ref, on='ITEM_NO', how='left')

    # 4. Processing Waktu & Margin
    df_sales['FORM_DATE'] = pd.to_datetime(df_sales['FORM_DATE'], errors='coerce')
    df_sales = df_sales.dropna(subset=['FORM_DATE'])
    df_sales['TAHUN'] = df_sales['FORM_DATE'].dt.year.astype(str)
    df_sales['BULAN_TAHUN'] = df_sales['FORM_DATE'].dt.to_period('M').astype(str)

    for c in ['NET_AMOUNT', 'HPP1', 'QTY']:
        if c in df_sales.columns:
            df_sales[c] = pd.to_numeric(df_sales[c], errors='coerce').fillna(0)
    
    df_sales['MARGIN_VALUE'] = df_sales['NET_AMOUNT'] - df_sales['HPP1']
    df_sales['GM_PCT'] = (df_sales['MARGIN_VALUE'] / df_sales['NET_AMOUNT'] * 100).fillna(0).replace([float('inf'), -float('inf')], 0)

    return df_sales, df_stock

# 3. LOGIKA DASHBOARD
try:
    df_s, df_stk = load_and_prepare_data()

    if not df_s.empty:
        # --- SIDEBAR: SEMUA FILTER ---
        if st.sidebar.button("🔄 Reset Semua Filter"):
            st.rerun()

        st.sidebar.divider()
        st.sidebar.header("🔎 Pencarian Cepat")
        search_q = st.sidebar.text_input("Cari Nama Barang:", "").strip().upper()

        st.sidebar.header("🧾 Filter Transaksi")
        list_tahun = sorted(df_s['TAHUN'].unique(), reverse=True)
        sel_tahun = st.sidebar.multiselect("Tahun:", list_tahun, default=list_tahun[:1])

        # Filter Hierarki Dependent
        st.sidebar.header("🏗️ Hierarki Produk")
        list_dept = sorted([x for x in df_stk['DEPT'].unique() if x != 'UNKNOWN'])
        sel_dept = st.sidebar.multiselect("1. DEPT:", list_dept)

        stk_div = df_stk[df_stk['DEPT'].isin(sel_dept)] if sel_dept else df_stk
        sel_div = st.sidebar.multiselect("2. DIV:", sorted([x for x in stk_div['DIV'].unique() if x != 'UNKNOWN']))

        stk_kat = stk_div[stk_div['DIV'].isin(sel_div)] if sel_div else stk_div
        sel_kat = st.sidebar.multiselect("3. KAT:", sorted([x for x in stk_kat['KAT'].unique() if x != 'UNKNOWN']))

        # --- APPLY FILTERS ---
        df_f = df_s.copy()
        stk_f = df_stk.copy()

        if search_q: 
            df_f = df_f[df_f['ITEM_NAME'].str.contains(search_q, na=False, case=False)]
        if sel_tahun: 
            df_f = df_f[df_f['TAHUN'].isin(sel_tahun)]
        if sel_dept: 
            df_f, stk_f = df_f[df_f['DEPT'].isin(sel_dept)], stk_f[stk_f['DEPT'].isin(sel_dept)]
        if sel_div: 
            df_f, stk_f = df_f[df_f['DIV'].isin(sel_div)], stk_f[stk_f['DIV'].isin(sel_div)]
        if sel_kat: 
            df_f, stk_f = df_f[df_f['KAT'].isin(sel_kat)], stk_f[stk_f['KAT'].isin(sel_kat)]

        # --- DASHBOARD UI ---
        st.title("📊 Grand Mitra - Sales & Inventory Analytics")
        st.caption(f"Update Terakhir: {pd.Timestamp.now().strftime('%d %B %Y %H:%M')}")
        
        sku_ready = int(stk_f[stk_f['BALANCE_QTY'] > 0]['ITEM_NO'].nunique())

        # Metric Cards (Responsive Layout)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("SKU Ready", f"{sku_ready:,.0f}")
        c2.metric("SKU Terjual", f"{df_f['ITEM_NO'].nunique():,.0f}")
        c3.metric("Total Margin", f"Rp {df_f['MARGIN_VALUE'].sum():,.0f}")
        c4.metric("Avg GM %", f"{df_f['GM_PCT'].mean():.2f}%")

        # --- GRAPH: TREN OMSET & MARGIN ---
        st.subheader("📈 Tren Performa Bulanan")
        timeline = df_f.groupby('BULAN_TAHUN').agg({'NET_AMOUNT': 'sum', 'ITEM_NO': 'nunique', 'GM_PCT': 'mean'}).reset_index()
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=timeline['BULAN_TAHUN'].astype(str), y=timeline['NET_AMOUNT']/1_000_000, name="Omset (Juta)", marker_color='#2c3e50', opacity=0.6), secondary_y=False)
        fig.add_trace(go.Scatter(x=timeline['BULAN_TAHUN'].astype(str), y=timeline['GM_PCT'], name="GM %", mode='lines+markers', line=dict(color='#27ae60', dash='dot')), secondary_y=True)
        
        fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

        # --- PARETO TABLE ---
        st.subheader("🎯 Analisis Pareto & Profitability")
        pareto = df_f.groupby(["ITEM_NO", "ITEM_NAME"]).agg({
            'QTY': 'sum', 'NET_AMOUNT': 'sum', 'MARGIN_VALUE': 'sum', 'GM_PCT': 'mean'
        }).sort_values("NET_AMOUNT", ascending=False).reset_index()

        stk_info = stk_f[['ITEM_NO', 'BALANCE_QTY', 'AGING']].drop_duplicates('ITEM_NO')
        pareto = pd.merge(pareto, stk_info, on='ITEM_NO', how='left')

        total_s = pareto['NET_AMOUNT'].sum()
        if total_s > 0:
            pareto['KUMULATIF_%'] = (pareto['NET_AMOUNT'] / total_s).cumsum() * 100
            pareto['KELAS'] = pareto['KUMULATIF_%'].apply(lambda x: "⭐ A" if x <= 80 else "📦 B")
        
        # Formatting untuk tampilan tabel
        p_disp = pareto.copy()
        for c in ['NET_AMOUNT', 'MARGIN_VALUE']: 
            p_disp[c] = p_disp[c].map('{:,.0f}'.format)
        p_disp['GM_PCT'] = p_disp['GM_PCT'].map('{:.2f}%'.format)
        
        st.dataframe(p_disp[['KELAS', 'ITEM_NO', 'ITEM_NAME', 'QTY', 'BALANCE_QTY', 'NET_AMOUNT', 'MARGIN_VALUE', 'GM_PCT', 'AGING']], 
                     use_container_width=True, hide_index=True)

    else:
        st.warning("Data kosong atau filter tidak sesuai.")

except Exception as e:
    st.error(f"Sistem Error: {e}")
