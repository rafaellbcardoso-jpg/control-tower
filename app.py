import streamlit as st
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

# =========================
# CONFIG
# =========================
BUCKET_NAME = "control-tower-dados"

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["google"]
)

client = storage.Client(
    credentials=credentials,
    project="paine-stramlit"
)

bucket = client.bucket(BUCKET_NAME)

# =========================
# FUNÇÃO CORREÇÃO LAT/LONG
# =========================
def corrigir_coordenada(valor):
    try:
        valor = str(valor)
        if valor.count('.') > 1:
            partes = valor.split('.')
            valor = partes[0] + '.' + ''.join(partes[1:])
        return float(valor)
    except:
        return None

# =========================
# LER TODOS OS ARQUIVOS
# =========================
blobs = list(bucket.list_blobs(prefix="omnilink/"))

dfs = []

for blob in blobs:
    if blob.name.endswith(".csv"):
        content = blob.download_as_bytes()
        df_temp = pd.read_csv(BytesIO(content))
        dfs.append(df_temp)

if not dfs:
    st.error("Nenhum arquivo encontrado em omnilink/")
    st.stop()

df = pd.concat(dfs, ignore_index=True)

# =========================
# TRATAR COLUNAS
# =========================
df.columns = df.columns.str.strip()
# =========================
# 🕒 TRATAMENTO DATA (PADRÃO POWER BI)
# =========================
df["Posição"] = pd.to_datetime(
    df["Data de comunicação"]
    .astype(str)
    .str[4:24],
    errors="coerce"
)
# 👉 FORMATA PADRÃO BR
df["Posição"] = df["Posição"].dt.strftime("%d/%m/%Y %H:%M:%S")

# NÃO CONVERTER DATA (mantém original)
# df["Data de comunicação"] = pd.to_datetime(...)

df["Latitude"] = df["Latitude"].apply(corrigir_coordenada)
df["Longitude"] = df["Longitude"].apply(corrigir_coordenada)

# =========================
# GARANTIR DATA COMO DATETIME (pra ordenar certo)
# =========================
df["Data de comunicação"] = pd.to_datetime(df["Data de comunicação"], errors="coerce")

# =========================
# PEGAR ÚLTIMA POSIÇÃO POR PLACA
# =========================
df = df.sort_values("Data de comunicação", ascending=False)

df_final = df.drop_duplicates(subset=["Placa"], keep="first")[
    ["Placa","Posição", "Latitude", "Longitude"]
]

# =========================
# EXIBIR
# =========================
st.title("🚛 Base Omni")
# =========================
# FILTROS SIDEBAR
# =========================
st.sidebar.header("Filtros")

# Converter posição pra datetime (usar antes do filtro)
df_final["Posição_dt"] = pd.to_datetime(df_final["Posição"], errors="coerce")

# Datas padrão: ontem até hoje
hoje = pd.Timestamp.today().normalize()
ontem = hoje - pd.Timedelta(days=1)

data_inicio, data_fim = st.sidebar.date_input(
    "Período",
    [ontem, hoje]
)

# Filtro por tipo
df_final["Tipo"] = df_final["Proprietário"].apply(
    lambda x: "Frota" if str(x).strip().upper() == "LEMAR" else "Agregado"
)

tipo_filtro = st.sidebar.multiselect(
    "Tipo",
    ["Frota", "Agregado"],
    default=["Frota", "Agregado"]
)

# =========================
# APLICAR FILTROS
# =========================
df_filtrado = df_final[
    (df_final["Posição_dt"].dt.date >= data_inicio) &
    (df_final["Posição_dt"].dt.date <= data_fim) &
    (df_final["Tipo"].isin(tipo_filtro))
]

# =========================
# EXIBIR
# =========================
st.dataframe(df_filtrado.drop(columns=["Posição_dt"]))
st.dataframe(df_final)
