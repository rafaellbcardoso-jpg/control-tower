import streamlit as st
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

BUCKET_NAME = "control-tower-dados"

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["google"]
)

client = storage.Client(
    credentials=credentials,
    project="paine-stramlit"
)

bucket = client.bucket(BUCKET_NAME)

blobs = list(bucket.list_blobs(prefix="omnilink/"))

dfs = []

for blob in blobs:
    if blob.name.endswith(".csv"):
        content = blob.download_as_bytes()
        df_temp = pd.read_csv(BytesIO(content))
        dfs.append(df_temp)

if not dfs:
    st.error("Nenhum arquivo encontrado")
    st.stop()

df = pd.concat(dfs, ignore_index=True)

# 🧠 CRIAR COLUNA POSIÇÃO (MESMA LÓGICA DO POWER BI)
df["Posição"] = pd.to_datetime(
    df["Data de comunicação"].astype(str).str[4:24],
    errors="coerce"
)

# 🧠 Coluna derivada Tipo
df["Tipo"] = df["Proprietário"].apply(
    lambda x: "Frota" if str(x).strip().upper() == "LEMAR" else "Agregado"
)

# 🔽 Seleção de colunas
colunas_finais = [
    "Placa",
    "Tipo",
    "Posição",
    "Data de comunicação",
    "Latitude",
    "Longitude"
]

df = df[[col for col in colunas_finais if col in df.columns]]

# 🎛️ FILTRO NA SIDEBAR
st.sidebar.title("Filtros")

tipo_selecionado = st.sidebar.multiselect(
    "Tipo",
    options=df["Tipo"].unique(),
    default=df["Tipo"].unique()
)

# Aplicando filtro
df_filtrado = df[df["Tipo"].isin(tipo_selecionado)]

# 📊 Exibição
st.title("🚛 Base Omni - Operacional")

st.dataframe(df_filtrado)
