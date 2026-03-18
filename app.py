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

# 🧠 CRIAR POSIÇÃO (datetime real)
df["Posição"] = pd.to_datetime(
    df["Data de comunicação"].astype(str).str[4:24],
    errors="coerce"
)

# 🧠 Tipo
df["Tipo"] = df["Proprietário"].apply(
    lambda x: "Frota" if str(x).strip().upper() == "LEMAR" else "Agregado"
)

# 🔥 PEGAR ÚLTIMA POSIÇÃO POR PLACA
df = df.sort_values(by="Posição", ascending=False)
df = df.drop_duplicates(subset="Placa", keep="first")

# 🇧🇷 FORMATAÇÃO BR (APENAS VISUAL)
df["Posição"] = df["Posição"].dt.strftime("%d/%m/%Y %H:%M:%S")

# 🔽 COLUNAS
colunas_finais = [
    "Placa",
    "Tipo",
    "Posição",
    "Latitude",
    "Longitude"
]

df = df[[col for col in colunas_finais if col in df.columns]]

# 🎛️ FILTRO
st.sidebar.title("Filtros")

tipo_selecionado = st.sidebar.multiselect(
    "Tipo",
    options=df["Tipo"].unique(),
    default=df["Tipo"].unique()
)

df_filtrado = df[df["Tipo"].isin(tipo_selecionado)]

# 📊 EXIBIÇÃO
st.title("🚛 Base Omni - Última Posição por Placa")

st.dataframe(df_filtrado)
