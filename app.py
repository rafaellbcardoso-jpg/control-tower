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

# 🔽 FILTRANDO SOMENTE AS COLUNAS NECESSÁRIAS
colunas_desejadas = [
    "Placa",
    "Proprietário",
    "Data de Comunicação",
    "Latitude",
    "Longitude"
]

# Mantém só as colunas que existem no dataset (evita erro)
df = df[[col for col in colunas_desejadas if col in df.columns]]

st.title("🚛 Base Omni - Filtrada")

st.dataframe(df)
