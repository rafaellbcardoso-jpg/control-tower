import streamlit as st
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

# CONFIG
BUCKET_NAME = "control-tower-dados"

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["google"]
)

client = storage.Client(
    credentials=credentials,
    project="paine-stramlit"
)

bucket = client.bucket(BUCKET_NAME)

# LISTAR ARQUIVOS
blobs = list(bucket.list_blobs(prefix="omnilink/"))

st.write("Arquivos encontrados:", [b.name for b in blobs])

dfs = []

# LER ARQUIVOS (FORTE PRA QUALQUER CSV)
for blob in blobs:
    if blob.name.endswith(".csv"):
        content = blob.download_as_bytes()
        try:
            df_temp = pd.read_csv(BytesIO(content), sep=";", encoding="latin1")
        except:
            df_temp = pd.read_csv(BytesIO(content), sep=None, engine="python")
        dfs.append(df_temp)

# VALIDAÇÃO
if not dfs:
    st.error("Nenhum CSV válido encontrado")
    st.stop()

df = pd.concat(dfs, ignore_index=True)

# DEBUG
st.write("Qtd linhas:", len(df))
st.write("Colunas:", df.columns)

# EXIBIR
st.title("🚛 Base Omni")
st.dataframe(df)
