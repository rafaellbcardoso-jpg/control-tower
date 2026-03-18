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
# FUNÇÃO CORREÇÃO
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
# TRATAR DATA
# =========================
df["Data_Hora"] = (
    df["Data_Hora"]
    .astype(str)
    .str[4:24]
)

df["Data_Hora"] = pd.to_datetime(df["Data_Hora"], errors="coerce")

# =========================
# TRATAR LAT/LONG
# =========================
df["Latitude"] = df["Latitude"].apply(corrigir_coordenada)
df["Longitude"] = df["Longitude"].apply(corrigir_coordenada)

# =========================
# EXIBIR
# =========================
st.title("🚛 Base Omni")

st.dataframe(df)
