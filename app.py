import streamlit as st
import pandas as pd
from google.cloud import storage
from io import BytesIO

# =========================
# CONFIG
# =========================
BUCKET_NAME = "control-tower-dados"

@st.cache_data
def carregar_etl():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob("etl/tabela_painel.csv")
    content = blob.download_as_bytes()

    df = pd.read_csv(BytesIO(content))
    return df

# =========================
# APP
# =========================
st.set_page_config(layout="wide")

st.title("🚛 Control Tower - Operação")

df = carregar_etl()

st.success("Dados carregados com sucesso")

st.dataframe(df)