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

# =========================
# FUNÇÃO ETL
# =========================
@st.cache_data
def carregar_etl():
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob("etl/tabela_painel.csv")
    content = blob.download_as_bytes()

    df = pd.read_csv(BytesIO(content))

    # 🔥 TRATAMENTO DATA_HORA (igual Power BI)
    if "Data_Hora" in df.columns:
        df["Data_Hora"] = (
            df["Data_Hora"]
            .astype(str)
            .str[4:24]  # equivalente ao Text.Middle
        )

        df["Data_Hora"] = pd.to_datetime(
            df["Data_Hora"],
            errors="coerce"
        )

    return df

# =========================
# APP
# =========================
st.set_page_config(layout="wide")

st.title("🚛 Control Tower - Operação")

df = carregar_etl()

st.success("Dados carregados com sucesso")

# 🔍 Visualização
st.dataframe(df)
