import streamlit as st
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

# =========================
# CONFIG
# =========================
BUCKET_NAME = "Torre - Zé da rpta - Lemar"

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

    # 🔥 TRATAMENTO DATA_HORA
    if "Data_Hora" in df.columns:
        df["Data_Hora"] = (
            df["Data_Hora"]
            .astype(str)
            .str[4:24]
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

# =========================
# 🔥 ÚLTIMA POSIÇÃO POR PLACA
# =========================
df = df.dropna(subset=["Latitude", "Longitude"])

df = df.sort_values("Data_Hora", ascending=False)

df_ult = df.drop_duplicates(subset=["Placa"])

# =========================
# 📊 INDICADORES
# =========================
col1, col2 = st.columns(2)

col1.metric("Veículos Ativos", len(df_ult))
col2.metric("Total de Registros", len(df))

# =========================
# 🗺️ MAPA
# =========================
st.subheader("📍 Posição Atual dos Veículos")

st.map(df_ult[["Latitude", "Longitude"]])

# =========================
# 🔍 TABELA FINAL
# =========================
st.subheader("📋 Última posição por placa")

st.dataframe(df_ult)
