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
    client = storage.Client(
        credentials=credentials,
        project="paine-stramlit"
    )

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob("etl/tabela_painel.csv")

    content = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(content), encoding="latin1")

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
# 🚛 TIPO DE FROTA
# =========================
df["Tipo_Frota"] = df["Proprietario"].apply(
    lambda x: "Frota" if str(x).strip().lower() == "lemar" else "Agregado"
)

# =========================
# 📅 FILTRO DE DATA
# =========================
st.subheader("📅 Filtro de Data")

df["Data_Hora"] = pd.to_datetime(df["Data_Hora"], errors="coerce")

data_min = df["Data_Hora"].min()
data_max = df["Data_Hora"].max()

data_inicio, data_fim = st.date_input(
    "Selecione o período",
    [data_min, data_max]
)

df = df[
    (df["Data_Hora"].dt.date >= data_inicio) &
    (df["Data_Hora"].dt.date <= data_fim)
]

# =========================
# 🚛 FILTRO LATERAL
# =========================
st.sidebar.subheader("🚛 Tipo de Frota")

tipo_frota = st.sidebar.multiselect(
    "Selecione",
    options=df["Tipo_Frota"].unique(),
    default=df["Tipo_Frota"].unique()
)

df = df[df["Tipo_Frota"].isin(tipo_frota)]

# =========================
# VALIDAÇÃO
# =========================
if df.empty:
    st.warning("Nenhum dado encontrado com os filtros aplicados")
    st.stop()

# =========================
# 📊 INDICADORES
# =========================
col1, col2 = st.columns(2)

col1.metric("Total de Registros", len(df))
col2.metric("Placas únicas", df["Placa"].nunique())

# =========================
# 📋 TABELA
# =========================
st.subheader("📋 Dados da Operação")

st.dataframe(df)
