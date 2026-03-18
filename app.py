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
# 🚛 TIPO DE FROTA
# =========================
df["Tipo_Frota"] = df["Proprietário"].apply(
    lambda x: "Frota" if str(x).strip().lower() == "lemar" else "Agregado"
)

# =========================
# 📅 FILTRO DE DATA
# =========================
from datetime import datetime, timedelta

# =========================
# 📅 FILTRO DE DATA (SIDEBAR)
# =========================
st.sidebar.subheader("📅 Período")

df["Data_Hora"] = pd.to_datetime(df["Data_Hora"], errors="coerce")

hoje = datetime.today().date()
ontem = hoje - timedelta(days=1)

data_inicio, data_fim = st.sidebar.date_input(
    "Selecione o período",
    [ontem, hoje]
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
# 🚛 ÚLTIMA POSIÇÃO POR PLACA (MOVIDO PRA CÁ)
# =========================
df = df.sort_values("Data_Hora")

df = df.drop_duplicates(
    subset="Placa",
    keep="last"
)

# =========================
# 🌎 TRATAMENTO LAT/LONG
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


df["Latitude_tratada"] = df["Latitude"].apply(corrigir_coordenada)
df["Longitude_tratada"] = df["Longitude"].apply(corrigir_coordenada)
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
# 📊 RESUMO ESTILO BI
# =========================
st.subheader("📋 Resumo da Operação")

# -------------------------
# 🔵 POSIÇÃO (OMNI)
# -------------------------
df_posicao = df.sort_values("Data_Hora").drop_duplicates(
    subset="Placa",
    keep="last"
)[["Placa", "Latitude_tratada", "Longitude_tratada"]]

# -------------------------
# 🟢 PROGRAMAÇÃO (ROBO)
# -------------------------
df_programacao = df.sort_values("Data").drop_duplicates(
    subset="Placa",
    keep="last"
)[[
    "Placa",
    "Motoristas",
    "PV",
    "Data",
    "Rotas",
    "Status",
    "ETA_2"
]]

# -------------------------
# 🔗 JOIN
# -------------------------
df_final = df_posicao.merge(df_programacao, on="Placa", how="left")

# -------------------------
# 🧾 AJUSTE FINAL
# -------------------------
df_final = df_final.rename(columns={
    "Placa": "Cavalo",
    "Motoristas": "Motorista",
    "PV": "Programação",
    "Data": "Ultima Data",
    "Rotas": "Rota Hoje",
    "Status": "Status",
    "ETA_2": "Carregamento"
})

st.dataframe(df_final)
# =========================
# 📊 TABELA RESUMIDA
# =========================
st.subheader("📋 Resumo da Operação")

df_ordenado = df.sort_values("Data_Hora")

df_resumo = df_ordenado.loc[
    df_ordenado.groupby("Placa")["Data_Hora"].idxmax()
][["Placa", "Motoristas", "Data_Hora", "Data"]]

df_resumo = df_resumo.rename(columns={
    "Motoristas": "Motorista",
    "Data_Hora": "Posicionamento"
})

st.dataframe(df_resumo)
