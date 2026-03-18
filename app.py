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
# FUNÇÕES DE CARGA
# =========================
@st.cache_data
def carregar_omni():
    client = storage.Client(credentials=credentials, project="paine-stramlit")
    bucket = client.bucket(BUCKET_NAME)

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

    if "Data_Hora" in df.columns:
        df["Data_Hora"] = (
            df["Data_Hora"]
            .astype(str)
            .str[4:24]
        )
        df["Data_Hora"] = pd.to_datetime(df["Data_Hora"], errors="coerce")

    return df


@st.cache_data
def carregar_robo():
    client = storage.Client(credentials=credentials, project="paine-stramlit")
    bucket = client.bucket(BUCKET_NAME)

    blobs = list(bucket.list_blobs(prefix="robo/"))

    dfs = []
    for blob in blobs:
        if blob.name.endswith(".csv"):
            content = blob.download_as_bytes()
            df_temp = pd.read_csv(BytesIO(content))
            dfs.append(df_temp)

    if not dfs:
        st.error("Nenhum arquivo encontrado em robo/")
        st.stop()

    df = pd.concat(dfs, ignore_index=True)

    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    return df


# =========================
# APP
# =========================
st.set_page_config(layout="wide")

st.title("🚛 Control Tower - Operação")

df_omni = carregar_omni()
df_robo = carregar_robo()

st.success("Bases carregadas com sucesso")

# =========================
# 🚛 TIPO DE FROTA (mantido do robo)
# =========================
df_robo["Tipo_Frota"] = df_robo["Proprietário"].apply(
    lambda x: "Frota" if str(x).strip().lower() == "lemar" else "Agregado"
)

# =========================
# 📅 FILTRO DE DATA
# =========================
from datetime import datetime, timedelta

st.sidebar.subheader("📅 Período")

hoje = datetime.today().date()
ontem = hoje - timedelta(days=1)

data_inicio, data_fim = st.sidebar.date_input(
    "Selecione o período",
    [ontem, hoje]
)

df_omni = df_omni[
    (df_omni["Data_Hora"].dt.date >= data_inicio) &
    (df_omni["Data_Hora"].dt.date <= data_fim)
]

df_robo = df_robo[
    (df_robo["Data"].dt.date >= data_inicio) &
    (df_robo["Data"].dt.date <= data_fim)
]

# =========================
# 🚛 FILTRO FROTA
# =========================
st.sidebar.subheader("🚛 Tipo de Frota")

tipo_frota = st.sidebar.multiselect(
    "Selecione",
    options=df_robo["Tipo_Frota"].unique(),
    default=df_robo["Tipo_Frota"].unique()
)

df_robo = df_robo[df_robo["Tipo_Frota"].isin(tipo_frota)]

# =========================
# 🌎 TRATAMENTO LAT/LONG (SÓ OMNI)
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

df_omni["Latitude_tratada"] = df_omni["Latitude"].apply(corrigir_coordenada)
df_omni["Longitude_tratada"] = df_omni["Longitude"].apply(corrigir_coordenada)

# =========================
# 🔵 POSIÇÃO (OMNI)
# =========================
df_posicao = df_omni.sort_values("Data_Hora").drop_duplicates(
    subset="Placa",
    keep="last"
)

# =========================
# 🟢 PROGRAMAÇÃO (ROBO)
# =========================
df_programacao = df_robo.sort_values("Data").drop_duplicates(
    subset="Placa",
    keep="last"
)

# =========================
# 🔗 JOIN
# =========================
df_final = df_posicao.merge(df_programacao, on="Placa", how="left")

# =========================
# VALIDAÇÃO
# =========================
if df_final.empty:
    st.warning("Nenhum dado encontrado")
    st.stop()

# =========================
# 📊 INDICADORES
# =========================
col1, col2 = st.columns(2)

col1.metric("Total de Veículos", len(df_final))
col2.metric("Placas únicas", df_final["Placa"].nunique())

# =========================
# 📊 TABELA FINAL
# =========================
st.subheader("📋 Resumo da Operação")

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
# 🗺️ MAPA (mantido)
# =========================
st.subheader("🗺️ Mapa dos Veículos")

df_mapa = df_final.dropna(subset=["Latitude_tratada", "Longitude_tratada"])

st.map(df_mapa.rename(columns={
    "Latitude_tratada": "lat",
    "Longitude_tratada": "lon"
}))
