import streamlit as st
st.set_page_config(layout="wide")

import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

# =========================
# 🧠 NOW GLOBAL (PADRÃO)
# =========================
from datetime import datetime

agora = datetime.now()
hoje = agora.date()
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
# 🔽 BASE OMNILINK
# =========================
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

# ========================= 
# 🧠 POSIÇÃO 
# ========================= 
df["Posição"] = pd.to_datetime(
    df["Data de comunicação"].astype(str).str[4:24],
    errors="coerce" )

# =========================
# 🧠 TIPO
# =========================
df["Tipo"] = df["Proprietário"].apply(
    lambda x: "Frota" if str(x).strip().upper() == "LEMAR" else "Agregado"
)

# =========================
# 🔧 CORREÇÃO LAT/LONG
# =========================
def corrigir_coord(valor):
    if pd.isna(valor):
        return None
    
    valor = str(valor)
    
    if valor.count(".") > 1:
        partes = valor.split(".")
        valor = partes[0] + "." + "".join(partes[1:])
    
    try:
        return float(valor)
    except:
        return None

df["Latitude_corrigida"] = df["Latitude"].apply(corrigir_coord)
df["Longitude_corrigida"] = df["Longitude"].apply(corrigir_coord)

# =========================
# 🔥 ÚLTIMA POSIÇÃO
# =========================
df = df.sort_values(by="Posição", ascending=False)
df = df.drop_duplicates(subset="Placa", keep="first")

# =========================
# 🌍 BASE DE CIDADES
# =========================
blob_cidades = bucket.blob("cidades/Cidades_Bucket.xlsx")
conteudo = blob_cidades.download_as_bytes()

df_cidades = pd.read_excel(BytesIO(conteudo))

df_cidades.columns = df_cidades.columns.str.strip()

df_cidades = df_cidades.rename(columns={
    "LATITU": "Lat",
    "LONGIT": "Lon",
    "Cidade - UF": "Localização Atual"
})

# 🔥 CORREÇÃO VÍRGULA → PONTO
df_cidades["Lat"] = df_cidades["Lat"].astype(str).str.replace(",", ".")
df_cidades["Lon"] = df_cidades["Lon"].astype(str).str.replace(",", ".")

# 🔥 CONVERSÃO PRA FLOAT
df_cidades["Lat"] = pd.to_numeric(df_cidades["Lat"], errors="coerce")
df_cidades["Lon"] = pd.to_numeric(df_cidades["Lon"], errors="coerce")

df_cidades = df_cidades.dropna(subset=["Lat", "Lon"])

# =========================
# 🧠 KNN
# =========================
coords_cidades = np.radians(df_cidades[["Lat", "Lon"]].values)
tree = BallTree(coords_cidades, metric="haversine")

df_validos = df.dropna(subset=["Latitude_corrigida", "Longitude_corrigida"]).copy()

coords_veiculos = np.radians(
    df_validos[["Latitude_corrigida", "Longitude_corrigida"]].values
)

dist, ind = tree.query(coords_veiculos, k=1)

df_validos["Localização Atual"] = df_cidades.iloc[ind.flatten()]["Localização Atual"].values

# =========================
# 🔗 MERGE
# =========================
df = df.merge(
    df_validos[["Placa", "Localização Atual"]],
    on="Placa",
    how="left"
)

# =========================
# 🔽 BASE PV (BUCKET)
# =========================
blobs_pv = list(bucket.list_blobs(prefix="robo/"))

dfs_pv = []

for blob in blobs_pv:
    if blob.name.endswith(".xlsx"):
        content = blob.download_as_bytes()
        df_temp = pd.read_excel(BytesIO(content))
        dfs_pv.append(df_temp)

# 🔒 garante que df_pv sempre existe
df_pv = pd.DataFrame()

if dfs_pv:
    df_pv = pd.concat(dfs_pv, ignore_index=True)

# =========================
# 🔧 NORMALIZAR PV
# =========================
if not df_pv.empty:
    df_pv["Placas_clean"] = (
        df_pv["Placas"]
        .astype(str)
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
    )

# =========================
# 🔧 NORMALIZAR OMNI
# =========================
df["Placa_clean"] = (
    df["Placa"]
    .astype(str)
    .str.upper()
    .str.replace(r"[^A-Z0-9]", "", regex=True)
)

# =========================
# 🔥 CONTAGEM DE MATCH
# =========================
contagens = []

for _, row in df.iterrows():
    placa = row["Placa_clean"]

    if not df_pv.empty:
        qtd = df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True).sum()
    else:
        qtd = 0

    contagens.append(qtd)

df["Qtd PV"] = contagens
# =========================
# 🔥 ÚLTIMA DATA PV
# =========================
if not df_pv.empty:
    df_pv["Data"] = pd.to_datetime(df_pv["Data"], errors="coerce", dayfirst=True)

datas = []

for _, row in df.iterrows():
    placa = row["Placa_clean"]

    if not df_pv.empty:
        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]
        
        data = df_match["Data"].max() if not df_match.empty else None

# 👇 formata para DD/MM/AAAA
datas = []

for _, row in df.iterrows():
    placa = row["Placa_clean"]

    if not df_pv.empty:
        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]
        
        data = df_match["Data"].max() if not df_match.empty else None
    else:
        data = None

    # 👇 AQUI DENTRO DO LOOP
    data = data.strftime("%d/%m/%Y") if pd.notna(data) else None

    datas.append(data)

df["Ultima Data PV"] = datas
# =========================
# 🔥 CONTAGEM DE MATCH
# =========================
contagens = []

for _, row in df.iterrows():
    placa = row["Placa_clean"]

    if not df_pv.empty:
        qtd = df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True).sum()
    else:
        qtd = 0

    contagens.append(qtd)

df["Qtd PV"] = contagens


# =========================
# 🔥 ÚLTIMA DATA PV
# =========================
if not df_pv.empty:
    df_pv["Data"] = pd.to_datetime(df_pv["Data"], errors="coerce")

datas = []

for _, row in df.iterrows():
    placa = row["Placa_clean"]

    if not df_pv.empty:
        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]
        
        if not df_match.empty:
            data = df_match["Data"].dropna().max()
        else:
            data = None
    else:
        data = None

    datas.append(data)

df["Programação"] = datas

from datetime import datetime

hoje = datetime.now().date()

df["Programação"] = df["Programação"].apply(
    lambda x: "Hoje" if pd.notnull(x) and x.date() == hoje else (
        x.strftime("%Y-%m-%d") if pd.notnull(x) else None
    )
)
# =========================
# 🧠 OPERAÇÃO (ÚLTIMA)
# =========================

operacoes = []

for _, row in df.iterrows():

    placa = row["Placa_clean"]

    if not df_pv.empty:

        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]

        if not df_match.empty:

            # pega a última data
            df_match = df_match.copy()
            df_match["Data"] = pd.to_datetime(df_match["Data"], errors="coerce")

            ultima_data = df_match["Data"].dropna().max()

            # filtra a linha da última data
            linha = df_match[df_match["Data"] == ultima_data].iloc[0]

            operacao = linha.get("Operação", None)

        else:
            operacao = None

    else:
        operacao = None

    operacoes.append(operacao)

df["Operação"] = operacoes
# =========================
# 🚀 ROTA (SE FOR HOJE)
# =========================

rotas = []

for _, row in df.iterrows():

    if row["Programação"] == "Hoje":

        placa = row["Placa_clean"]

        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]

        if not df_match.empty:

            linha = df_match.sort_values("Data", ascending=False).iloc[0]

            origem = linha.get("Origem", "")
            uf_origem = linha.get("Orig. UF", "")
            destino = linha.get("Destino", "")
            uf_destino = linha.get("Dest. UF", "")

            rota = f"{origem} - {uf_origem} x {destino} - {uf_destino}"

        else:
            rota = None

    else:
        rota = None

    rotas.append(rota)

df["Rota"] = rotas
# =========================
# 🧠 ANDAMENTO (ETA)
# =========================

andamentos = []

for _, row in df.iterrows():

    if row["Programação"] == "Hoje":

        placa = row["Placa_clean"]

        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]

        if not df_match.empty:

            linha = df_match.sort_values("Data", ascending=False).iloc[0]

            eta_str = linha.get("ETA", None)

            if pd.notnull(eta_str) and eta_str != "":

                try:
                    # transforma ETA (HH:MM) em datetime hoje
                    eta = datetime.strptime(eta_str, "%H:%M")
                    eta = eta.replace(
                        year=agora.year,
                        month=agora.month,
                        day=agora.day
                    )

                    diferenca = eta - agora

                    if agora > eta:
                        andamento = "Em andamento"

                    else:
                        horas = diferenca.total_seconds() / 3600

                        if horas > 4:
                            andamento = f"🟢 {eta_str}"
                        else:
                            andamento = f"🔴 {eta_str}"

                except:
                    andamento = None
            else:
                andamento = None

        else:
            andamento = None

    else:
        andamento = None

    andamentos.append(andamento)

df["Andamento"] = andamentos
# =========================
# 🔥 CONTAGEM PV COM DATA
# =========================
qtd_datas = []

for _, row in df.iterrows():
    placa = row["Placa_clean"]

    if not df_pv.empty:
        df_match = df_pv[
            (df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)) &
            (df_pv["Data"].notna())
        ]
        
        qtd = len(df_match)
    else:
        qtd = 0

    qtd_datas.append(qtd)

df["Qtd PV Data"] = qtd_datas

# =========================
# 🔥 MOTORISTA ÚLTIMA DATA PV
# =========================
motoristas = []

for _, row in df.iterrows():
    placa = row["Placa_clean"]

    if not df_pv.empty:
        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]
        
        if not df_match.empty:
            df_match = df_match.sort_values(by="Data", ascending=False)
            motorista = df_match.iloc[0]["Motoristas"]
        else:
            motorista = None
    else:
        motorista = None

    motoristas.append(motorista)

df["Motorista"] = motoristas
# =========================
# 🔽 COLUNAS
# =========================
df = df[[
    "Tipo",
    "Placa",
    "Posição",
    "Localização Atual",
    "Programação",
    "Rota",
    "Andamento",
    "Motorista"
]]
# =========================
# 🎯 FILTROS
# =========================
st.sidebar.title("Filtros")

# 🔹 GARANTE COLUNA
if "Tipo" not in df.columns:
    df["Tipo"] = None

# 🔹 FILTRO TIPO
tipo_selecionado = st.sidebar.multiselect(
    "Tipo",
    options=df["Tipo"].dropna().unique(),
    default=df["Tipo"].dropna().unique()
)

df_filtrado = df[df["Tipo"].isin(tipo_selecionado)]
# =========================
# 📊 TABELA
# =========================
st.dataframe(df_filtrado, use_container_width=True)
st.title("🚛 Base Omni - Última Posição por Placa")

st.dataframe(df_filtrado)

# =========================
# 🗺️ MAPA
# =========================
st.subheader("📍 Mapa")

df_mapa = df_validos.rename(columns={
    "Latitude_corrigida": "lat",
    "Longitude_corrigida": "lon"
})

st.map(df_mapa)
