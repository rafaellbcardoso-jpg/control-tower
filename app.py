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
# 🔽 BASE MOTORISTAS (BUCKET)
# =========================
blobs_moto = list(bucket.list_blobs(prefix="motoristas/"))

dfs_moto = []

for blob in blobs_moto:
    if blob.name.endswith(".xlsx"):
        content = blob.download_as_bytes()
        df_temp = pd.read_excel(BytesIO(content))
        dfs_moto.append(df_temp)

df_moto = pd.DataFrame()

if dfs_moto:
    df_moto = pd.concat(dfs_moto, ignore_index=True)

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

# 🔧 garante que Data está correta
df_pv["Data"] = pd.to_datetime(df_pv["Data"], errors="coerce", dayfirst=True)

operacoes = []

for _, row in df.iterrows():

    placa = row["Placa_clean"]

    if not df_pv.empty:

        df_match = df_pv[
            df_pv["Placas_clean"].str.contains(rf"{placa}(?![A-Z0-9])", na=False, regex=True)
        ]

        if not df_match.empty:

            linha = df_match.sort_values("Data", ascending=False).iloc[0]

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
# 🧠 ANDAMENTO (ROBUSTO)
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
            eta2_str = linha.get("ETA_2", None)
            data_destino = linha.get("DT_Destino", None)

            if pd.notnull(eta_str) and pd.notnull(eta2_str) and pd.notnull(data_destino):

                try:
                    # INÍCIO (ETA)
                    eta_inicio = datetime.strptime(eta_str, "%H:%M").replace(
                        year=agora.year,
                        month=agora.month,
                        day=agora.day
                    )

                    # FIM (DT_Destino + ETA_2)
                    data_destino = pd.to_datetime(data_destino, errors="coerce")

                    eta_fim = datetime.strptime(eta2_str, "%H:%M")
                    eta_fim = eta_fim.replace(
                        year=data_destino.year,
                        month=data_destino.month,
                        day=data_destino.day
                    )

                    # 🔥 LÓGICA
                    if agora > eta_fim:
                        andamento = "Finalizado"

                    elif eta_inicio <= agora <= eta_fim:
                        andamento = "Em andamento"

                    else:
                        diferenca = eta_inicio - agora
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
# 🧠 DISPONIBILIDADE MOTORISTAS (BASE NOVA)
# =========================

df_pv["DT_Destino"] = pd.to_datetime(df_pv["DT_Destino"], errors="coerce", dayfirst=True)

motoristas_lista = df_moto["Motoristas"].dropna().unique()

registros = []

for motorista in motoristas_lista:

    df_match = df_pv[df_pv["Motoristas"] == motorista]

    if not df_match.empty:

        # pega última linha pela data
        linha = df_match.sort_values("DT_Destino", ascending=False).iloc[0]

        data_destino = linha.get("DT_Destino", None)
        eta2_str = linha.get("ETA_2", None)

        if pd.notnull(data_destino) and pd.notnull(eta2_str):

            try:
                # monta data + hora final
                hora = datetime.strptime(eta2_str, "%H:%M")

                fim_viagem = hora.replace(
                    year=data_destino.year,
                    month=data_destino.month,
                    day=data_destino.day
                )

                horas = (agora - fim_viagem).total_seconds() / 3600

            except:
                horas = None
        else:
            horas = None
    else:
        horas = None

    # 🔥 STATUS BASE MOTO
    status_base = df_moto.loc[
        df_moto["Motoristas"] == motorista, "Status Motorista"
    ]
    status_base = status_base.iloc[0] if not status_base.empty else None

    # 🔥 CLASSIFICAÇÃO
    if horas is None:
        status_final = None

    elif horas < 0:
        status_final = "🟡 Disponível em breve"

    elif horas <= 12:
        status_final = "🔴 Em descanso"

    else:
        status_final = "🟢 Disponível"

    registros.append({
        "Motoristas": motorista,
        "Horas sem viagem": horas,
        "Status": status_base,
        "Disponibilidade": status_final
    })

df_disp = pd.DataFrame(registros)

df_disp["Horas sem viagem"] = df_disp["Horas sem viagem"].round(1)

df_disp = df_disp.sort_values("Horas sem viagem", ascending=False)

# =========================
# 🔽 COLUNAS
# =========================
df = df[[
    "Tipo",
    "Operação",
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

# 🔹 GARANTE COLUNAS
if "Tipo" not in df.columns:
    df["Tipo"] = None

if "Operação" not in df.columns:
    df["Operação"] = None

# 🔹 FILTRO TIPO
tipo_selecionado = st.sidebar.multiselect(
    "Tipo",
    options=df["Tipo"].dropna().unique(),
    default=df["Tipo"].dropna().unique()
)

# 🔹 FILTRO OPERAÇÃO
operacao_selecionada = st.sidebar.multiselect(
    "Operação",
    options=df["Operação"].dropna().unique(),
    default=df["Operação"].dropna().unique()
)

# =========================
# 🔍 APLICAR FILTROS
# =========================
df_filtrado = df[
    df["Tipo"].isin(tipo_selecionado) &
    df["Operação"].isin(operacao_selecionada)
]
# =========================
# 🚛 BASE FROTA (BUCKET)
# =========================
blob_frota = bucket.blob("frota/Frota-Att.xlsx")
conteudo_frota = blob_frota.download_as_bytes()

df_frota = pd.read_excel(BytesIO(conteudo_frota))

# mantém apenas a coluna de placa
df_frota = df_frota[["PLACA"]]

st.subheader("🚛 Placas Frota")

st.dataframe(df_frota, use_container_width=True)
# =========================
# 🔢 TOTAL FROTA HOJE
# =========================
df_total = df[
    (df["Tipo"] == "Frota") &
    (df["Posição"].dt.date == hoje)
]

total = df_total["Placa"].nunique()

st.subheader("🔢 Total")
st.metric("Frota com posição hoje", total)

# =========================
# 🔢 TOTAL PROGRAMAÇÃO HOJE
# =========================
df_total_prog = df[
    (df["Tipo"] == "Frota") &
    (df["Programação"] == "Hoje")
]

total_prog = df_total_prog["Placa"].nunique()

st.subheader("🔢 Total Programação Hoje")
st.metric("Frota programada hoje", total_prog)

# =========================
# 📊 TABELA
# =========================
st.title("🚛 Omnilink")

st.dataframe(df_filtrado, use_container_width=True)

# 👇 AQUI
st.subheader("🧑‍✈️ Motoristas Disponíveis (>12h)")

st.dataframe(
    df_disp[["Motoristas", "Horas sem viagem","Disponibilidade","Status"]],
    use_container_width=True
)

# =========================
# 🗺️ MAPA
# =========================
st.subheader("📍 Mapa")

df_mapa = df_validos.rename(columns={
    "Latitude_corrigida": "lat",
    "Longitude_corrigida": "lon"
})

st.map(df_mapa)
