import streamlit as st
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

BUCKET_NAME = "control-tower-dados"

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["google"]
)

client = storage.Client(
    credentials=credentials,
    project="paine-stramlit"
)

bucket = client.bucket(BUCKET_NAME)

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

# 🧠 POSIÇÃO
df["Posição"] = pd.to_datetime(
    df["Data de comunicação"].astype(str).str[4:24],
    errors="coerce"
)

# 🧠 Tipo
df["Tipo"] = df["Proprietário"].apply(
    lambda x: "Frota" if str(x).strip().upper() == "LEMAR" else "Agregado"
)

# 🔧 CORREÇÃO LAT/LONG
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

# 🔥 ÚLTIMA POSIÇÃO POR PLACA
df = df.sort_values(by="Posição", ascending=False)
df = df.drop_duplicates(subset="Placa", keep="first")

# 🌍 GEOLOCALIZAÇÃO
geolocator = Nominatim(user_agent="control_tower")
reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1)

def obter_localizacao(lat, lon):
    try:
        location = reverse((lat, lon), language="pt")
        if location and location.raw.get("address"):
            address = location.raw["address"]
            
            cidade = (
                address.get("city") or
                address.get("town") or
                address.get("village") or
                address.get("municipality")
            )
            
            uf = address.get("state_code") or address.get("state")
            
            if cidade and uf:
                return f"{cidade} - {uf}"
        
        return "Não identificado"
    
    except:
        return "Erro"

df["Localização Atual"] = df.apply(
    lambda row: obter_localizacao(
        row["Latitude_corrigida"],
        row["Longitude_corrigida"]
    ) if pd.notna(row["Latitude_corrigida"]) else "Sem coordenada",
    axis=1
)

# 🇧🇷 FORMATAÇÃO DATA
df["Posição"] = df["Posição"].dt.strftime("%d/%m/%Y %H:%M:%S")

# 🔽 COLUNAS (SEM LAT/LONG)
colunas_finais = [
    "Placa",
    "Tipo",
    "Posição",
    "Localização Atual"
]

df = df[[col for col in colunas_finais if col in df.columns]]

# 🎛️ FILTRO
st.sidebar.title("Filtros")

tipo_selecionado = st.sidebar.multiselect(
    "Tipo",
    options=df["Tipo"].unique(),
    default=df["Tipo"].unique()
)

df_filtrado = df[df["Tipo"].isin(tipo_selecionado)]

# 📊 TABELA
st.title("🚛 Base Omni - Última Posição por Placa")

st.dataframe(df_filtrado)
