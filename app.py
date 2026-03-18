import pandas as pd
from google.cloud import storage
from io import BytesIO

BUCKET_NAME = "control-tower-dados"

client = storage.Client()
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
# SALVAR NO BUCKET
# =========================
output = BytesIO()
df.to_csv(output, index=False)

blob_output = bucket.blob("etl/omni_tratado.csv")
blob_output.upload_from_string(output.getvalue(), content_type="text/csv")

print("Omni tratado salvo com sucesso")
