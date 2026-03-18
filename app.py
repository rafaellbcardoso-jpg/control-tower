# =========================
# 🔥 TRATAMENTO LAT/LONG (CORRIGIDO DE VERDADE)
# =========================
def limpar_coordenada(valor):
    try:
        valor = str(valor)

        # mantém sinal negativo
        negativo = "-" if valor.startswith("-") else ""

        # remove tudo que não é número ou ponto
        valor = valor.replace("-", "")
        
        partes = valor.split(".")

        if len(partes) >= 2:
            valor = partes[0] + "." + "".join(partes[1:])
        else:
            valor = partes[0]

        valor = float(negativo + valor)

        return valor
    except:
        return None


df["Latitude"] = df["Latitude"].apply(limpar_coordenada)
df["Longitude"] = df["Longitude"].apply(limpar_coordenada)

# remove inválidos
df = df.dropna(subset=["Latitude", "Longitude"])

# remove coordenadas absurdas
df = df[
    (df["Latitude"].between(-35, 5)) &
    (df["Longitude"].between(-75, -30))
]
