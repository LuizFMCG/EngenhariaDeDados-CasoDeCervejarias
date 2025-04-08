import pandas as pd
import json
import os

# Caminho local onde os dados exportados do Docker foram salvos
CAMINHO_BRONZE_LOCAL = "datalake_export/datalake/bronze/breweries.json"

# Caminho onde o relatório será salvo (na mesma pasta deste script)
CAMINHO_RELATORIO = os.path.join(os.path.dirname(__file__), "analise_exploratoria_bronze.txt")

# Verifica se o arquivo existe
if not os.path.exists(CAMINHO_BRONZE_LOCAL):
    raise FileNotFoundError(f"Arquivo não encontrado: {CAMINHO_BRONZE_LOCAL}")

# Carrega os dados do JSON
with open(CAMINHO_BRONZE_LOCAL, "r") as f:
    dados = json.load(f)

df = pd.DataFrame(dados)

# Abre o arquivo txt para salvar a análise
with open(CAMINHO_RELATORIO, "w", encoding="utf-8") as f:

    # Exibe as colunas disponíveis
    f.write("Colunas disponíveis:\n")
    f.write(", ".join(df.columns.tolist()) + "\n\n")
    print("Colunas disponíveis:")
    print(df.columns.tolist())

    # Verifica colunas com valores nulos
    nulos = df.isnull().sum()
    f.write("Valores nulos por coluna:\n")
    f.write(str(nulos) + "\n\n")
    print("\nValores nulos por coluna:")
    print(nulos)

    # Distribuição de valores únicos em colunas de localização
    for col in ['country', 'state', 'city']:
        if col in df.columns:
            dist = df[col].value_counts()
            f.write(f"Distribuição de valores únicos em '{col}':\n")
            f.write(str(dist) + "\n\n")
            print(f"\nDistribuição de valores únicos em '{col}':")
            print(dist)

    # Verifica a presença de valores nulos em latitude e longitude
    coord_nulls = df[['latitude', 'longitude']].isnull().sum()
    f.write("Verificação de valores nulos em latitude e longitude:\n")
    f.write(str(coord_nulls) + "\n\n")
    print("\nVerificação de valores nulos em latitude e longitude:")
    print(coord_nulls)

    # Número de valores únicos por coluna
    unicos = df.nunique().sort_values()
    f.write("Número de valores únicos por coluna:\n")
    f.write(str(unicos) + "\n\n")
    print("\nNúmero de valores únicos por coluna:")
    print(unicos)


