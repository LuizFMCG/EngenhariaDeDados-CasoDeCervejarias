import os
import json
import gzip
import requests
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logger para rastrear execução e eventuais erros
logger = logging.getLogger(__name__)

# ---------------------- CONFIGURAÇÕES GERAIS ----------------------

# Endpoint da API da Open Brewery DB (v1)
BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 50  # Número de registros por página de requisição

# ---------------------- FUNÇÃO: BAIXA UMA PÁGINA ----------------------

def baixar_pagina(page):
    """
    Faz requisição de uma página da API.
    Retorna a lista de registros ou uma lista vazia se falhar.
    """
    url = f"{BASE_URL}?page={page}&per_page={PER_PAGE}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Página {page} retornou {len(data)} registros.")
        return data
    except requests.RequestException as e:
        logger.error(f"Erro na página {page}: {e}")
        return []

# ---------------------- FUNÇÃO: DESCOBRE QUANTIDADE DE PÁGINAS ----------------------

def descobrir_paginas_disponiveis():
    """
    Descobre quantas páginas de dados estão disponíveis na API.
    Para quando uma página retornar vazia.
    """
    logger.info("Descobrindo total de páginas disponíveis...")
    paginas_validas = []
    page = 1

    while True:
        dados = baixar_pagina(page)
        if not dados:
            break
        paginas_validas.append(page)
        page += 1

    logger.info(f"Total de páginas com dados: {len(paginas_validas)}")
    return paginas_validas

# ---------------------- FUNÇÃO PRINCIPAL: CAMADA BRONZE ----------------------

def camada_bronze():
    """
    Extrai dados da API Open Brewery DB.
    Salva uma versão bruta compactada e uma versão estática para leitura pela camada prata.
    Também registra metadados da execução.
    """

    logger.info("Iniciando extração paralela da API Open Brewery DB...")

    # Descobre páginas úteis da API
    paginas = descobrir_paginas_disponiveis()
    all_data = []

    # Realiza a coleta paralela com até 10 threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(baixar_pagina, p): p for p in paginas}
        for future in as_completed(futures):
            data = future.result()
            if data:
                all_data.extend(data)

    # Remove duplicidades com base no campo 'id'
    ids_unicos = {item["id"]: item for item in all_data}
    all_data = list(ids_unicos.values())

    # Geração do timestamp para versionamento
    execucao_data = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    execucao_hora = datetime.now(timezone.utc).strftime("%H-%M-%S")

    # Caminhos no data lake
    bronze_base_dir = "/opt/airflow/datalake/bronze"
    bronze_dir = os.path.join(bronze_base_dir, f"date={execucao_data}")
    os.makedirs(bronze_dir, exist_ok=True)

    # ---------------------- ARMAZENAMENTO ----------------------

    # Salva versão histórica e compactada (.json.gz)
    json_path = os.path.join(bronze_dir, f"breweries_{execucao_hora}.json.gz")
    with gzip.open(json_path, "wt", encoding="utf-8") as f:
        json.dump(all_data, f)

    # Salva metadados úteis sobre a execução
    metadata = {
        "timestamp_extracao": datetime.now(timezone.utc).isoformat(),
        "quantidade_registros": len(all_data),
        "arquivo_gerado": json_path
    }
    meta_path = os.path.join(bronze_base_dir, "metadata.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f)

    # Salva versão estática e descompactada para leitura pela camada prata
    json_latest = os.path.join(bronze_base_dir, "breweries.json")
    with open(json_latest, "w", encoding="utf-8") as f:
        json.dump(all_data, f)

    logger.info(f"Extração finalizada com {len(all_data)} registros únicos.")
    logger.info(f"Dados salvos em: {json_path}")




