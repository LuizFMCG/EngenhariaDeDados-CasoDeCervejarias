import os
import shutil
import pandas as pd
import logging

# Configuração do logger para registrar informações, avisos e erros
logger = logging.getLogger(__name__)

def camada_prata():
    """
    Etapa de transformação da arquitetura de medalhão.
    Esta função implementa a camada prata, que transforma dados brutos da camada bronze
    em dados limpos, normalizados e particionados no formato Parquet para uso analítico.
    """

    # Caminhos para os dados de entrada e saída
    bronze_base_path = "/opt/airflow/datalake/bronze"
    prata_path = "/opt/airflow/datalake/prata/breweries.parquet"

    logger.info("Iniciando transformação da camada prata...")

    # Localização do arquivo JSON contendo os dados brutos coletados da API
    bronze_file = os.path.join(bronze_base_path, "breweries.json")

    # Verificação se o arquivo de entrada existe
    if not os.path.exists(bronze_file):
        logger.error(f"Arquivo fixo não encontrado: {bronze_file}")
        raise FileNotFoundError(f"Arquivo fixo não encontrado: {bronze_file}")

    logger.info(f"Lendo arquivo fixo: {bronze_file}")

    # Leitura do JSON bruto como DataFrame do pandas
    df = pd.read_json(bronze_file)

    # ------------------------- TRANSFORMAÇÕES -------------------------

    # Remoção de espaços em branco nas colunas de localização, que são usadas para particionamento
    colunas_localizacao = ['country', 'state_province', 'city']
    for col in colunas_localizacao:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Validação das colunas mínimas necessárias para a agregação na camada ouro
    colunas_necessarias = ['country', 'state_province', 'brewery_type']
    for col in colunas_necessarias:
        if col not in df.columns:
            logger.error(f"Coluna obrigatória ausente: {col}")
            raise ValueError(f"Coluna obrigatória ausente: {col}")

    logger.info(f"Total de registros após transformação: {len(df)}")

    # ---------------------- PERSISTÊNCIA DOS DADOS ----------------------

    # Limpeza do caminho de destino para garantir que não haja dados antigos
    if os.path.exists(prata_path):
        shutil.rmtree(prata_path)

    # Criação do diretório se não existir
    os.makedirs(os.path.dirname(prata_path), exist_ok=True)

    # Escrita dos dados em formato colunar (Parquet) particionado por localização
    # Isso melhora performance em análises futuras na camada ouro
    df.to_parquet(
        path=prata_path,
        index=False,
        partition_cols=["country", "state_province"]
    )

    logger.info(f"Dados salvos na camada prata: {prata_path}")

# Execução local da função para testes ou execução direta
if __name__ == "__main__":
    try:
        camada_prata()
        print("Transformação concluída com sucesso.")
    except Exception as e:
        print(f"Erro durante a transformação: {e}")


