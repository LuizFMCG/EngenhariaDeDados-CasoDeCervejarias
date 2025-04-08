import os
import pandas as pd
import logging

# Inicializa o logger para registrar logs de execução e possíveis erros
logger = logging.getLogger(__name__)

def camada_ouro():
    """
    Etapa de agregação da arquitetura de medalhão.
    Esta função implementa a camada ouro, gerando uma visão analítica com
    a contagem de cervejarias por país, estado e tipo.
    Os dados vêm da camada prata (Parquet) e são salvos em CSV.
    """

    # Caminhos para a entrada (prata) e saída (ouro)
    prata_path = "/opt/airflow/datalake/prata/breweries.parquet"
    ouro_path = "/opt/airflow/datalake/ouro/cervejarias_agregado.csv"

    logger.info("Iniciando agregação para a camada ouro...")

    # Verifica se a entrada está disponível
    if not os.path.exists(prata_path):
        logger.error(f"Arquivo não encontrado: {prata_path}")
        raise FileNotFoundError(f"Arquivo não encontrado: {prata_path}")

    # Leitura da camada prata em formato colunar
    df = pd.read_parquet(prata_path)

    # ---------------------- TRATAMENTO E VALIDAÇÃO ----------------------

    # Limpeza preventiva de espaços em colunas críticas
    for col in ['country', 'state_province', 'brewery_type']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Filtra apenas registros completos (dados válidos para agregação)
    df = df[df['country'].notnull() & df['state_province'].notnull() & df['brewery_type'].notnull()]

    # Log informativo: exemplos de localizações únicas
    logger.info("Exemplos de combinações únicas:")
    exemplos = df[['country', 'state_province']].drop_duplicates().head(10)
    logger.info(f"\n{exemplos.to_string(index=False)}")

    # ---------------------- AGREGAÇÃO DOS DADOS ----------------------

    # Contagem de cervejarias agrupadas por país, estado e tipo
    agrupado = (
        df.groupby(['country', 'state_province', 'brewery_type'])
          .size()
          .reset_index(name="quantidade")
          .sort_values(by="quantidade", ascending=False)
    )

    # ---------------------- SALVANDO RESULTADO ----------------------

    # Garante que o diretório de destino existe
    os.makedirs(os.path.dirname(ouro_path), exist_ok=True)

    # Exporta o resultado como CSV
    agrupado.to_csv(ouro_path, index=False)

    logger.info(f"Dados agregados salvos na camada ouro: {ouro_path}")
    logger.info(f"Total de registros agregados: {len(agrupado)}")
    
    return ouro_path

# Execução direta para teste ou execução local
if __name__ == "__main__":
    try:
        camada_ouro()
        print("Agregação concluída com sucesso.")
    except Exception as e:
        print(f"Erro durante a agregação: {e}")




