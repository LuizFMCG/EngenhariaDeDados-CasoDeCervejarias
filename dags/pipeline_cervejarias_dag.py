from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Importação das funções de cada camada do pipeline
from bronze import camada_bronze
from prata import camada_prata
from ouro import camada_ouro

logger = logging.getLogger(__name__)

# Argumentos padrão aplicados a todas as tarefas
default_args = {
    "owner": "airflow",                      # Responsável pela DAG
    "depends_on_past": False,                # Execuções não dependem de runs anteriores
    "email_on_failure": False,               # Sem envio de e-mails em caso de falha
    "email_on_retry": False,                 # Sem e-mail ao reexecutar
    "retries": 2,                            # Tenta novamente até 2 vezes
    "retry_delay": timedelta(minutes=5)      # Intervalo entre tentativas
}

# Definição do DAG (pipeline) principal
with DAG(
    dag_id="pipeline_cervejarias_dag",       # Identificador único da DAG
    description="Pipeline de ingestão, transformação e agregação dos dados da Open Brewery DB",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),         # Data de início das execuções agendadas
    schedule_interval="@daily",              # Execução diária automática
    catchup=False,                           # Ignora execuções pendentes no passado
    max_active_runs=1,                       # Evita sobreposição de execuções
    tags=["cervejarias", "engenharia de dados", "ETL"]  # Tags para organização no Airflow
) as dag:

    # ------------------ Camada Bronze ------------------
    tarefa_bronze = PythonOperator(
        task_id="camada_bronze",             # Nome identificador da tarefa
        python_callable=camada_bronze,       # Função Python que será executada
        retries=3,                            # Tenta 3 vezes em caso de falha
        retry_delay=timedelta(minutes=3),    # Espera 3 min entre tentativas
        execution_timeout=timedelta(minutes=5),  # Tempo máximo de execução
        on_failure_callback=None             # Pode ser usado futuramente para alertas
    )

    # ------------------ Camada Prata ------------------
    tarefa_prata = PythonOperator(
        task_id="camada_prata",
        python_callable=camada_prata,
        execution_timeout=timedelta(minutes=5)
    )

    # ------------------ Camada Ouro ------------------
    tarefa_ouro = PythonOperator(
        task_id="camada_ouro",
        python_callable=camada_ouro,
        execution_timeout=timedelta(minutes=5)
    )

    # ------------------ DEFINIÇÃO DA ORDEM DE EXECUÇÃO ------------------
    # A camada bronze deve ser executada antes da prata, que antecede a ouro
    tarefa_bronze >> tarefa_prata >> tarefa_ouro


