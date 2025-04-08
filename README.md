Desafio Técnico – Engenharia de Dados – Caso de Cervejarias (PSW)
Este documento descreve a solução para o desafio técnico de Engenharia de Dados da PSW. O objetivo é construir um pipeline ETL utilizando Python e Apache Airflow, aplicando a arquitetura de data lake em camadas (medalhão).
Tecnologias Utilizadas
- Python 3.10
- Apache Airflow 2.9
- Docker e Docker Compose
- Pandas com suporte a Parquet via PyArrow
- Open Brewery DB API (v1)
Estrutura do Projeto
├── dags/                          # Scripts do pipeline (bronze, prata, ouro)
│   ├── bronze.py                  # Ingestão da API e salvamento dos dados brutos
│   ├── prata.py                   # Transformação e padronização dos dados
│   └── ouro.py                    # Agregação analítica para camada ouro
├── datalake/
│   ├── bronze/                    # Dados brutos (.json.gz e versão leitura)
│   ├── prata/                     # Dados tratados (.parquet particionado)
│   └── ouro/                      # Dados agregados (.csv)
├── docker-compose.yaml            # Configuração do ambiente Docker + Airflow
├── .env                           # Variáveis de ambiente
└── README.md
Como Executar
Pré-requisitos: Docker e Docker Compose instalados.

1. Clone este repositório:
   git clone https://github.com/seu-usuario/desafio-cervejarias.git
   cd desafio-cervejarias

2. O arquivo `.env` com as variáveis necessárias já está incluso no repositório.

3. Inicie os serviços com Docker Compose:
   docker-compose up -d

4. Acesse o Airflow em http://localhost:8080
   Usuário: DesafioPSW
   Senha: pswpoa

5. No painel do Airflow, ative e execute manualmente a DAG pipeline_cervejarias_dag.
Pipeline ETL
A DAG pipeline_cervejarias_dag orquestra as seguintes etapas:
1. camada_bronze: coleta os dados da API da Open Brewery DB em paralelo, salva versão histórica compactada (.json.gz) e versão estática para leitura.
2. camada_prata: aplica transformações e validações nos dados, e salva como Parquet particionado por país e estado.
3. camada_ouro: realiza agregações por país, estado e tipo de cervejaria, e exporta o resultado como CSV.
Design da Solução e Justificativas Técnicas
- Arquitetura de data lake estruturada em três camadas (bronze, prata, ouro), garantindo separação de responsabilidades e rastreabilidade.
- Orquestração com Apache Airflow dentro de contêineres, simulando um ambiente real de produção.
- Ingestão com execução paralela utilizando ThreadPoolExecutor para acelerar a coleta paginada da API.
- Transformação e particionamento em Parquet com Pandas e PyArrow, priorizando simplicidade e performance para baixo volume de dados.
- Agregação final salva em CSV, formato amplamente aceito por ferramentas como Power BI.
- Versionamento por data e hora para facilitar auditoria e reprocessamentos.
Monitoramento e Alertas
- Logging estruturado em todos os scripts (nível info e error).
- Configuração de retries e retry_delay nas tarefas do Airflow para tolerância a falhas.
- Validações internas de colunas obrigatórias, estrutura dos dados e consistência de localização.
- Estrutura compatível com on_failure_callback, permitindo fácil extensão com notificações por Slack ou e-mail.
Verificação dos Resultados
Os dados são salvos no caminho /opt/airflow/datalake dentro do container.
Para copiar os dados para o host local:
   docker cp airflow-docker-airflow-worker-1:/opt/airflow/datalake ./datalake_export

A estrutura exportada incluirá:
├── bronze/
│   ├── date=YYYY-MM-DD/
│   │   └── breweries_HH-MM-SS.json.gz
│   └── breweries.json
├── prata/
│   └── breweries.parquet
└── ouro/
    └── cervejarias_agregado.csv
Extensões Futuras Possíveis
- Inclusão de testes automatizados (Pytest)
- Substituição de Pandas por PySpark para maior escalabilidade
- Implementação de alertas por Slack ou e-mail com Airflow
- Upload automático do CSV da camada ouro para algum destino externo (S3, FTP, Google Drive)
Autor
Luiz Felipe de M. C. Giacobbo
