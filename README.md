# EngenhariaDeDados-CasoDeCervejarias
Engenharia de Dados – Caso de Cervejarias - Open Brewery DB API
# Desafio Técnico – Engenharia de Dados – Caso de Cervejarias

Este documento descreve a solução para o desafio técnico de Engenharia de Dados. O objetivo é construir um pipeline ETL utilizando Python e Apache Airflow, aplicando arquitetura de medalhão tendo uma camada bronze, prata e ouro, conforme orientado no desafio.

## Tecnologias Utilizadas
- Python 3.10
- Apache Airflow 2.9
- Docker e Docker Compose
- Pandas com suporte a Parquet via PyArrow
- Open Brewery DB API (v1)

## Estrutura do Projeto

```
├── config/                         # Arquivos de configuração diversos
├── dags/                           # Scripts do pipeline (bronze, prata, ouro)
│   ├── camada_bronze.py            # Ingestão da API e salvamento dos dados brutos
│   ├── camada_prata.py             # Transformação e padronização dos dados
│   └── camada_ouro.py              # Agregação analítica para camada ouro
├── datalake_export/                # Pasta para dados exportados do container Airflow para verificação
│      └──datalake                  # Dados exportados do container Airflow
│          ├── bronze/              # Dados brutos exportados (.json.gz e versão leitura)
│          ├── prata/               # Dados tratados exportados (.parquet particionado)
│          └── ouro/                # Dados agregados exportados (.csv)
├── logs/                           # Logs do Airflow
├── plugins/                        # Plugins adicionais para Airflow
├── utils/                          # Scripts utilitários diversos
│   └── perfil_bronze.py            # Script para gerar relatório exploratório da camada bronze
├── docker-compose.yaml             # Configuração do ambiente Docker + Airflow
├── .env                            # Variáveis de ambiente
└── README.md                       # Documentação do projeto
```

## Como Executar

**Pré-requisitos:** Docker e Docker Compose instalados.

1. Clone este repositório:
```bash
git clone https://github.com/LuizFMCG/EngenhariaDeDados-CasoDeCervejarias.git
cd EngenhariaDeDados-CasoDeCervejarias
```

2. O arquivo `.env` com as variáveis necessárias já está incluso no repositório.

3. Inicie os serviços com Docker Compose:
```bash
docker-compose up -d
```

4. Acesse o Airflow em http://localhost:8080
   - Usuário: `DesafioPSW`
   - Senha: `pswpoa`

5. No painel do Airflow, ative e execute manualmente a DAG `pipeline_cervejarias_dag`.

## Pipeline ETL

A DAG `pipeline_cervejarias_dag` orquestra as seguintes etapas:
- **camada_bronze**: coleta os dados da API da Open Brewery DB em paralelo, salva versão histórica compactada (`.json.gz`) e versão estática para leitura.
- **camada_prata**: aplica transformações e validações nos dados, salvando-os em formato Parquet particionado por país e estado.
- **camada_ouro**: realiza agregações por país, estado e tipo de cervejaria, exportando o resultado em CSV.

## Design da Solução e Justificativas Técnicas
- Arquitetura medalhão garantindo separação clara de responsabilidades e rastreabilidade.
- Airflow via Docker para uma simulação realística do ambiente produtivo.
- Paralelismo na coleta da API para melhor performance.
- Uso do Parquet com Pandas/PyArrow na camada prata, favorecendo desempenho e compatibilidade.
- Exportação final em CSV para fácil consumo em ferramentas analíticas.
- Versionamento com timestamps para auditoria e reprocessamentos.

## Monitoramento e Alertas
- Logs estruturados em todas as etapas.
- Retries e delays configurados nas tasks do Airflow.
- Validações internas para garantir integridade e qualidade.
- Preparação para integração futura com notificações por Slack ou e-mail via callbacks do Airflow.

## Verificação dos Resultados

- Os dados são salvos em `/opt/airflow/datalake` no container.

- Para copiar os dados para o host local, primeiro obtenha o nome do container Airflow:

```bash 
docker ps
```
- Em seguida, execute (substitua <NOME_DO_CONTAINER> pelo nome correto obtido acima):
```
docker cp <NOME_DO_CONTAINER>:/opt/airflow/datalake ./datalake_export
```

- A estrutura exportada será:
```
├── bronze/
│   ├── date=YYYY-MM-DD/
│   │   └── breweries_HH-MM-SS.json.gz
│   └── breweries.json
|   └── metadata.json
├── prata/
│   └── breweries.parquet
└── ouro/
    └── cervejarias_agregado.csv
```

## Explicação do diretório utils
perfil_bronze.py:

- Script utilizado no início do desenvolvimento para realizar uma análise exploratória dos dados brutos exportados da camada bronze (importados manualmente do container Airflow).

- Gera um relatório em formato .txt contendo informações essenciais para entender a estrutura inicial dos dados, incluindo colunas existentes, valores nulos por coluna, distribuição de valores únicos nas colunas de localização (país, estado, cidade) e verificação da presença de coordenadas geográficas.

- Esse relatório orientou diretamente as decisões técnicas sobre limpeza, transformação e particionamento adotadas posteriormente na camada prata.

## Possíveis Sofisticações Futuras
- Testes automatizados com Pytest.
- Uso de PySpark para maior escalabilidade.
- Alertas automatizados via Airflow.

## Autor
Luiz Felipe de M. C. Giacobbo

