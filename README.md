# Investimentos
Pipeline de dados para análise fundamentalista
Estrutura do Repositório:
Mini-IO/
├── config.py                    # Configurações centralizadas (MinIO, Spark)
├── ingest.py                    # Ingestão Bronze (CVM + Fundamentus)
├── silver.py                    # Processamento Silver (PySpark + Delta)
├── silver_quality.py            # Validações de qualidade de dados
├── gold.py                      # Modelagem Gold (Star Schema)
├── run_metrics.py               # Servidor HTTP de métricas Prometheus
├── Dockerfile                   # Imagem Docker customizada
├── docker-compose.yml           # Orquestração de 9 serviços
├── prometheus.yml               # Coleta métrica de forma temporal (em dev)
├── requirements.txt             # Dependências Python
├── README.md                    # Documentação do projeto
└── grafana/
    └── provisioning/
        ├── datasources/         
        └── dashboards/          # Dashboards de qualidade (em dev)
# 1. Iniciar infraestrutura Docker
docker-compose up -d

# 2. Ingestão Bronze (5 anos × 3 formulários = 15 downloads)
docker exec -it ingest python ingest.py
# Saída esperada: 15 arquivos ZIP + metadados JSON em s3://bronze/

# 3. Processamento Silver (CSVs → Delta Lake)
docker exec -it ingest python silver.py
# Saída esperada: 35 tabelas Delta em s3://silver/cvm/

# 4. Modelagem Gold (Star Schema)
docker exec -it ingest python gold.py
# Saída esperada: 3 dimensões + 5 fatos em s3://gold/

# 5. Exportar para Power BI
docker exec -it ingest python -c "
from gold import GoldDimensionalProcessor
processor = GoldDimensionalProcessor()
spark = processor.spark
for dim in ['DIM_EMPRESA', 'DIM_TEMPO', 'DIM_TIPO_ACAO']:
    df = spark.read.format('delta').load(f's3a://gold/dimensoes/{dim}')
    df.coalesce(1).write.mode('overwrite').parquet(f'/data/{dim}.parquet')
for fato in ['FATO_BALANCO', 'FATO_DRE', 'FATO_COTACAO', 'FATO_DIVIDENDOS', 'FATO_CAPITAL_SOCIAL']:
    df = spark.read.format('delta').load(f's3a://gold/fatos/{fato}')
    df.coalesce(1).write.mode('overwrite').parquet(f'/data/{fato}.parquet')
"
# Saída esperada em C:/mini-io-export/