# silver.py
from datetime import datetime
import zipfile
from io import BytesIO
import pandas as pd
from minio import Minio
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, when, lit, current_timestamp, rlike, lower
from pyspark.sql.types import DoubleType, IntegerType,StringType
from config import MinIOConfig
import logging
from typing import List, Dict, Optional
from silver_quality import SilverQualityValidator, silver_processing_time
import time
import tempfile
import os
import chardet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverProcessor:
    """Classe base para processar dados da camada Bronze → Silver"""
    
    def __init__(self, minio_config: MinIOConfig = None):
        # Isso é só guardar as informações
        self.minio_config = minio_config or MinIOConfig()
        # Isso é abrir a conexão de fato
        self.minio_client = Minio(
            self.minio_config.endpoint,
            access_key=self.minio_config.access_key,
            secret_key=self.minio_config.secret_key,
            secure=self.minio_config.secure
        )
        self.spark = self._create_spark_session()
        self._ensure_bucket_exists()
        # Inicializar o validador de qualidade - prometheus
        self.validator = SilverQualityValidator(self.spark)
        logger.info("✅ SilverQualityValidator inicializado")
    def _create_spark_session(self) -> SparkSession:
        """Cria sessão Spark com Delta Lake e S3 configurados"""
        
        # Configurar Spark Builder
        builder = (
            # Inicia o construtor da sessão Spark
            SparkSession.builder

                # Nome da aplicação exibida na interface do Spark
                .appName("CVM_Silver_Processing")

                # ------------------------------------------------------------
                # Dependências externas que o Spark precisa baixar automaticamente
                # ------------------------------------------------------------
                .config(
                    "spark.jars.packages",
                    "io.delta:delta-core_2.12:2.4.0"  # Só Delta
                )
                .config(
                    "spark.jars",
                    "/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar"
                )
                .config(
                    "spark.driver.extraClassPath",
                    "/opt/spark-jars/hadoop-aws-3.3.4.jar:/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar"
                )
                .config(
                    "spark.executor.extraClassPath",
                    "/opt/spark-jars/hadoop-aws-3.3.4.jar:/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar"
                )

                # ------------------------------------------------------------
                # Ativação do Delta Lake no Spark
                # ------------------------------------------------------------

                # Liga extensões SQL específicas do Delta Lake
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

                # Torna o catálogo Delta o padrão para criação/leitura de tabelas
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

                # ------------------------------------------------------------
                # Configurações para acessar o MinIO usando S3A
                # ------------------------------------------------------------

                # Endereço HTTP do serviço MinIO
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_config.endpoint}")

                # Access key (usuário público) do MinIO
                .config("spark.hadoop.fs.s3a.access.key", self.minio_config.access_key)

                # Secret key (senha) do MinIO
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_config.secret_key)

                # Obrigatório para MinIO: usa estilo path, não VHost
                .config("spark.hadoop.fs.s3a.path.style.access", "true")

                # Define explicitamente o filesystem S3A correto
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

                # Desativa SSL porque MinIO normalmente roda em HTTP no ambiente local
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

                # Usa provedor simples de credenciais (ideal para MinIO)
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                # ------------------------------------------------------------
                # Configurações de performance do Spark
                # ------------------------------------------------------------

                # Ativa otimização adaptativa → Spark ajusta planos de execução dinamicamente
                .config("spark.sql.adaptive.enabled", "true")

                # Número de partições usadas em operações como join e groupBy
                .config("spark.sql.shuffle.partitions", "8")

                # Tamanho máximo de cada partição de arquivo (128 MB)
                # Evita gerar muitos arquivos pequenos
                .config("spark.sql.files.maxPartitionBytes", 134217728)
        )
        
        # Adicionar Delta Lake
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark Session criada com Delta Lake")
        return spark
    
    def _ensure_bucket_exists(self):
        """Garante que o bucket silver existe"""
        if not self.minio_client.bucket_exists(self.minio_config.bucket_silver):
            self.minio_client.make_bucket(self.minio_config.bucket_silver)
            logger.info(f"Bucket {self.minio_config.bucket_silver} criado")


class CVMSilverProcessor(SilverProcessor):
    """Processa dados da CVM da camada Bronze para Silver usando Delta Lake"""
    
    # Definir quais CSVs queremos extrair do ZIP
    CSV_TARGETS = {
        "DRE_con": "dfp_cia_aberta_DRE_con",    # DRE Consolidado
        "DFC_DMPL_con": "dfp_cia_aberta_DMPL_con",  # Fluxo de Caixa Método Direto
        "BPA_con": "dfp_cia_aberta_BPA_con",     # Balanço Patrimonial Ativo Consolidado
        "BPP_con": "dfp_cia_aberta_BPP_con"      # Balanço Patrimonial Passivo Consolidado
    }
    
    def process_year(self, ano: str) -> Dict[str, bool]:
        """
        Processa todos os CSVs de um ano específico
        
        Args:
            ano: Ano a processar (ex: "2023")
            
        Returns:
            Dicionário com status de processamento de cada CSV
        """
        results = {}
        
        try:
            logger.info(f"🔄 Iniciando processamento Silver para ano {ano}")
            
            # 1. Buscar ZIP da camada Bronze
            zip_path = self._find_latest_zip(ano)
            if not zip_path:
                logger.error(f"❌ ZIP não encontrado para ano {ano}")
                return results
            
            # 2. Baixar ZIP do MinIO
            zip_bytes = self._download_from_minio(zip_path)
            
            # 3. Extrair e processar cada CSV
            with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
                for csv_key, csv_prefix in self.CSV_TARGETS.items():
                    csv_filename = f"{csv_prefix}_{ano}.csv"
                    
                    if csv_filename in zf.namelist():
                        logger.info(f"📄 Processando {csv_filename}...")
                        success = self._process_csv(zf, csv_filename, csv_key, ano)
                        results[csv_key] = success
                    else:
                        logger.warning(f"⚠️  {csv_filename} não encontrado no ZIP")
                        results[csv_key] = False
            
            logger.info(f"✅ Processamento concluído para {ano}")
            return results
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar ano {ano}: {e}")
            return results
    
    def _find_latest_zip(self, ano: str) -> Optional[str]:
        """Encontra o ZIP mais recente para um ano na camada Bronze"""
        prefix = f"gov_br_cvm/demonstracoes_financeiras_padronizadas/ano={ano}/"
        
        try:
            objects = self.minio_client.list_objects(
                self.minio_config.bucket_bronze,
                prefix=prefix,
                recursive=True
            )
            
            zip_files = [obj.object_name for obj in objects if obj.object_name.endswith('.zip')]
            
            if not zip_files:
                return None
            
            # Retornar o mais recente (último da lista ordenada)
            return sorted(zip_files)[-1]
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar ZIP: {e}")
            return None
    
    def _download_from_minio(self, object_path: str) -> bytes:
        """Baixa objeto do MinIO e retorna bytes"""
        response = self.minio_client.get_object(
            self.minio_config.bucket_bronze,
            object_path
        )
        data = response.read()
        response.close()
        response.release_conn()
        return data
    
    def _process_csv(self, zf: zipfile.ZipFile, csv_filename: str, csv_key: str, ano: str) -> bool:
        """
        Processa um CSV específico do ZIP e salva como Delta Lake.
        Executa uma única validação de qualidade após salvar.
        Args:
            zf: Objeto ZipFile já aberto
            csv_filename: Nome do arquivo CSV dentro do ZIP
            csv_key: Chave identificadora para aplicar filtros específicos
            ano: Ano de referência (para metadata e métricas)
        Returns: True se processado com sucesso, False caso contrário
        """
        tmp_path = None
        try:
             # 1. Extrair CSV do ZIP para arquivo temporário
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
                tmp_file.write(zf.read(csv_filename))
                tmp_path = tmp_file.name

            # Detectar encoding do arquivo (muitas vezes é ISO-8859-1)
            with open(tmp_path, 'rb') as f:
                encoding = chardet.detect(f.read(100000))['encoding']
            
            # 2. Ler diretamente com Spark
            df_spark = (
                self.spark.read
                .option("header", "true")
                .option("sep", ";")
                .option("encoding", encoding)
                .option("inferSchema", "false")  # Manter tudo como string inicialmente
                .csv(tmp_path)
            )
            logger.info(f"  📊 Linhas lidas: {df_spark.count():,}")

            # 3. Aplicar transformações de limpeza
            df_clean = self._clean_dataframe(df_spark, csv_key)

            # 4. Adicionar colunas de metadata
            df_final = (
                df_clean
                .withColumn("ano_referencia", lit(int(ano)))
                .withColumn("data_processamento", current_timestamp())
                .withColumn("fonte", lit("CVM"))
                .withColumn("tipo_demonstracao", lit(csv_key))
            )

            # 5. Salvar como Delta Lake
            delta_path = f"s3a://{self.minio_config.bucket_silver}/cvm/{csv_key}/"
            (
                df_final.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("replaceWhere", f"ano_referencia = {ano}")
                .partitionBy("ano_referencia")
                .save(delta_path)
            )
            logger.info(f"  ✅ Salvo em Delta: {delta_path}")
            logger.info(f"  📈 Total de registros: {df_final.count():,}")

            # 6. Validar qualidade e enviar métricas ao Prometheus (UMA VEZ)
            #    Usar self.validator — inicializado no __init__, não criar instância nova
            start_time = time.time()
            self.validator.validate_and_report_metrics(df_final, csv_key, ano)
            elapsed = time.time() - start_time
            silver_processing_time.labels(
                csv_type=csv_key,
                ano=ano
            ).observe(elapsed)
            logger.info(f"⏱️ Tempo de validação: {elapsed:.2f}s")

            return True

        except Exception as e:
            logger.error(f"  ❌ Erro ao processar {csv_filename}: {e}")
            return False
        
        finally:
        # Limpar arquivo temporário
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
                
    def _clean_dataframe(self, df, csv_key: str):
        """
        Aplica transformações de limpeza e padronização no DataFrame
        Args:
            df: DataFrame original lido do CSV
            csv_key: Chave identificadora para aplicar filtros específicos
        Returns: DataFrame limpo e padronizado

        """
        df_clean = df

        # Filter por DS_CONTA — apenas para tipos relevantes
        # Localização: silver.py, dentro de _clean_dataframe()
        # SUBSTITUIR o bloco ds_conta_filters pelo código abaixo:

        ds_conta_filters = {
            # ================================================================
            # DRE — Demonstração do Resultado do Exercício
            # ================================================================
            # Critérios Graham que dependem da DRE:
            #   ✅ Não ter prejuízo → precisa de "lucro líquido"
            #   ✅ P/L → precisa de "lucro por ação" ou "lucro líquido" + nº ações
            #   ✅ Dividendos → precisa de "dividendos"
            #
            "DRE_con": r"lucro líquido|lucro por ação|dividendos",
            #            ↑                ↑               ↑
            #            |                |               |
            #     Para saber se     Para calcular     Para critério
            #   houve prejuízo       o P/L                de DY

            # ================================================================
            # BPA — Balanço Patrimonial Ativo (o que a empresa TEM)
            # ================================================================
            # Critério Graham que depende do BPA:
            #   ✅ Liquidez Corrente → precisa de "ativo circulante"
            #
            "BPA_con": r"ativo circulante",
            #            ↑
            #     LC = Ativo Circulante / Passivo Circulante

            # ================================================================
            # BPP — Balanço Patrimonial Passivo (o que a empresa DEVE)
            # ================================================================
            # Critérios Graham que dependem do BPP:
            #   ✅ Liquidez Corrente → precisa de "passivo circulante"
            #   ✅ P/VPA → precisa de "patrimônio líquido"
            #
            "BPP_con": r"passivo circulante|patrimônio líquido",
            #            ↑                   ↑
            #   LC = Ativo Circ /     P/VPA = Preço / (PL / nº ações)
            #        Passivo Circ

            # ================================================================
            # DFC_DMPL — Mutações do Patrimônio Líquido
            # ================================================================
            # Mantido igual ao original (filtro por COLUNA_DF, não DS_CONTA)
            # Sem alteração necessária aqui.
        }

        # Resultado esperado: ~30.000–50.000 registros (vs ~12.000 antes)
        # Motivo: adicionamos "lucro líquido" e "patrimônio líquido"

        # ================================================================
        # 💡 FILTROS PARA DESENVOLVIMENTO FUTURO (não implementados agora)
        # ================================================================
        # Os filtros abaixo seriam úteis para análises mais avançadas,
        # mas não são necessários para os critérios de Graham utilizados
        # neste TCC (Silva Júnior, 2020). Deixados como referência:
        #
        # "DRE_con": r"receita|lucro|ebitda|resultado|custos|despesas|margem"
        #   → Para calcular margens operacionais, EBITDA, ROE etc.
        #
        # "BPA_con": r"ativo|caixa|aplicações financeiras|contas a receber|
        #              estoques|imobilizado|intangível|investimentos"
        #   → Para análise completa do balanço patrimonial
        #
        # "BPP_con": r"passivo|fornecedores|empréstimos|financiamentos|
        #              debêntures|capital social|reservas"
        #   → Para análise de endividamento e estrutura de capital
        #
        # "DFC_DMPL_con": r"patrimônio líquido|capital|reservas|lucros acumulados"
        #   → Para análise de fluxo de caixa completo
        # ================================================================

        if csv_key in ds_conta_filters and "DS_CONTA" in df_clean.columns:
            df_clean = df_clean.filter(
                lower(col("DS_CONTA")).rlike(ds_conta_filters[csv_key])
            )

        # Filter COLUNA_DF — apenas para DMPL
        if csv_key == "DFC_DMPL_con" and "COLUNA_DF" in df_clean.columns:
            df_clean = df_clean.filter(
                lower(col("COLUNA_DF")).rlike(r"patrimônio líquido")
            )

        # Trimmar strings
        string_cols = [field.name for field in df.schema.fields
                    if isinstance(field.dataType, StringType)]
        for col_name in string_cols:
            df_clean = df_clean.withColumn(col_name, trim(col(col_name)))

        # Converter VL_CONTA com escala
        df_clean = df_clean.withColumn(
            "VL_CONTA",
            when(col("ESCALA_MOEDA") == "MIL", col("VL_CONTA").cast(DoubleType()) * 1000)
            .otherwise(col("VL_CONTA").cast(DoubleType()))
        )

        # Converter datas
        date_cols = ['DT_REFER', 'DT_INI_EXERC', 'DT_FIM_EXERC']
        for col_name in date_cols:
            if col_name in df_clean.columns:
                df_clean = df_clean.withColumn(
                    col_name,
                    to_date(col(col_name), "yyyy-MM-dd")
                )

        # Remover linhas completamente nulas
        df_clean = df_clean.dropna(how='all')

        # ⚠️ Guardar se DataFrame está vazio antes de retornar
        count = df_clean.count()
        if count == 0:
            logger.warning(f"⚠️ DataFrame vazio após filtros para {csv_key}!")
        else:
            logger.info(f"  ✅ {count:,} linhas após limpeza")

        return df_clean
        
    def read_silver_table(self, table_name: str, ano: Optional[int] = None):
        """
        Lê uma tabela Delta da camada Silver
        
        Args:
            table_name: Nome da tabela (BP_con, DRE_con, etc)
            ano: Ano específico (opcional, None = todos os anos)
            
        Returns:
            Spark DataFrame
        """
        delta_path = f"s3a://{self.minio_config.bucket_silver}/cvm/{table_name}"
        
        df = self.spark.read.format("delta").load(delta_path)
        
        if ano:
            df = df.filter(
                (col("ano_referencia") == ano) | 
                (col("ano_referencia") == str(ano))
            )
        
        return df


processor = CVMSilverProcessor()

# Processar múltiplos anos
if __name__ == "__main__":
    anos = ["2020", "2021", "2022", "2023", "2024","2025"]
    for ano in anos:
        results = processor.process_year(ano)
        print(f"\n📊 Resultados para {ano}:")
        for csv_key, success in results.items():
            status = "✅" if success else "❌"
            print(f"  {status} {csv_key}")