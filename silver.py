# silver.py
from datetime import datetime
import zipfile
from io import BytesIO
import pandas as pd
from minio import Minio
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, when, lit, current_timestamp, rlike, lower, translate, ilike
from pyspark.sql.types import DoubleType, IntegerType, StringType
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

def normalize_text(column):
    """
    Normaliza texto para facilitar filtros e comparações.

    Etapas:
    - converte para minúsculo
    - remove acentos
    """

    return translate(
        lower(column),
        "áàãâäéèêëíìîïóòõôöúùûüç",
        "aaaaaeeeeiiiiooooouuuuc"
    )


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
    
    # ========================================================================
    # DEFINIÇÃO DOS CSVs A SEREM PROCESSADOS
    # ========================================================================
    # Separados por tipo de documento (DFP e FRE) para melhor organização
    
    # ────────────────────────────────────────────────────────────────────────
    # DFP - Demonstrações Financeiras Padronizadas (anual)
    # ────────────────────────────────────────────────────────────────────────
    DFP_TARGETS = {
        "composicao_capital": "dfp_cia_aberta_composicao_capital",  # Composição do Capital Social
        "DRE_con": "dfp_cia_aberta_DRE_con",           # DRE Consolidado
        "DFC_DMPL_con": "dfp_cia_aberta_DMPL_con",     # Fluxo de Caixa Método Direto
        "BPA_con": "dfp_cia_aberta_BPA_con",           # Balanço Patrimonial Ativo Consolidado
        "BPP_con": "dfp_cia_aberta_BPP_con"            # Balanço Patrimonial Passivo Consolidado
    }
    
    # ────────────────────────────────────────────────────────────────────────
    # FRE - Formulário de Referência (trimestral/anual)
    # ────────────────────────────────────────────────────────────────────────

    FRE_TARGETS = {

        "volume_valor_mobiliario": "fre_cia_aberta_volume_valor_mobiliario",  # Volume e Valor Mobiliário (Preço da Ação)
        "distribuicao_dividendos": "fre_cia_aberta_distribuicao_dividendos",
    }
    # ────────────────────────────────────────────────────────────────────────
    

    
    def process_year(self, ano: str, document_type: str = "DFP") -> Dict[str, bool]:
        """
        Processa todos os CSVs de um ano específico
        
        Args:
            ano: Ano a processar (ex: "2023")
            document_type: Tipo de documento ("DFP" ou "FRE")
            
        Returns:
            Dicionário com status de processamento de cada CSV
        """
        results = {}
        
        try:
            logger.info(f"🔄 Iniciando processamento Silver para {document_type} {ano}")
            
            # Selecionar targets apropriados baseado no tipo de documento
            if document_type == "DFP":
                CSV_TARGETS = self.DFP_TARGETS
                bronze_path_prefix = "gov_br_cvm/demonstracoes_financeiras_padronizadas"
            elif document_type == "FRE":
                CSV_TARGETS = self.FRE_TARGETS
                bronze_path_prefix = "gov_br_cvm/formulario_de_referencia"
            else:
                raise ValueError(f"Tipo de documento inválido: {document_type}")
            
            # 1. Buscar ZIP da camada Bronze e baixa os dados binários (_find_latest_zip e _download_from_minio)
            zip_data = self._get_bronze_zip(ano, bronze_path_prefix)
            if not zip_data:
                logger.error(f"❌ Arquivo ZIP não encontrado para {document_type} {ano}")
                return {csv_key: False for csv_key in CSV_TARGETS.keys()}
            
            # 2. Processar cada CSV do ZIP
            for csv_key, csv_filename in CSV_TARGETS.items():
                try:
                    logger.info(f"\n📄 Processando: {csv_key} ({document_type})")
                    
                    # Medir tempo de processamento para métricas Prometheus
                    start_time = time.time()
                    
                    # 2.1 Extrair CSV do ZIP
                    df = self._extract_csv_from_zip(zip_data, csv_filename)
                    if df is None:
                        logger.warning(f"⚠️ CSV {csv_filename} não encontrado no ZIP")
                        results[csv_key] = False
                        continue
                    
                    # 2.2 Limpar e transformar dados
                    df_clean = self._clean_dataframe(df, csv_key, document_type)
                    
                    # 2.3 Adicionar metadados
                    df_clean = df_clean.withColumn("ano_referencia", lit(ano))
                    df_clean = df_clean.withColumn("data_processamento", current_timestamp())
                    df_clean = df_clean.withColumn("document_type", lit(document_type))
                    
                    # 2.4 Salvar em Delta Lake
                    self._save_to_delta(df_clean, csv_key, ano)
                    
                    # 2.5 Validação de qualidade + métricas Prometheus
                    self.validator.validate_and_report_metrics(df_clean, csv_key, ano)
                    
                    # 2.6 Registrar tempo de processamento
                    processing_time = time.time() - start_time

                    silver_processing_time.labels(csv_type=csv_key, ano=ano).observe(processing_time)
                    
                    results[csv_key] = True
                    logger.info(f"✅ {csv_key} processado com sucesso ({processing_time:.2f}s)")
                    
                except Exception as e:
                    logger.error(f"❌ Erro ao processar {csv_key}: {e}")
                    results[csv_key] = False
                    
        except Exception as e:
            logger.error(f"❌ Erro geral no processamento: {e}")
            return {csv_key: False for csv_key in CSV_TARGETS.keys()}
            
        return results
    
    def _get_bronze_zip(self, ano: str, bronze_path_prefix: str) -> Optional[bytes]:
        """
        Busca o arquivo ZIP mais recente da camada Bronze
        
        Args:
            ano: Ano de referência
            bronze_path_prefix: Prefixo do caminho (diferente para DFP e FRE)
        Returns:
            Dados binários do arquivo ZIP ou None se não encontrado
        """
        try:
            # Construir o path do ZIP na Bronze
            # Formato: gov_br_cvm/{tipo_documento}/ano={ano}/data_extracao={data}/arquivo.zip
            prefix = f"{bronze_path_prefix}/ano={ano}/"
            
            # Listar objetos no MinIO
            objects = self.minio_client.list_objects(
                self.minio_config.bucket_bronze,
                prefix=prefix,
                recursive=True
            )
            
            # Procurar pelo arquivo ZIP mais recente
            zip_files = [obj.object_name for obj in objects if obj.object_name.endswith('.zip')]
            
            if not zip_files:
                logger.error(f"_get_bronze_zip - Nenhum ZIP encontrado em {prefix}")
                return None
                
            # Pegar o mais recente (último na lista ordenada)
            object_path = sorted(zip_files)[-1]
            logger.info(f"📦 Lendo ZIP: {object_path}")
            
            # Baixar o arquivo
            response = self.minio_client.get_object(
                self.minio_config.bucket_bronze,
                object_path
            )
            
            data = response.read()
            response.close()
            response.release_conn()
            return data
            
        except Exception as e:
            logger.error(f"_get_bronze_zip - Erro ao buscar ZIP: {e}")
            return None
    
    def _extract_csv_from_zip(self, zip_data: bytes, csv_filename: str):
        """
        Extrai um CSV específico do ZIP e converte para Spark DataFrame
        
        Args:
            zip_data: Dados binários do arquivo ZIP
            csv_filename: Nome do CSV dentro do ZIP (sem extensão .csv)
        """
        try:
            with zipfile.ZipFile(BytesIO(zip_data)) as zf:
                # Procurar pelo arquivo CSV no ZIP
                csv_name = None
                for name in zf.namelist():
                    if csv_filename in name and name.endswith('.csv'):
                        csv_name = name
                        break
                
                if not csv_name:
                    logger.warning(f"CSV {csv_filename} não encontrado no ZIP")
                    return None
                
                logger.info(f"  📥 Extraindo: {csv_name}")
                
                # Ler o CSV com pandas (mais fácil para encoding)
                with zf.open(csv_name) as csv_file:
                    # Detectar encoding
                    raw_data = csv_file.read(100000)  # Ler os primeiros 100 KB para detecção
                    detected = chardet.detect(raw_data)
                    confianca=detected['confidence']
                    encoding = detected['encoding'] if confianca > 0.7 else 'windows-1252'
                    
                    # Ler CSV com pandas
                    pdf = pd.read_csv(
                        BytesIO(raw_data),
                        encoding=encoding,
                        sep=';',
                        dtype=str,  # Ler tudo como string primeiro
                        na_values=['', 'NA', 'NULL', 'None']
                    )
                    
                    logger.info(f"  📊 Linhas lidas: {len(pdf):,}")
                    
                    # Converter para Spark DataFrame
                    df = self.spark.createDataFrame(pdf)
                    
                    return df
                    
        except Exception as e:
            logger.error(f"Erro ao extrair CSV {csv_filename}: {e}")
            return None
    
    def _save_to_delta(self, df, table_name: str, ano: str):
        """
        Salva DataFrame em Delta Lake com particionamento por ano.
        Estratégia: replaceWhere para substituir apenas o ano específico.
        """
        delta_path = f"s3a://{self.minio_config.bucket_silver}/cvm/{table_name}"
    
    # Verificar se tabela existe e se está particionada
        try:
            from delta.tables import DeltaTable
            dt = DeltaTable.forPath(self.spark, delta_path)
            details = dt.detail().select("partitionColumns").collect()[0]
            is_partitioned = len(details["partitionColumns"]) > 0
            
            if not is_partitioned:
                logger.warning(f"  ⚠️  Tabela existe SEM particionamento - recriando...")
                dt.delete()  # Força recriação
                
        except Exception:
            logger.info(f"  ✨ Tabela não existe - será criada")
        
        # Salvar com replaceWhere (substitui só o ano)
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"ano_referencia = '{ano}'") \
            .option("mergeSchema", "true") \
            .partitionBy("ano_referencia") \
            .save(delta_path)
        
        logger.info(f"  ✅ Ano {ano} salvo em {delta_path}")
    
    def _clean_dataframe(self, df, csv_key: str, document_type: str):
        """
        Aplica transformações de limpeza e padronização no DataFrame
        
        Args:
            df: DataFrame original lido do CSV
            csv_key: Chave identificadora para aplicar filtros específicos
            document_type: Tipo de documento ("DFP" ou "FRE")
            
        Returns: 
            DataFrame limpo e padronizado
        """
        df_clean = df

        # ====================================================================
        # FILTROS ESPECÍFICOS POR TIPO DE TABELA E DOCUMENTO
        # ====================================================================
        
        # ────────────────────────────────────────────────────────────────────
        # FILTROS DFP
        # ────────────────────────────────────────────────────────────────────
        if document_type == "DFP":
            ds_conta_filters = {
                # DRE — Demonstração do Resultado do Exercício
                # Critérios Graham que dependem da DRE:
                #   ✅ Não ter prejuízo → precisa de "lucro líquido"
                #   ✅ P/L → precisa de "lucro por ação" ou "lucro líquido" + nº ações
                #   ✅ Dividendos → precisa de "dividendos"
                "DRE_con": r"lucro/prejuízo consolidado do período|lucro ou prejuízo líquido consolidado do período|por ação",
                
                # BPA — Balanço Patrimonial Ativo
                # Critério Graham: Liquidez Corrente → precisa de "ativo circulante"
                "BPA_con": r"ativo circulante",
                
                # BPP — Balanço Patrimonial Passivo
                # Critérios Graham:
                #   ✅ Liquidez Corrente → precisa de "passivo circulante"
                #   ✅ P/VPA → precisa de "patrimônio líquido"
                "BPP_con": r"passivo circulante|patrimônio líquido consolidado",
            }
            
            # Aplicar filtro DS_CONTA se aplicável
            required_cols = ["DS_CONTA", "ORDEM_EXERC"]
            if csv_key in ds_conta_filters and all(col in df_clean.columns for col in required_cols):
                df_clean = df_clean.filter(
                    lower(col("DS_CONTA")).rlike(ds_conta_filters[csv_key]) &
                    lower(col("ORDEM_EXERC")).ilike("ÚLTIMO")
                )

            # Filter COLUNA_DF — apenas para DMPL
            if csv_key == "DFC_DMPL_con" and "COLUNA_DF" in df_clean.columns:
                df_clean = df_clean.filter(
                    lower(col("COLUNA_DF")).rlike(r"patrimônio líquido") &
                    lower(col("ORDEM_EXERC")).ilike("ÚLTIMO")
                )
        
        # ────────────────────────────────────────────────────────────────────
        # FILTROS FRE
        # ────────────────────────────────────────────────────────────────────
        elif document_type == "FRE":
            # ================================================================
            # 1. VOLUME_VALOR_MOBILIARIO (Preço da Ação)
            # Baseado em: 04_Indicador_Preco_Acao_ATUALIZADO.docx
            # ================================================================
            if csv_key == "volume_valor_mobiliario":
                # Segundo o documento, NÃO há necessidade de filtrar por TP_MERC
                # pois o campo relevante é "Especie_Acao" (Ordinária/Preferencial)
                # 
                # Mantemos todos os registros para permitir análise de:
                # - Ações Ordinárias (ON - ticker termina em 3)
                # - Ações Preferenciais (PN - ticker termina em 4)
                # - Outras classes (PNA, PNB, etc.)
                #
                # Filtros de qualidade básicos:
                if "Valor_Cotacao_Media" in df_clean.columns:
                    # Remove registros sem cotação (dados inválidos)
                    df_clean = df_clean.filter(
                        col("Valor_Cotacao_Media").isNotNull()
                    )
                
                # ⚠️ IMPORTANTE (do documento):
                # "Sempre filtre por Especie_Acao para garantir que está pegando a ação correta"
                # Mas aqui na Silver, mantemos TODAS as espécies disponíveis.
                # O filtro por espécie será feito na camada Gold conforme necessidade.
                
                logger.info(f"  ℹ️  Mantendo todas as espécies de ação (ON, PN, PNA, PNB, etc)")
            
            # ================================================================
            # 2. DISTRIBUICAO_DIVIDENDOS (Dividendos e JCP)
            # Baseado em: 06_Indicador_Dividendos_JCP_ATUALIZADO.docx
            # ================================================================
            elif csv_key == "distribuicao_dividendos":
                # Segundo o documento, campos-chave são:
                # - Dividendo_Distribuido_Total
                # - Lucro_Liquido_Ajustado
                # - Data_Fim_Exercicio_Social
                #
                # Filtros de qualidade:
                if "Dividendo_Distribuido_Total" in df_clean.columns:
                    # Manter apenas registros com valores informados
                    # (permite valores zero - significa que não houve distribuição)
                    df_clean = df_clean.filter(
                        col("Dividendo_Distribuido_Total").isNotNull()
                    )
                
                logger.info(f"  ℹ️  Mantendo todos os registros de dividendos (incluindo zeros)")

        # ====================================================================
        # LIMPEZA GERAL (APLICADA A TODOS OS DATAFRAMES)
        # ====================================================================
        
        # Trimmar strings
        string_cols = [field.name for field in df.schema.fields
                    if isinstance(field.dataType, StringType)]
        for col_name in string_cols:
            df_clean = df_clean.withColumn(col_name, trim(col(col_name)))


        # DFP - Converter VL_CONTA com escala
        if "VL_CONTA" in df_clean.columns:
            df_clean = df_clean.withColumn(
                "VL_CONTA",
                when(
                    lower(col("DS_CONTA")).like("%por ação%"),   
                    col("VL_CONTA").cast(DoubleType())        
                )
                .when(
                    col("ESCALA_MOEDA") == "MIL",
                    col("VL_CONTA").cast(DoubleType()) * 1000
                )
                .otherwise(col("VL_CONTA").cast(DoubleType()))
            )
        
        # FRE - Converter valores FRE - DIVIDENDOS

        if "Dividendo_Distribuido_Total" in df_clean.columns:
            df_clean = df_clean.withColumn(
                "Dividendo_Distribuido_Total",
                col("Dividendo_Distribuido_Total").cast(DoubleType())
            )
        
        if "Lucro_Liquido_Ajustado" in df_clean.columns:
            df_clean = df_clean.withColumn(
                "Lucro_Liquido_Ajustado",
                col("Lucro_Liquido_Ajustado").cast(DoubleType())
            )
        
        # Converter valores FRE - COTAÇÕES

        cotacao_cols = [
            "Valor_Cotacao_Media",
            "Valor_Maior_Cotacao",
            "Valor_Menor_Cotacao",
            "Valor_Volume_Negociado"
        ]
        
        for col_name in cotacao_cols:
            if col_name in df_clean.columns:
                df_clean = df_clean.withColumn(
                    col_name,
                    col(col_name).cast(DoubleType())
                )
        # DFP - Converter datas

        date_cols_dfp = ['DT_REFER', 'DT_INI_EXERC', 'DT_FIM_EXERC']
        for col_name in date_cols_dfp:
            if col_name in df_clean.columns:
                df_clean = df_clean.withColumn(
                    col_name,
                    to_date(col(col_name), "yyyy-MM-dd")
                )
        
        # FRE - Converter datas

        date_cols_fre = [
            'Data_Fim_Trimestre',       # volume_valor_mobiliario
            'Data_Fim_Exercicio_Social'  # distribuicao_dividendos
        ]
        for col_name in date_cols_fre:
            if col_name in df_clean.columns:
                df_clean = df_clean.withColumn(
                    col_name,
                    to_date(col(col_name), "yyyy-MM-dd")
                )

        # Remover linhas completamente nulas
        df_clean = df_clean.dropna(how='all')

        # Logging de quantidade de linhas
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
            table_name: Nome da tabela (BP_con, DRE_con, volume_valor_mobiliario, etc)
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

# Processar múltiplos anos para DFP e FRE
if __name__ == "__main__":
    anos = ["2020", "2021", "2022", "2023", "2024"]
    
    # Processar DFP (anual)
    logger.info("\n" + "="*60)
    logger.info("📊 PROCESSANDO DFP (DEMONSTRAÇÕES FINANCEIRAS)")
    logger.info("="*60)
    for ano in anos:
        results = processor.process_year(ano, document_type="DFP")
        print(f"\n📊 Resultados DFP para {ano}:")
        for csv_key, success in results.items():
            status = "✅" if success else "❌"
            print(f"  {status} {csv_key}")
    
    # Processar FRE (trimestral/anual)
    logger.info("\n" + "="*60)
    logger.info("📋 PROCESSANDO FRE (FORMULÁRIO DE REFERÊNCIA)")
    logger.info("="*60)
    for ano in anos:
        results = processor.process_year(ano, document_type="FRE")
        print(f"\n📋 Resultados FRE para {ano}:")
        for csv_key, success in results.items():
            status = "✅" if success else "❌"
            print(f"  {status} {csv_key}")