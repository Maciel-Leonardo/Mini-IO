# gold.py
from datetime import datetime
from minio import Minio
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, when, lit, 
    first, coalesce, abs as spark_abs, count, countDistinct
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, BooleanType
from config import MinIOConfig
import logging
from typing import List, Dict, Optional
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoldProcessor:
    """Classe base para processar dados da camada Silver → Gold"""
    
    def __init__(self, minio_config: MinIOConfig = None):
        # Guardar as informações
        self.minio_config = minio_config or MinIOConfig()
        # Abrir a conexão de fato
        self.minio_client = Minio(
            self.minio_config.endpoint,
            access_key=self.minio_config.access_key,
            secret_key=self.minio_config.secret_key,
            secure=self.minio_config.secure
        )
        self.spark = self._create_spark_session()
        self._ensure_bucket_exists()
        logger.info("✅ GoldProcessor inicializado")
    
    def _create_spark_session(self) -> SparkSession:
        """Cria sessão Spark com Delta Lake e S3 configurados"""
        
        # Configurar Spark Builder
        builder = (
            # Inicia o construtor da sessão Spark
            SparkSession.builder

                # Nome da aplicação exibida na interface do Spark
                .appName("CVM_Gold_Processing")

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
        """Garante que o bucket gold existe"""
        if not self.minio_client.bucket_exists(self.minio_config.bucket_gold):
            self.minio_client.make_bucket(self.minio_config.bucket_gold)
            logger.info(f"✅ Bucket {self.minio_config.bucket_gold} criado")
        else:
            logger.info(f"✅ Bucket {self.minio_config.bucket_gold} já existe")


class CVMGoldProcessor(GoldProcessor):
    """Processa dados da CVM da camada Silver para Gold - Indicadores de Graham"""
    
    # Tabelas Silver necessárias
    SILVER_TABLES = ["DRE_con", "BPA_con", "BPP_con"]
    
    def process_year(self, ano: int) -> bool:
        """
        Processa dados de um ano específico e calcula indicadores de Graham
        
        Args:
            ano: Ano a processar (ex: 2024)
            
        Returns:
            True se processamento foi bem-sucedido
        """
        try:
            start_time = time.time()
            logger.info(f"🎯 Iniciando processamento Gold para ano {ano}")
            
            # 1. Carregar tabelas Silver
            logger.info("📚 Carregando tabelas Silver...")
            dre = self._read_silver_table("DRE_con", ano)
            bpa = self._read_silver_table("BPA_con", ano)
            bpp = self._read_silver_table("BPP_con", ano)
            
            # Validar se há dados
            if dre.count() == 0:
                logger.warning(f"⚠️  Nenhum dado DRE encontrado para {ano}")
                return False
            
            # 2. Extrair indicadores
            logger.info("🔍 Extraindo indicadores...")
            lucro = self._extract_lucro_liquido(dre)
            patrimonio = self._extract_patrimonio_liquido(bpp)
            ativo = self._extract_ativo_circulante(bpa)
            passivo = self._extract_passivo_circulante(bpp)
            dividendos = self._extract_dividendos(dre)
            num_acoes = self._extract_num_acoes(dre)
            
            # 3. Consolidar indicadores
            logger.info("🔗 Consolidando indicadores...")
            indicadores = self._consolidate_indicators(
                lucro, patrimonio, ativo, passivo, dividendos, num_acoes, ano
            )
            
            # 4. Calcular métricas derivadas
            logger.info("🧮 Calculando métricas derivadas...")
            indicadores = self._calculate_derived_metrics(indicadores)
            
            # 5. Aplicar filtros de Graham
            logger.info("✅ Aplicando critérios de Benjamin Graham...")
            indicadores = self._apply_graham_filters(indicadores)
            
            # 6. Salvar na camada Gold
            logger.info("💾 Salvando na camada Gold...")
            self._save_to_gold(indicadores, ano)
            
            # 7. Exibir estatísticas
            self._show_statistics(indicadores, ano)
            
            elapsed = time.time() - start_time
            logger.info(f"⏱️  Processamento concluído em {elapsed:.2f}s")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar ano {ano}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _read_silver_table(self, table_name: str, ano: int):
        """
        Lê uma tabela Delta da camada Silver
        
        Args:
            table_name: Nome da tabela (DRE_con, BPA_con, BPP_con)
            ano: Ano de referência
            
        Returns:
            Spark DataFrame
        """
        delta_path = f"s3a://{self.minio_config.bucket_silver}/cvm/{table_name}"
        
        logger.info(f"  📖 Lendo {table_name} de {delta_path}")
        
        df = self.spark.read.format("delta").load(delta_path)
        
        # Filtrar por ano
        df = df.filter(
            (col("ano_referencia") == ano) | 
            (col("ano_referencia") == str(ano))
        )
        
        count = df.count()
        logger.info(f"     ✓ {count:,} registros carregados")
        
        return df
    
    def _extract_lucro_liquido(self, dre_df):
        """
        Extrai Lucro Líquido da DRE
        
        CRITÉRIO GRAHAM: Empresa não teve prejuízo
        """
        logger.info("  💰 Extraindo Lucro Líquido...")
        
        lucro = dre_df.filter(
            col("DS_CONTA").rlike("(?i)lucro.*líquido")
        ).groupBy("CNPJ_CIA", "DENOM_CIA", "DT_REFER").agg(
            spark_sum("VL_CONTA").alias("lucro_liquido")
        )
        
        count = lucro.count()
        logger.info(f"     ✓ {count:,} registros extraídos")
        return lucro
    
    def _extract_patrimonio_liquido(self, bpp_df):
        """
        Extrai Patrimônio Líquido do BPP
        
        CRITÉRIO GRAHAM: P/VPA (necessita PL para calcular VPA)
        """
        logger.info("  🏦 Extraindo Patrimônio Líquido...")
        
        patrimonio = bpp_df.filter(
            col("DS_CONTA").rlike("(?i)patrimônio.*líquido")
        ).groupBy("CNPJ_CIA", "DT_REFER").agg(
            spark_sum("VL_CONTA").alias("patrimonio_liquido")
        )
        
        count = patrimonio.count()
        logger.info(f"     ✓ {count:,} registros extraídos")
        return patrimonio
    
    def _extract_ativo_circulante(self, bpa_df):
        """
        Extrai Ativo Circulante do BPA
        
        CRITÉRIO GRAHAM: Liquidez Corrente >= 1.0
        """
        logger.info("  📊 Extraindo Ativo Circulante...")
        
        ativo = bpa_df.filter(
            col("DS_CONTA").rlike("(?i)ativo.*circulante")
        ).groupBy("CNPJ_CIA", "DT_REFER").agg(
            spark_sum("VL_CONTA").alias("ativo_circulante")
        )
        
        count = ativo.count()
        logger.info(f"     ✓ {count:,} registros extraídos")
        return ativo
    
    def _extract_passivo_circulante(self, bpp_df):
        """
        Extrai Passivo Circulante do BPP
        
        CRITÉRIO GRAHAM: Liquidez Corrente >= 1.0
        """
        logger.info("  📉 Extraindo Passivo Circulante...")
        
        passivo = bpp_df.filter(
            col("DS_CONTA").rlike("(?i)passivo.*circulante")
        ).groupBy("CNPJ_CIA", "DT_REFER").agg(
            spark_sum("VL_CONTA").alias("passivo_circulante")
        )
        
        count = passivo.count()
        logger.info(f"     ✓ {count:,} registros extraídos")
        return passivo
    
    def _extract_dividendos(self, dre_df):
        """
        Extrai Dividendos da DRE
        
        CRITÉRIO GRAHAM: Empresa paga dividendos
        """
        logger.info("  💸 Extraindo Dividendos...")
        
        dividendos = dre_df.filter(
            col("DS_CONTA").rlike("(?i)dividendo")
        ).groupBy("CNPJ_CIA", "DT_REFER").agg(
            spark_sum(spark_abs(col("VL_CONTA"))).alias("dividendos")
        )
        
        count = dividendos.count()
        logger.info(f"     ✓ {count:,} registros extraídos")
        return dividendos
    
    def _extract_num_acoes(self, dre_df):
        """
        Extrai número de ações da DRE (ou usa estimativa)
        
        Necessário para calcular LPA e VPA
        """
        logger.info("  🔢 Extraindo número de ações...")
        
        # Tentar buscar LPA reportado
        lpa_info = dre_df.filter(
            col("DS_CONTA").rlike("(?i)lucro.*por.*ação")
        ).groupBy("CNPJ_CIA", "DT_REFER").agg(
            first("VL_CONTA").alias("lpa_reportado")
        )
        
        # Se não tiver LPA, usar estimativa conservadora de 1 bilhão de ações
        num_acoes = lpa_info.withColumn(
            "num_acoes",
            when(col("lpa_reportado").isNotNull(), 
                 lit(1000000000))
            .otherwise(lit(1000000000))
        ).select("CNPJ_CIA", "DT_REFER", "num_acoes")
        
        count = num_acoes.count()
        logger.info(f"     ✓ {count:,} registros (estimativa: 1bi ações)")
        return num_acoes
    
    def _consolidate_indicators(self, lucro, patrimonio, ativo, passivo, 
                                dividendos, num_acoes, ano):
        """
        Consolida todos os indicadores em um único DataFrame
        """
        # Base: lucro (tem DENOM_CIA)
        indicadores = lucro
        
        # Join com outros indicadores
        indicadores = indicadores \
            .join(patrimonio, ["CNPJ_CIA", "DT_REFER"], "left") \
            .join(ativo, ["CNPJ_CIA", "DT_REFER"], "left") \
            .join(passivo, ["CNPJ_CIA", "DT_REFER"], "left") \
            .join(dividendos, ["CNPJ_CIA", "DT_REFER"], "left") \
            .join(num_acoes, ["CNPJ_CIA", "DT_REFER"], "left")
        
        # Adicionar ano de referência
        indicadores = indicadores.withColumn("ANO_REFERENCIA", lit(ano))
        
        # Adicionar trimestre baseado na data
        indicadores = indicadores.withColumn(
            "TRIMESTRE", 
            when(col("DT_REFER").endswith("03-31"), lit("T1"))
            .when(col("DT_REFER").endswith("06-30"), lit("T2"))
            .when(col("DT_REFER").endswith("09-30"), lit("T3"))
            .when(col("DT_REFER").endswith("12-31"), lit("T4"))
            .otherwise(lit("ANUAL"))
        )
        
        count = indicadores.count()
        logger.info(f"  ✓ {count:,} registros consolidados")
        
        return indicadores
    
    def _calculate_derived_metrics(self, df):
        """
        Calcula métricas derivadas: LPA, VPA, LC
        
        LPA = Lucro Líquido / Número de Ações
        VPA = Patrimônio Líquido / Número de Ações
        LC  = Ativo Circulante / Passivo Circulante
        """
        df = df.withColumn(
            "lpa",
            when(col("num_acoes").isNotNull() & (col("num_acoes") > 0),
                 col("lucro_liquido") / col("num_acoes"))
            .otherwise(lit(None).cast(DoubleType()))
        ).withColumn(
            "vpa",
            when(col("num_acoes").isNotNull() & (col("num_acoes") > 0),
                 col("patrimonio_liquido") / col("num_acoes"))
            .otherwise(lit(None).cast(DoubleType()))
        ).withColumn(
            "lc",
            when(col("passivo_circulante").isNotNull() & (col("passivo_circulante") != 0),
                 col("ativo_circulante") / col("passivo_circulante"))
            .otherwise(lit(None).cast(DoubleType()))
        )
        
        logger.info("  ✓ Métricas derivadas calculadas (LPA, VPA, LC)")
        
        return df
    
    def _apply_graham_filters(self, df):
        """
        Aplica os critérios de filtro de Benjamin Graham
        
        FILTRO 1: Lucro Líquido > 0 (empresa não teve prejuízo)
        FILTRO 2: P/L × P/VPA < 22.5 (requer preços - fase 2)
        FILTRO 3: Dividendos > 0 (empresa paga dividendos)
        FILTRO 4: LC >= 1.0 (boa saúde financeira)
        """
        df = df.withColumn(
            "passou_filtro_1",
            when(col("lucro_liquido") > 0, True).otherwise(False)
        ).withColumn(
            "passou_filtro_3",
            when(coalesce(col("dividendos"), lit(0)) > 0, True).otherwise(False)
        ).withColumn(
            "passou_filtro_4",
            when(coalesce(col("lc"), lit(0)) >= 1.0, True).otherwise(False)
        ).withColumn(
            "passou_todos_filtros",
            when(
                (col("passou_filtro_1") == True) & 
                (col("passou_filtro_3") == True) & 
                (col("passou_filtro_4") == True),
                True
            ).otherwise(False)
        )
        
        logger.info("  ✓ Filtros de Graham aplicados")
        
        return df
    
    def _save_to_gold(self, df, ano):
        """
        Salva DataFrame na camada Gold usando Delta Lake
        """
        delta_path = f"s3a://{self.minio_config.bucket_gold}/cvm/indicadores_graham"
        
        logger.info(f"  💾 Salvando em {delta_path}")
        
        try:
            # Tentar salvar com merge (upsert) se tabela já existe
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("replaceWhere", f"ANO_REFERENCIA = {ano}") \
                .partitionBy("ANO_REFERENCIA") \
                .save(delta_path)
            
            logger.info(f"  ✅ Dados salvos com sucesso (partição: {ano})")
            
        except Exception as e:
            logger.warning(f"  ⚠️  Erro ao salvar com particionamento: {e}")
            logger.info("  🔄 Tentando salvar sem particionamento...")
            
            # Fallback: salvar sem particionamento
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(delta_path)
            
            logger.info("  ✅ Dados salvos (modo fallback)")
    
    def _show_statistics(self, df, ano):
        """
        Exibe estatísticas do processamento
        """
        total = df.count()
        aprovados_f1 = df.filter(col("passou_filtro_1") == True).count()
        aprovados_f3 = df.filter(col("passou_filtro_3") == True).count()
        aprovados_f4 = df.filter(col("passou_filtro_4") == True).count()
        aprovados_todos = df.filter(col("passou_todos_filtros") == True).count()
        
        empresas_unicas = df.select("CNPJ_CIA").distinct().count()
        
        logger.info(f"""
            ╔═══════════════════════════════════════════════════════════╗
            ║ RESULTADO DO PROCESSAMENTO - ANO {ano}                    
            ╠═══════════════════════════════════════════════════════════╣
            ║ Total de registros: {total:>6}                            
            ║ Empresas únicas: {empresas_unicas:>6}                     
            ║                                                           
            ║ CRITÉRIOS DE BENJAMIN GRAHAM:                            
            ║ Passou Filtro 1 (LL > 0): {aprovados_f1:>6} ({aprovados_f1/total*100:>5.1f}%)      
            ║ Passou Filtro 3 (Div > 0): {aprovados_f3:>6} ({aprovados_f3/total*100:>5.1f}%)     
            ║ Passou Filtro 4 (LC >= 1): {aprovados_f4:>6} ({aprovados_f4/total*100:>5.1f}%)     
            ║                                                           
            ║ 🎯 Passou TODOS os filtros: {aprovados_todos:>6} ({aprovados_todos/total*100:>5.1f}%)       
            ╚═══════════════════════════════════════════════════════════╝
        """)
    
    def read_gold_table(self, ano: Optional[int] = None):
        """
        Lê a tabela Delta da camada Gold
        
        Args:
            ano: Ano específico (opcional, None = todos os anos)
            
        Returns:
            Spark DataFrame
        """
        delta_path = f"s3a://{self.minio_config.bucket_gold}/cvm/indicadores_graham"
        
        df = self.spark.read.format("delta").load(delta_path)
        
        if ano:
            df = df.filter(
                (col("ANO_REFERENCIA") == ano) | 
                (col("ANO_REFERENCIA") == str(ano))
            )
        
        return df


# Instanciar processador
processor = CVMGoldProcessor()

# Processar múltiplos anos
if __name__ == "__main__":
    anos = [2020, 2021, 2022, 2023, 2024]
    
    logger.info("=" * 70)
    logger.info("🚀 INICIANDO PIPELINE GOLD - INDICADORES DE GRAHAM")
    logger.info("=" * 70)
    
    for ano in anos:
        success = processor.process_year(ano)
        status = "✅" if success else "❌"
        logger.info(f"\n{status} Ano {ano} processado\n")
    
    logger.info("=" * 70)
    logger.info("🎉 PIPELINE GOLD CONCLUÍDO!")
    logger.info("=" * 70)