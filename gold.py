# /home/claude/Mini-IO/gold.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, sum as spark_sum, abs as spark_abs
from delta.tables import DeltaTable
from config import MinIOConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GrahamIndicatorCalculator:
    """Calcula indicadores fundamentalistas Graham"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.minio_config = MinIOConfig()
        logger.info("✅ GrahamIndicatorCalculator inicializado")
    
    def _create_spark_session(self):
        """Cria sessão Spark (copiar de silver.py)"""
        # [COPIAR configuração completa de silver.py linha 58-145]
        pass
    
    def read_silver_table(self, tabela: str, ano: int):
        """Lê tabela Silver"""
        path = f"s3a://{self.minio_config.bucket_silver}/cvm/{tabela}"
        df = self.spark.read.format("delta").load(path)
        return df.filter(
            (col("ano_referencia") == ano) | 
            (col("ano_referencia") == str(ano))
        )
    
    def calculate_indicators(self, ano: int):
        """
        Calcula TODOS os indicadores Graham para um ano
        
        Fluxo:
        1. Ler 6 tabelas Silver
        2. Filtrar contas específicas
        3. Fazer joins
        4. Calcular cada indicador
        """
        logger.info(f"📊 Calculando indicadores para {ano}...")
        
        # ================================================================
        # PASSO 1: LER TABELAS SILVER
        # ================================================================
        
        # DFP
        bpp = self.read_silver_table("BPP_con", ano)
        dre = self.read_silver_table("DRE_con", ano)
        bpa = self.read_silver_table("BPA_con", ano)
        acoes = self.read_silver_table("composicao_capital", ano)
        
        # FRE
        precos = self.read_silver_table("volume_valor_mobiliario", ano)
        dividendos = self.read_silver_table("distribuicao_dividendos", ano)
        
        # ================================================================
        # PASSO 2: FILTRAR CONTAS ESPECÍFICAS
        # ================================================================
        
        # Patrimônio Líquido (BPP - CD_CONTA = 2.07)
        pl = bpp.filter(col("CD_CONTA") == "2.07") \
               .select(
                   col("CNPJ_CIA"),
                   col("DENOM_CIA"),
                   col("VL_CONTA").alias("PATRIMONIO_LIQUIDO"),
                   col("DT_REFER")
               )
        
        # Lucro Líquido (DRE - CD_CONTA = 3.11)
        lucro = dre.filter(col("CD_CONTA") == "3.11") \
                   .select(
                       col("CNPJ_CIA"),
                       col("VL_CONTA").alias("LUCRO_LIQUIDO")
                   )
        
        # Ativo Circulante (BPA - CD_CONTA começa com 1.01)
        ativo_circ = bpa.filter(col("CD_CONTA").startswith("1.01")) \
                        .groupBy("CNPJ_CIA") \
                        .agg(spark_sum("VL_CONTA").alias("ATIVO_CIRCULANTE"))
        
        # Passivo Circulante (BPP - CD_CONTA começa com 2.01)
        passivo_circ = bpp.filter(col("CD_CONTA").startswith("2.01")) \
                          .groupBy("CNPJ_CIA") \
                          .agg(spark_sum("VL_CONTA").alias("PASSIVO_CIRCULANTE"))
        
        # Ações em Circulação
        acoes = acoes.withColumn(
            "ACOES_CIRCULACAO",
            col("QT_ACAO_TOTAL_CAP_INTEGR") - col("QT_ACAO_TOTAL_TESOURO")
        ).select("CNPJ_CIA", "ACOES_CIRCULACAO")
        
        # Preço Médio (FRE - média dos trimestres)
        # ⚠️ IMPORTANTE: FRE usa CNPJ_Companhia, não CNPJ_CIA
        preco_medio = precos.groupBy("CNPJ_Companhia") \
                           .agg(avg("Valor_Cotacao_Media").alias("PRECO_MEDIO")) \
                           .withColumnRenamed("CNPJ_Companhia", "CNPJ_CIA")
        
        # Dividendos Totais (FRE)
        div_total = dividendos.select(
            col("CNPJ_Companhia").alias("CNPJ_CIA"),
            col("Dividendo_Distribuido_Total").alias("DIVIDENDOS_TOTAL")
        )
        
        # ================================================================
        # PASSO 3: COMBINAR TUDO (JOINS)
        # ================================================================
        
        df_base = pl \
            .join(lucro, "CNPJ_CIA", "left") \
            .join(acoes, "CNPJ_CIA", "left") \
            .join(ativo_circ, "CNPJ_CIA", "left") \
            .join(passivo_circ, "CNPJ_CIA", "left") \
            .join(preco_medio, "CNPJ_CIA", "left") \
            .join(div_total, "CNPJ_CIA", "left")
        
        # ================================================================
        # PASSO 4: CALCULAR INDICADORES
        # ================================================================
        
        df_indicadores = df_base \
            .withColumn("VPA", col("PATRIMONIO_LIQUIDO") / col("ACOES_CIRCULACAO")) \
            .withColumn("LPA", col("LUCRO_LIQUIDO") / col("ACOES_CIRCULACAO")) \
            .withColumn("P_VPA", col("PRECO_MEDIO") / col("VPA")) \
            .withColumn("P_L", col("PRECO_MEDIO") / col("LPA")) \
            .withColumn(
                "DIVIDEND_YIELD",
                ((col("DIVIDENDOS_TOTAL") / col("ACOES_CIRCULACAO")) / col("PRECO_MEDIO")) * 100
            ) \
            .withColumn("ROE", (col("LUCRO_LIQUIDO") / col("PATRIMONIO_LIQUIDO")) * 100) \
            .withColumn("LIQUIDEZ_CORRENTE", col("ATIVO_CIRCULANTE") / col("PASSIVO_CIRCULANTE")) \
            .withColumn("ANO", lit(ano))
        
        logger.info(f"✅ Indicadores calculados: {df_indicadores.count()} empresas")
        return df_indicadores
    
    def apply_graham_filters(self, df):
        """Aplica 6 filtros Graham"""
        logger.info("🔍 Aplicando filtros de Graham...")
        
        df_filtrado = df.filter(
            (col("P_L") > 0) & (col("P_L") < 15) &
            (col("P_VPA") > 0) & (col("P_VPA") < 1.5) &
            (col("DIVIDEND_YIELD") > 0) &
            (col("LUCRO_LIQUIDO") > 0) &
            (col("ROE") > 0) &
            (col("LIQUIDEZ_CORRENTE") > 2) &
            col("PRECO_MEDIO").isNotNull() &
            col("VPA").isNotNull()
        )
        
        count_total = df.count()
        count_aprovadas = df_filtrado.count()
        
        logger.info(f"📊 Total analisadas: {count_total}")
        logger.info(f"✅ Aprovadas: {count_aprovadas} ({count_aprovadas/count_total*100:.1f}%)")
        
        return df_filtrado
    
    def save_to_gold(self, df, tabela_name: str):
        """Salva em Delta Lake Gold"""
        path = f"s3a://{self.minio_config.bucket_gold}/{tabela_name}"
        
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .save(path)
        
        logger.info(f"💾 Salvo: {path}")
    
    def process_year(self, ano: int):
        """Pipeline completo para um ano"""
        logger.info(f"🚀 Processando {ano}...")
        
        df_indicadores = self.calculate_indicators(ano)
        self.save_to_gold(df_indicadores, f"indicadores_completos/ano={ano}")
        
        df_aprovadas = self.apply_graham_filters(df_indicadores)
        self.save_to_gold(df_aprovadas, f"carteira_graham/ano={ano}")
        
        logger.info(f"✅ {ano} concluído!")
        return df_aprovadas


if __name__ == "__main__":
    calculator = GrahamIndicatorCalculator()
    
    # Testar com 2020 primeiro
    df_2020 = calculator.process_year(2020)
    
    print("\n" + "="*80)
    print("CARTEIRA 2020 - EMPRESAS APROVADAS")
    print("="*80)
    df_2020.select(
        "DENOM_CIA", "P_L", "P_VPA", "DIVIDEND_YIELD", "ROE", "LIQUIDEZ_CORRENTE"
    ).show(20, truncate=False)