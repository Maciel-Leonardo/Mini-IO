# gold_dimensional.py
"""
Camada Gold - Modelo Dimensional para Power BI (Star Schema)

FILOSOFIA:
- Gold NÃO calcula indicadores (isso fica no Power BI com DAX)
- Gold ESTRUTURA dados em modelo dimensional (fatos + dimensões)
- Resultado: Flexibilidade total para análises

MODELO:
- 4 Dimensões: DIM_EMPRESA, DIM_CONTA, DIM_TEMPO, DIM_ESPECIE_ACAO
- 5 Fatos: FATO_BALANCO, FATO_DRE, FATO_COTACAO, FATO_DIVIDENDOS, FATO_COMPOSICAO_CAPITAL

REDUÇÃO vs versão anterior:
- 1.172 linhas → 200 linhas (83% menor!)
- Sem cálculos complexos
- Apenas ETL simples
"""

from silver import CVMSilverProcessor, clean_cnpj
from pyspark.sql.functions import (
    col, lit, coalesce, when, concat, 
    lower, trim, regexp_replace,
    year, quarter, month, dayofmonth,
    to_date, last_day, dense_rank, split, size
)
from pyspark.sql.window import Window
import logging
from datetime import datetime, timedelta, date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoldDimensionalProcessor:
    """
    Processa dados Silver → Gold em modelo dimensional (Star Schema)
    
    Responsabilidade: estruturar dados, NÃO calcular indicadores
    """
    
    def __init__(self):
        """Inicializa processador conectando ao Silver"""
        logger.info("🔧 Inicializando Gold Dimensional Processor...")
        
        self.processor = CVMSilverProcessor()
        self.spark = self.processor.spark
        self.minio_config = self.processor.minio_config
        
        logger.info("✅ Inicializado")
    
    def save_dimension(self, df, dim_name: str):
        """
        Salva tabela dimensão (sem particionamento por ano)
        
        Dimensões são relativamente pequenas e estáticas
        """
        path = f"s3a://{self.minio_config.bucket_gold}/dimensoes/{dim_name}"
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(path)
        
        logger.info(f"✅ Dimensão salva: {dim_name} ({df.count():,} registros)")
    
    def save_fact(self, df, fact_name: str, ano: int):
        """
        Salva tabela fato (particionada por ano)
        
        Fatos crescem com o tempo, particionamento melhora performance
        """
        path = f"s3a://{self.minio_config.bucket_gold}/fatos/{fact_name}"
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"ano_referencia = {ano}") \
            .partitionBy("ano_referencia") \
            .save(path)
        
        logger.info(f"✅ Fato salvo: {fact_name}/ano={ano} ({df.count():,} registros)")
    
    # ========================================================================
    # DIMENSÕES
    # ========================================================================
    
    def create_dim_empresa(self, ano: int):
        """
        DIM_EMPRESA: Cadastro de empresas com respectivos setores
        
        Fonte: cad_cia_aberta.csv (CVM) - tabela de cadastro de companhias abertas
        Granularidade: 1 linha por CNPJ
        """
        logger.info("📊 Criando DIM_EMPRESA...")
        cad_cia_aberta = self.processor.read_silver_table("cad_cia_aberta")
        
        
        # Selecionar colunas únicas de empresa
        dim_empresa = cad_cia_aberta.select(
            col("CNPJ").alias("cnpj"),
            col("SETOR_ATIV").alias("setor"),
            col("SIT").alias("situacao")
        )
        
        # Adicionar nome normalizado (para joins futuros)
        dim_empresa = dim_empresa.withColumn(
            "nome_normalizado",
            lower(trim(regexp_replace(col("nome_empresa"), r"\s+", " ")))
        )
        
        # Adicionar metadados
        dim_empresa = dim_empresa.withColumn("data_atualizacao", lit(datetime.now())) \
            .withColumn("tipo_demonstracao", lit("composicao_capital"))
        
        return dim_empresa
    
    def create_dim_conta(self, ano: int):
        """
        DIM_CONTA: Plano de contas CVM
        
        Fonte: Todas as tabelas DFP (união de contas de BP, DRE, DFC)
        Granularidade: 1 linha por CD_CONTA
        """
        logger.info("📊 Criando DIM_CONTA...")
        
        # Ler todas as demonstrações
        bpp = self.processor.read_silver_table("BPP_con", ano)
        bpa = self.processor.read_silver_table("BPA_con", ano)
        dre = self.processor.read_silver_table("DRE_con", ano)
        
        
        # Selecionar contas de cada demonstração
        contas_bpp = bpp.select(
            col("CD_CONTA").alias("cd_conta"),
            col("DS_CONTA").alias("ds_conta"),
            lit("BPP").alias("tipo_demonstracao")
        ).distinct()
        
        contas_bpa = bpa.select(
            col("CD_CONTA").alias("cd_conta"),
            col("DS_CONTA").alias("ds_conta"),
            lit("BPA").alias("tipo_demonstracao")
        ).distinct()
        
        contas_dre = dre.select(
            col("CD_CONTA").alias("cd_conta"),
            col("DS_CONTA").alias("ds_conta"),
            lit("DRE").alias("tipo_demonstracao")
        ).distinct()
        
        

        # Unir todas as contas
        dim_conta = contas_bpp.union(contas_bpa).union(contas_dre).distinct()
        
        # Calcular nível hierárquico (quantidade de pontos + 1)
        # Ex: "2.07" = 2, "2.07.01" = 3
        dim_conta = dim_conta.withColumn(
            "nivel",
            size(split(col("cd_conta"), r"\."))
        )
        
        return dim_conta
    
    def create_dim_tempo(self):
        """
        DIM_TEMPO: Calendário de datas
        
        Fonte: Gerada programaticamente (2020-2030)
        Granularidade: 1 linha por data
        """
        logger.info("📊 Criando DIM_TEMPO...")
        
        # Gerar range de datas (2020-hoje)
        start_date = date(2019, 1, 1)
        end_date = date(2030, 12, 31)
        
        # Criar lista de datas
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append((current_date,))
            current_date += timedelta(days=1)
        
        # Criar DataFrame
        dim_tempo = self.spark.createDataFrame(dates, ["data"])
        
        # Adicionar atributos temporais
        dim_tempo = dim_tempo \
            .withColumn("ano", year("data")) \
            .withColumn("mes", month("data")) \
            .withColumn("trimestre", quarter("data")) \
            .withColumn("semestre", when(quarter("data") <= 2, 1).otherwise(2)) \
            .withColumn("dia", dayofmonth("data")) \
            .withColumn("ano_trimestre", concat(col("ano"), lit("-Q"), col("trimestre"))) \
            .withColumn("ano_mes", concat(col("ano"), lit("-"), 
                when(col("mes") < 10, concat(lit("0"), col("mes")))
                .otherwise(col("mes")))) \
            .withColumn("eh_fim_trimestre", 
                (col("mes").isin([3, 6, 9, 12])) & (col("data") == last_day("data"))) \
            .withColumn("eh_fim_ano",
                (col("mes") == 12) & (col("data") == last_day("data")))
        
        return dim_tempo
    
    def create_dim_especie_acao(self,ano: int):
        """
        DIM_ESPECIE_ACAO: Tipos de ações (ON, PN, PNA, etc)
        
        Fonte: Hardcoded (dados estáticos conhecidos)
        Granularidade: 1 linha por espécie
        """
        logger.info("📊 Criando DIM_ESPECIE_ACAO...")

        # Ler FCA para pegar espécies de ações cadastradas (simplificação)
        fca = self.processor.read_silver_table("cad_valor_mobiliario",ano)
        dim_especie = fca.select(
            col("CNPJ").alias("cnpj"),
            col("Valor_Mobiliario").alias("especie_acao"),
            col("Sigla_Classe_Acao_Preferencial").alias("sigla_classe_acao"),
            col("Classe_Acao_Preferencial").alias("classe_acao"),
            col("Sigla_Classe_Acao_Preferencial").alias("sigla_classe_acao"),
            col("Classe_Acao_Preferencial").alias("classe_acao"),
            col("Codigo_Negociacao").alias("codigo_negociacao"),
            lit(ano).alias("ano_referencia")
        ).distinct()
        
        return dim_especie
    
    # ========================================================================
    # FATOS
    # ========================================================================
    
    def create_fato_balanco(self, ano: int):
        """
        FATO_BALANCO: Valores de Ativos e Passivos
        
        Fonte: Silver BPA_con + BPP_con
        Granularidade: empresa × conta × data
        """
        logger.info("📊 Criando FATO_BALANCO...")
        
        # Ler BPA e BPP
        bpa = self.processor.read_silver_table("BPA_con", ano)
        bpp = self.processor.read_silver_table("BPP_con", ano)
        
        # Padronizar colunas BPA
        fato_bpa = bpa.select(
            col("CNPJ").alias("cnpj"),
            col("CD_CONTA").alias("cd_conta"),
            col("DT_REFER").alias("data_referencia"),
            col("VL_CONTA").alias("valor"),
            lit("BPA").alias("tipo_balanco"),
            lit(ano).alias("ano_referencia")
        )
        
        # Padronizar colunas BPP
        fato_bpp = bpp.select(
            col("CNPJ").alias("cnpj"),
            col("CD_CONTA").alias("cd_conta"),
            col("DT_REFER").alias("data_referencia"),
            col("VL_CONTA").alias("valor"),
            lit("BPP").alias("tipo_balanco"),
            lit(ano).alias("ano_referencia")
        )
        
        # Unir BPA + BPP
        fato_balanco = fato_bpa.union(fato_bpp)
        
        # Filtrar valores nulos/zerados (opcional - reduz tamanho)
        fato_balanco = fato_balanco.filter(col("valor").isNotNull())
        
        return fato_balanco
    def create_fato_dmpl(self, ano: int):
        """
        FATO_DMPL: Demonstração das Mutações do Patrimônio Líquido
        
        Fonte: Silver DMPL_con
        Granularidade: empresa × conta × data
        """
        logger.info("📊 Criando FATO_DMPL...")
        
        dmpl = self.processor.read_silver_table("DFC_DMPL_con", ano)
        
        fato_dmpl = dmpl.select(
            col("CNPJ").alias("cnpj"),
            col("COLUNA_DF").alias("coluna_df"),
            col("CD_CONTA").alias("cd_conta"),
            col("DS_CONTA").alias("ds_conta"),
            col("DT_REFER").alias("data_referencia"),
            col("VL_CONTA").alias("valor"),
        )
        
        return fato_dmpl

    def create_fato_dre(self, ano: int):
        """
        FATO_DRE: Demonstração do Resultado do Exercício
        
        Fonte: Silver DRE_con
        Granularidade: empresa × conta × data
        """
        logger.info("📊 Criando FATO_DRE...")
        
        dre = self.processor.read_silver_table("DRE_con", ano)
        
        fato_dre = dre.select(
            col("CNPJ").alias("cnpj"),
            col("CD_CONTA").alias("cd_conta"),
            col("DT_REFER").alias("data_referencia"),
            col("DT_INI_EXERC").alias("data_inicio_exercicio"),
            col("DT_FIM_EXERC").alias("data_fim_exercicio"),
            col("VL_CONTA").alias("valor"),
            lit(ano).alias("ano_referencia")
        )
        
        # Filtrar valores nulos
        fato_dre = fato_dre.filter(col("valor").isNotNull())
        
        return fato_dre
    
    def create_fato_cotacao(self, ano: int):
        """
        FATO_COTACAO: Preços das ações (dados trimestrais)
        
        Fonte: Silver volume_valor_mobiliario (FRE)
        Granularidade: empresa × espécie × data
        """
        logger.info("📊 Criando FATO_COTACAO...")
        
        precos = self.processor.read_silver_table("volume_valor_mobiliario", ano)
        
        # Mapear Nome_Companhia para CNPJ via DIM_EMPRESA
        # (simplificação: assumir que nome bate)
        dim_empresa = self.create_dim_empresa(ano)
        
        fato_cotacao = precos.join(
            dim_empresa,
            precos.CNPJ == dim_empresa.cnpj,
            "inner"
        )
        
        fato_cotacao = fato_cotacao.select(
            dim_empresa.cnpj,
            col("Especie_Acao").alias("especie_acao"),
            col("Data_Fim_Trimestre").alias("data_referencia"),
            col("Valor_Cotacao_Media").alias("valor_cotacao_media"),
            col("Valor_Maior_Cotacao").alias("valor_maior_cotacao"),
            col("Valor_Menor_Cotacao").alias("valor_menor_cotacao"),
            col("Valor_Volume_Negociado").alias("volume_negociado"),
            lit(ano).alias("ano_referencia")
        )
        
        # Filtrar preços nulos
        fato_cotacao = fato_cotacao.filter(col("valor_cotacao_media").isNotNull())
        
        return fato_cotacao
    
    def create_fato_dividendos(self, ano: int):
        """
        FATO_DIVIDENDOS: Dividendos e JCP distribuídos
        
        Fonte: Silver distribuicao_dividendos (FRE)
        Granularidade: empresa × data
        """
        logger.info("📊 Criando FATO_DIVIDENDOS...")
        
        dividendos = self.processor.read_silver_table("distribuicao_dividendos", ano)
        
        # Mapear Nome_Companhia para CNPJ
        dim_empresa = self.create_dim_empresa(ano)
        
        fato_dividendos = dividendos.join(
            dim_empresa,
            dividendos.CNPJ == dim_empresa.cnpj,
            "inner"
        )
        
        fato_dividendos = fato_dividendos.select(
            dim_empresa.cnpj,
            col("Data_Fim_Exercicio_Social").alias("data_exercicio_social"),
            col("Dividendo_Distribuido_Total").alias("dividendo_total"),
            col("Lucro_Liquido_Ajustado").alias("lucro_liquido_ajustado"),
            lit("Dividendo").alias("tipo_provento"),  # Simplificação
            lit(ano).alias("ano_referencia")
        )
        
        return fato_dividendos
    
    def create_fato_composicao_capital(self, ano: int):
        """
        FATO_COMPOSICAO_CAPITAL: Quantidade de ações
        
        Fonte: Silver composicao_capital (DFP)
        Granularidade: empresa × data
        """
        logger.info("📊 Criando FATO_COMPOSICAO_CAPITAL...")
        
        acoes = self.processor.read_silver_table("composicao_capital", ano)
        
        fato_acoes = acoes.select(
            col("CNPJ").alias("cnpj"),
            col("DT_REFER").alias("data_referencia"),
            col("QT_ACAO_TOTAL_CAP_INTEGR").alias("qt_acao_capital_integralizado"),
            coalesce(col("QT_ACAO_TOTAL_TESOURO"), lit(0)).alias("qt_acao_tesouraria"),
            lit(ano).alias("ano_referencia")
        )
        
        # Calcular ações em circulação
        fato_acoes = fato_acoes.withColumn(
            "qt_acao_circulacao",
            col("qt_acao_capital_integralizado") - col("qt_acao_tesouraria")
        )
        
        return fato_acoes
    
    # ========================================================================
    # MÉTODO PRINCIPAL
    # ========================================================================
    
    def process_year(self, ano: int):
        """
        Processa ano completo: cria dimensões + fatos
        
        Fluxo:
        1. Criar dimensões (executar apenas 1x para todos os anos)
        2. Criar fatos (1x por ano)
        3. Salvar tudo em Delta Lake
        
        Args:
            ano: Ano a processar (ex: 2024)
        """
        logger.info("\n" + "="*70)
        logger.info(f"🚀 PROCESSANDO ANO {ano} - MODELO DIMENSIONAL")
        logger.info("="*70)
        
        try:
            # ════════════════════════════════════════════════════════════════
            # ETAPA 1: CRIAR DIMENSÕES (executar apenas para primeiro ano)
            # ════════════════════════════════════════════════════════════════
            
            # Verificar se dimensões já existem
            # (simplificação: sempre recriar - em produção, verificar antes)
            
            logger.info("\n📊 [1/2] Criando dimensões...")
            
            dim_empresa = self.create_dim_empresa(ano)
            self.save_dimension(dim_empresa, "DIM_EMPRESA")
            
            dim_conta = self.create_dim_conta(ano)
            self.save_dimension(dim_conta, "DIM_CONTA")
            
            dim_tempo = self.create_dim_tempo()
            self.save_dimension(dim_tempo, "DIM_TEMPO")
            
            dim_especie = self.create_dim_especie_acao(ano)
            self.save_dimension(dim_especie, "DIM_ESPECIE_ACAO")
            
            logger.info("✅ Todas as dimensões criadas!")
            
            # ════════════════════════════════════════════════════════════════
            # ETAPA 2: CRIAR FATOS
            # ════════════════════════════════════════════════════════════════
            logger.info("\n📊 [2/2] Criando fatos...")
            
            fato_dmpl = self.create_fato_dmpl(ano)
            self.save_fact(fato_dmpl, "FATO_DMPL", ano)

            fato_balanco = self.create_fato_balanco(ano)
            self.save_fact(fato_balanco, "FATO_BALANCO", ano)
            
            fato_dre = self.create_fato_dre(ano)
            self.save_fact(fato_dre, "FATO_DRE", ano)
            
            fato_cotacao = self.create_fato_cotacao(ano)
            self.save_fact(fato_cotacao, "FATO_COTACAO", ano)
            
            fato_dividendos = self.create_fato_dividendos(ano)
            self.save_fact(fato_dividendos, "FATO_DIVIDENDOS", ano)
            
            fato_acoes = self.create_fato_composicao_capital(ano)
            self.save_fact(fato_acoes, "FATO_COMPOSICAO_CAPITAL", ano)
            
            logger.info("✅ Todos os fatos criados!")
            
            # ════════════════════════════════════════════════════════════════
            # RESUMO FINAL
            # ════════════════════════════════════════════════════════════════
            logger.info("\n" + "="*70)
            logger.info(f"✅ PROCESSAMENTO {ano} CONCLUÍDO!")
            logger.info("="*70)
            logger.info("\n📊 Modelo Star Schema criado:")
            logger.info("   Dimensões:")
            logger.info("   - DIM_EMPRESA")
            logger.info("   - DIM_CONTA")
            logger.info("   - DIM_TEMPO")
            logger.info("   - DIM_ESPECIE_ACAO")
            logger.info(f"\n   Fatos (ano={ano}):")
            logger.info("   - FATO_BALANCO")
            logger.info("   - FATO_DRE")
            logger.info("   - FATO_COTACAO")
            logger.info("   - FATO_DIVIDENDOS")
            logger.info("   - FATO_COMPOSICAO_CAPITAL")
            logger.info(f"\n💾 Bucket: s3a://{self.minio_config.bucket_gold}/")
            logger.info("="*70)
            
        except Exception as e:
            logger.error(f"❌ Erro no processamento {ano}: {e}")
            import traceback
            traceback.print_exc()
            raise


# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    """
    Executa processamento para todos os anos disponíveis
    """
    
    logger.info("="*70)
    logger.info("🚀 INICIANDO PROCESSAMENTO GOLD - MODELO DIMENSIONAL")
    logger.info("="*70)
    logger.info("\nFilosofia:")
    logger.info("- Gold estrutura dados (Star Schema)")
    logger.info("- Power BI calcula indicadores (DAX)")
    logger.info("="*70)
    
    # Criar processador
    processor = GoldDimensionalProcessor()
    
    # Anos a processar
    anos = [2020, 2021, 2022, 2023, 2024]
    
    # Processar cada ano
    for ano in anos:
        try:
            processor.process_year(ano)
        except Exception as e:
            logger.error(f"❌ Falha no processamento de {ano}: {e}")
            continue
    
    logger.info("\n" + "="*70)
    logger.info("🎉 PROCESSAMENTO GOLD FINALIZADO!")
    logger.info("="*70)
    logger.info("\n📊 Próximo passo:")
    logger.info("   1. Conectar Power BI ao MinIO/Delta Lake")
    logger.info("   2. Criar relacionamentos no modelo")
    logger.info("   3. Construir medidas DAX (indicadores Graham)")
    logger.info("   4. Desenvolver dashboards")
    # Script de exportação (export_to_parquet.py)

    processor = GoldDimensionalProcessor()
    spark = processor.spark

    # Dimensões
    for dim in ["DIM_EMPRESA", "DIM_CONTA", "DIM_TEMPO", "DIM_ESPECIE_ACAO"]:
        df = spark.read.format("delta").load(f"s3a://gold/dimensoes/{dim}")
        df.coalesce(1).write.mode("overwrite").parquet(f"/data/{dim}.parquet")

    # Fatos (todas as partições de uma vez)
    for fato in ["FATO_BALANCO", "FATO_DRE", "FATO_COTACAO", "FATO_DIVIDENDOS", "FATO_COMPOSICAO_CAPITAL"]:
        df = spark.read.format("delta").load(f"s3a://gold/fatos/{fato}")
        df.coalesce(1).write.mode("overwrite").parquet(f"/data/{fato}.parquet")
