# silver_quality.py
"""
Validação de qualidade de dados + definição das Métricas Prometheus

RESPONSABILIDADE DESTE ARQUIVO:
  - Definir as métricas (Gauges, Counters, Histograms)
  - Validar os dados e popular essas métricas

IMPORTANTE — O QUE ESTE ARQUIVO NÃO FAZ MAIS:
  - Ele NÃO importa silver.py
  - Ele NÃO inicia o servidor HTTP do Prometheus
  - Ele NÃO lê dados do MinIO diretamente

Quem faz isso agora é o run_metrics.py, que age como "maestro":
importa silver.py e silver_quality.py separadamente, sem criar
o ciclo A→B→A que causava o ImportError.
"""

# Funções do PySpark para calcular métricas nos dados
from pyspark.sql.functions import col, count, when, isnan, isnull, max, min, avg, lit, sum as spark_sum

# Tipos de métricas do Prometheus:
#   Gauge   → valor que sobe e desce (ex: total de linhas)
#   Counter → valor que só cresce (ex: total de erros)
#   Histogram → distribuição de valores (ex: tempo de processamento)
from prometheus_client import Gauge, Counter, Histogram

# Biblioteca padrão de logs
import logging

# Tipo auxiliar para anotar o retorno de funções
from typing import Dict, Any

# Biblioteca padrão para lidar com datas (usada na validação de datas futuras)
from datetime import date

# Canal de log específico para este arquivo
logger = logging.getLogger(__name__)


# ============================================================================
# DEFINIÇÃO DAS MÉTRICAS PROMETHEUS
# ============================================================================
#
# As métricas são definidas aqui UMA VEZ, quando o arquivo é importado.
# Elas ficam "registradas" na memória, mas sem valor até alguém chamar
# .set() ou .inc() nelas — o que acontece durante validate_and_report_metrics().
#
# Labels são como "filtros": permitem ver a métrica separada por csv_type e ano.
# Exemplo: silver_rows_total{csv_type="DRE_con", ano="2023"} = 45000

# Total de linhas processadas por tabela e ano
silver_rows_total = Gauge(
    'silver_rows_total',
    'Total de linhas na camada Silver',
    ['csv_type', 'ano']  # labels: permite filtrar por tabela e ano no Prometheus
)

# Número de empresas distintas por tabela e ano
silver_companies_unique = Gauge(
    'silver_companies_unique',
    'Número de empresas únicas',
    ['csv_type', 'ano']
)

# Percentual de valores nulos por coluna (útil para detectar dados ruins)
silver_null_percentage = Gauge(
    'silver_null_percentage',
    'Percentual de valores nulos por coluna',
    ['csv_type', 'ano', 'column']  # label extra: qual coluna está nula
)

# Contador de erros de validação (só cresce, nunca diminui)
silver_validation_errors = Counter(
    'silver_validation_errors_total',
    'Total de erros de validação',
    ['csv_type', 'ano', 'error_type']  # label extra: tipo do erro
)

# Histograma do tempo de processamento (em segundos)
silver_processing_time = Histogram(
    'silver_processing_seconds',
    'Tempo de processamento em segundos',
    ['csv_type', 'ano']
)


# ============================================================================
# CLASSE VALIDADORA
# ============================================================================

class SilverQualityValidator:
    """
    Valida a qualidade dos dados da camada Silver e popula as métricas Prometheus.

    Uso:
        validator = SilverQualityValidator(spark)
        validator.validate_and_report_metrics(df, "DRE_con", "2023")
    """

    def __init__(self, spark):
        # Recebe a sessão Spark já criada por quem instancia esta classe
        # (evita abrir múltiplas sessões Spark desnecessariamente)
        self.spark = spark

    def validate_and_report_metrics(self, df, csv_key: str, ano: str) -> Dict[str, Any]:
        """
        Executa todas as validações no DataFrame e atualiza as métricas Prometheus.

        Args:
            df:      DataFrame Spark com os dados da camada Silver
            csv_key: Nome da tabela (ex: "DRE_con", "BPA_con")
            ano:     Ano de referência (ex: "2023")

        Returns:
            Dicionário com os resultados de todas as validações
        """

        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 Validação de Qualidade: {csv_key} - {ano}")
        logger.info(f"{'='*60}")

        # Estrutura que vai acumular todos os resultados desta validação
        metrics = {
            "csv_key": csv_key,
            "ano": ano,
            "total_rows": df.count(),        # conta quantas linhas tem o DataFrame
            "total_columns": len(df.columns), # conta quantas colunas
            "validations": {}
        }

        # ====================================================================
        # 1. COLUNAS OBRIGATÓRIAS
        # Verifica se todas as colunas essenciais estão presentes no DataFrame
        # ====================================================================
        required_cols = {
            "DRE_con":      ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "DT_REFER", "DS_CONTA"],
            "BPA_con":      ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "CD_CONTA", "DS_CONTA"],
            "BPP_con":      ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "CD_CONTA", "DS_CONTA"],
            "DFC_DMPL_con": ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "COLUNA_DF"]
        }
        #Verifica se a tabela passada em argumento está em required_cols, para só então validar as colunas obrigatórias. Isso evita erros de chave caso o csv_key seja diferente do esperado.
        if csv_key in required_cols:
            # Lista as colunas que deveriam existir mas não existem
            missing = [col for col in required_cols[csv_key] if col not in df.columns]
            metrics["validations"]["missing_columns"] = missing

            if missing:
                logger.error(f"❌ Colunas obrigatórias faltando: {missing}")
                # Incrementa o contador de erros no Prometheus
                silver_validation_errors.labels(
                    csv_type=csv_key,
                    ano=ano,
                    error_type='missing_columns'
                ).inc(len(missing))
                # Retorna sem continuar as outras validações (dado inválido)
                return metrics
            else:
                logger.info("✅ Todas as colunas obrigatórias presentes")

        # ====================================================================
        # 2. TOTAL DE REGISTROS
        # Envia o total de linhas para o Prometheus
        # ====================================================================
        silver_rows_total.labels(csv_type=csv_key, ano=ano).set(metrics["total_rows"])
        logger.info(f"📊 Total de registros: {metrics['total_rows']:,}")

        # ====================================================================
        # 3. EMPRESAS ÚNICAS
        # Conta quantas empresas diferentes aparecem nos dados
        # ====================================================================
        if "CNPJ_CIA" in df.columns:
            unique_companies = df.select("CNPJ_CIA").distinct().count()
            metrics["validations"]["unique_companies"] = unique_companies
            silver_companies_unique.labels(csv_type=csv_key, ano=ano).set(unique_companies)
            logger.info(f"🏢 Empresas únicas: {unique_companies:,}")

        # ====================================================================
        # 4. PERCENTUAL DE NULOS POR COLUNA
        # Para cada coluna, calcula quantos % dos valores são nulos
        # ====================================================================
        null_percentages = {}
        high_null_columns = []

        for col_name in df.columns:
            # Conta quantas linhas têm valor nulo nesta coluna
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / metrics["total_rows"]) * 100
            null_percentages[col_name] = round(null_pct, 2)

            # Envia o percentual de nulos desta coluna ao Prometheus
            silver_null_percentage.labels(
                csv_type=csv_key,
                ano=ano,
                column=col_name
            ).set(null_pct)

            # Se mais de 50% dos valores são nulos, é um problema grave
            if null_pct > 50:
                logger.warning(f"⚠️  {col_name}: {null_pct:.1f}% nulos")
                high_null_columns.append(col_name)
                silver_validation_errors.labels(
                    csv_type=csv_key,
                    ano=ano,
                    error_type='high_null_percentage'
                ).inc()

        metrics["validations"]["null_percentages"] = null_percentages
        metrics["validations"]["high_null_columns"] = high_null_columns

        # ====================================================================
        # 5. VALORES MONETÁRIOS INVÁLIDOS
        # Verifica se a coluna de valores financeiros tem nulos ou NaN
        # ====================================================================
        if "VL_CONTA" in df.columns:
            invalid_values = df.filter(
                col("VL_CONTA").isNull() | isnan(col("VL_CONTA"))
            ).count()

            invalid_pct = (invalid_values / metrics["total_rows"]) * 100
            metrics["validations"]["invalid_monetary_values"] = invalid_values
            metrics["validations"]["invalid_monetary_pct"] = round(invalid_pct, 2)

            if invalid_values > 0:
                logger.warning(f"⚠️  Valores monetários inválidos: {invalid_values:,} ({invalid_pct:.1f}%)")
                silver_validation_errors.labels(
                    csv_type=csv_key,
                    ano=ano,
                    error_type='invalid_monetary_values'
                ).inc(invalid_values)
            else:
                logger.info("✅ Todos os valores monetários são válidos")

        # ====================================================================
        # 6. DATAS NO FUTURO
        # Datas posteriores a hoje indicam erro de digitação ou dado corrompido
        # ====================================================================
        if "DT_REFER" in df.columns:
            

            future_dates = df.filter(col("DT_REFER") > lit(date.today())).count()
            metrics["validations"]["future_dates"] = future_dates

            if future_dates > 0:
                logger.warning(f"⚠️  Datas no futuro: {future_dates:,}")
                silver_validation_errors.labels(
                    csv_type=csv_key,
                    ano=ano,
                    error_type='future_dates'
                ).inc(future_dates)
            else:
                logger.info("✅ Todas as datas são válidas")

        # ====================================================================
        # 7. DUPLICATAS
        # Registros com a mesma chave (empresa + conta + data) duplicados
        # ====================================================================
        if csv_key in required_cols:
            key_cols = ["CNPJ_CIA", "CD_CONTA", "DT_REFER"] if "CD_CONTA" in df.columns else ["CNPJ_CIA", "DT_REFER"]
            key_cols = [c for c in key_cols if c in df.columns]

            if key_cols:
                # Conta quantos grupos de registros têm mais de 1 ocorrência
                duplicates = df.groupBy(key_cols).count().filter(col("count") > 1).count()
                metrics["validations"]["duplicates"] = duplicates

                if duplicates > 0:
                    logger.warning(f"⚠️  Registros duplicados: {duplicates:,}")
                    silver_validation_errors.labels(
                        csv_type=csv_key,
                        ano=ano,
                        error_type='duplicates'
                    ).inc(duplicates)
                else:
                    logger.info("✅ Nenhuma duplicata encontrada")

        # ====================================================================
        # 8. DISTRIBUIÇÃO DE VALORES MONETÁRIOS
        # Estatísticas descritivas da coluna VL_CONTA
        # ====================================================================
        if "VL_CONTA" in df.columns:
            stats = df.select(
                spark_sum("VL_CONTA").alias("total"),
                count("VL_CONTA").alias("count"),
                avg("VL_CONTA").alias("avg"),
                min("VL_CONTA").alias("min"),
                max("VL_CONTA").alias("max")
            ).collect()[0]

            metrics["validations"]["value_distribution"] = {
                "total": float(stats["total"]) if stats["total"] else 0,
                "count": stats["count"],
                "avg":   float(stats["avg"])   if stats["avg"]   else 0,
                "min":   float(stats["min"])   if stats["min"]   else 0,
                "max":   float(stats["max"])   if stats["max"]   else 0
            }

            logger.info("💰 Distribuição de valores:")
            logger.info(f"   Total: R$ {stats['total']:,.2f}" if stats["total"] else "   Total: N/A")
            logger.info(f"   Média: R$ {stats['avg']:,.2f}"   if stats["avg"]   else "   Média: N/A")
            logger.info(f"   Min: R$ {stats['min']:,.2f}"     if stats["min"]   else "   Min: N/A")
            logger.info(f"   Max: R$ {stats['max']:,.2f}"     if stats["max"]   else "   Max: N/A")

        # ====================================================================
        # 9. RESUMO FINAL
        # ====================================================================
        self._print_summary(metrics)

        return metrics

    def _print_summary(self, metrics: Dict[str, Any]):
        """Imprime um resumo consolidado no terminal após todas as validações"""

        logger.info(f"\n{'='*60}")
        logger.info(f"📋 RESUMO DA VALIDAÇÃO")
        logger.info(f"{'='*60}")
        logger.info(f"CSV: {metrics['csv_key']}")
        logger.info(f"Ano: {metrics['ano']}")
        logger.info(f"Total de registros: {metrics['total_rows']:,}")
        logger.info(f"Total de colunas: {metrics['total_columns']}")

        validations = metrics["validations"]

        if "unique_companies" in validations:
            logger.info(f"Empresas únicas: {validations['unique_companies']:,}")
        if "invalid_monetary_values" in validations:
            logger.info(f"Valores inválidos: {validations['invalid_monetary_values']:,}")
        if "duplicates" in validations:
            logger.info(f"Duplicatas: {validations['duplicates']:,}")
        if "future_dates" in validations:
            logger.info(f"Datas futuras: {validations['future_dates']:,}")
        if "high_null_columns" in validations and validations["high_null_columns"]:
            logger.info(f"Colunas com >50% nulos: {len(validations['high_null_columns'])}")

        logger.info(f"{'='*60}\n")