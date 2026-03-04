# silver_quality.py
"""
Validação de qualidade de dados + Métricas Prometheus
"""

from pyspark.sql.functions import col, count, when, isnan, isnull, max, min, avg, sum as spark_sum
from prometheus_client import Gauge, Counter, Histogram, generate_latest
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# ============================================================================
# MÉTRICAS PROMETHEUS
# ============================================================================

# Gauges (valores instantâneos)
silver_rows_total = Gauge(
    'silver_rows_total', 
    'Total de linhas na camada Silver',
    ['csv_type', 'ano']
)

silver_companies_unique = Gauge(
    'silver_companies_unique',
    'Número de empresas únicas',
    ['csv_type', 'ano']
)

silver_null_percentage = Gauge(
    'silver_null_percentage',
    'Percentual de valores nulos por coluna',
    ['csv_type', 'ano', 'column']
)

# Counters (valores acumulativos)
silver_validation_errors = Counter(
    'silver_validation_errors_total',
    'Total de erros de validação',
    ['csv_type', 'ano', 'error_type']
)

# Histograms (distribuições)
silver_processing_time = Histogram(
    'silver_processing_seconds',
    'Tempo de processamento em segundos',
    ['csv_type', 'ano']
)


class SilverQualityValidator:
    """Validador de qualidade de dados para camada Silver"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def validate_and_report_metrics(self, df, csv_key: str, ano: str) -> Dict[str, Any]:
        """
        Executa validações E atualiza métricas Prometheus
        
        Returns:
            Dict com métricas de qualidade
        """
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 Validação de Qualidade: {csv_key} - {ano}")
        logger.info(f"{'='*60}")
        
        metrics = {
            "csv_key": csv_key,
            "ano": ano,
            "total_rows": df.count(),
            "total_columns": len(df.columns),
            "validations": {}
        }
        
        # ====================================================================
        # 1. COLUNAS OBRIGATÓRIAS
        # ====================================================================
        required_cols = {
            "DRE_con": ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "DT_REFER", "DS_CONTA"],
            "BPA_con": ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "CD_CONTA", "DS_CONTA"],
            "BPP_con": ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "CD_CONTA", "DS_CONTA"],
            "DFC_DMPL_con": ["CNPJ_CIA", "DENOM_CIA", "VL_CONTA", "COLUNA_DF"]
        }
        
        if csv_key in required_cols:
            missing = [col for col in required_cols[csv_key] if col not in df.columns]
            metrics["validations"]["missing_columns"] = missing
            
            if missing:
                logger.error(f"❌ Colunas obrigatórias faltando: {missing}")
                silver_validation_errors.labels(
                    csv_type=csv_key, 
                    ano=ano, 
                    error_type='missing_columns'
                ).inc(len(missing))
                return metrics
            else:
                logger.info("✅ Todas as colunas obrigatórias presentes")
        
        # ====================================================================
        # 2. TOTAL DE REGISTROS (atualizar Prometheus)
        # ====================================================================
        silver_rows_total.labels(csv_type=csv_key, ano=ano).set(metrics["total_rows"])
        logger.info(f"📊 Total de registros: {metrics['total_rows']:,}")
        
        # ====================================================================
        # 3. EMPRESAS ÚNICAS
        # ====================================================================
        if "CNPJ_CIA" in df.columns:
            unique_companies = df.select("CNPJ_CIA").distinct().count()
            metrics["validations"]["unique_companies"] = unique_companies
            
            # Atualizar Prometheus
            silver_companies_unique.labels(csv_type=csv_key, ano=ano).set(unique_companies)
            
            logger.info(f"🏢 Empresas únicas: {unique_companies:,}")
        
        # ====================================================================
        # 4. PERCENTUAL DE NULOS POR COLUNA
        # ====================================================================
        null_percentages = {}
        high_null_columns = []
        
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / metrics["total_rows"]) * 100
            null_percentages[col_name] = round(null_pct, 2)
            
            # Atualizar Prometheus
            silver_null_percentage.labels(
                csv_type=csv_key, 
                ano=ano, 
                column=col_name
            ).set(null_pct)
            
            # Alertar se > 50% nulos
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
        # ====================================================================
        if "DT_REFER" in df.columns:
            from datetime import date
            from pyspark.sql.functions import lit
            
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
        # ====================================================================
        if csv_key in required_cols:
            # Definir chave única baseada nas colunas obrigatórias
            key_cols = ["CNPJ_CIA", "CD_CONTA", "DT_REFER"] if "CD_CONTA" in df.columns else ["CNPJ_CIA", "DT_REFER"]
            key_cols = [c for c in key_cols if c in df.columns]
            
            if key_cols:
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
        # 8. DISTRIBUIÇÃO DE VALORES
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
                "avg": float(stats["avg"]) if stats["avg"] else 0,
                "min": float(stats["min"]) if stats["min"] else 0,
                "max": float(stats["max"]) if stats["max"] else 0
            }
            
            logger.info(f"💰 Distribuição de valores:")
            logger.info(f"   Total: R$ {stats['total']:,.2f}" if stats["total"] else "   Total: N/A")
            logger.info(f"   Média: R$ {stats['avg']:,.2f}" if stats["avg"] else "   Média: N/A")
            logger.info(f"   Min: R$ {stats['min']:,.2f}" if stats["min"] else "   Min: N/A")
            logger.info(f"   Max: R$ {stats['max']:,.2f}" if stats["max"] else "   Max: N/A")
        
        # ====================================================================
        # 9. RESUMO FINAL
        # ====================================================================
        self._print_summary(metrics)
        
        return metrics
    
    def _print_summary(self, metrics: Dict[str, Any]):
        """Imprime resumo da validação"""
        
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


# ============================================================================
# FUNÇÃO PARA EXPOR MÉTRICAS PROMETHEUS
# ============================================================================

def export_prometheus_metrics(port: int = 8000):
    """
    Inicia servidor HTTP para expor métricas Prometheus
    
    Args:
        port: Porta para expor métricas (padrão: 8000)
    """
    from prometheus_client import start_http_server
    import time
    
    logger.info(f"📊 Iniciando servidor de métricas Prometheus na porta {port}")
    start_http_server(port)
    
    logger.info(f"✅ Métricas disponíveis em: http://localhost:{port}/metrics")
    
    # Manter servidor rodando
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Servidor de métricas encerrado")


if __name__ == "__main__":
    # Exemplo de uso
    from silver import CVMSilverProcessor
    
    processor = CVMSilverProcessor()
    validator = SilverQualityValidator(processor.spark)
    
    # Ler dados
    df = processor.read_silver_table("DRE_con", ano=2023)
    
    # Validar e gerar métricas
    metrics = validator.validate_and_report_metrics(df, "DRE_con", "2023")
    
    # Exportar métricas Prometheus (em background)
    import threading
    metrics_thread = threading.Thread(target=export_prometheus_metrics, daemon=True)
    metrics_thread.start()
    
    print("\n✅ Métricas disponíveis em http://localhost:8000/metrics")
    print("Pressione Ctrl+C para encerrar...")
    
    import time
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass