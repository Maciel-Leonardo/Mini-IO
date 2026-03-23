# run_metrics.py
#
# Este arquivo é o "maestro" do sistema de métricas.
# Ele existe para resolver um problema chamado "importação circular":
#
#   silver.py        → precisa de silver_quality.py (para validar dados)
#   silver_quality.py → precisaria de silver.py (para ler os dados)
#
# Isso criava um ciclo impossível: A depende de B que depende de A.
# A solução é criar um terceiro arquivo (este) que importa os dois
# separadamente, sem que eles precisem se importar entre si.
#
# FLUXO OTIMIZADO:
#   1. Sobe o servidor HTTP para o Prometheus coletar métricas
#   2. Conecta ao Spark e ao MinIO via CVMSilverProcessor
#   3. Lê os dados Silver UMA VEZ e atualiza as métricas
#   4. Mantém o servidor vivo (sem polling) para o Prometheus acessar

# ----------------------------------------------------------------------
# IMPORTAÇÕES
# ----------------------------------------------------------------------

# Biblioteca do Prometheus: inicia o servidor HTTP que expõe as métricas
from prometheus_client import start_http_server

# CVMSilverProcessor: classe que conecta ao Spark + MinIO e lê os dados
# Vem do silver.py — responsável por processar os dados da CVM
from silver import CVMSilverProcessor

# SilverQualityValidator: classe que valida os dados e popula as métricas
# Vem do silver_quality.py — responsável por calcular qualidade dos dados
from silver_quality import SilverQualityValidator

# time: biblioteca padrão do Python para usar o sleep (aguardar X segundos)
import time

# logging: biblioteca padrão para registrar mensagens no terminal
import logging

# ----------------------------------------------------------------------
# CONFIGURAÇÃO DO LOG
# ----------------------------------------------------------------------

# Configura o formato das mensagens que aparecem no terminal
logging.basicConfig(level=logging.INFO)

# Cria um "canal" de log específico para este arquivo
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# PORTA DO SERVIDOR DE MÉTRICAS
# ----------------------------------------------------------------------

# Porta onde o servidor HTTP vai rodar
# O Prometheus acessa http://ingest:8000/metrics para coletar as métricas
PORT = 8000

# ⚠️ REMOVIDO: Não precisamos mais de polling periódico
# As métricas são atualizadas UMA VEZ quando este script executa

# ----------------------------------------------------------------------
# INÍCIO DO PROGRAMA
# ----------------------------------------------------------------------

logger.info(f"📊 Subindo servidor de métricas na porta {PORT}...")

# Inicia o servidor HTTP em background (em paralelo com o resto do código)
# Após esta linha, o Prometheus já consegue acessar http://ingest:8000/metrics
# As métricas aparecerão como 0 ou vazias até o primeiro ciclo de validação
start_http_server(PORT)

logger.info(f"✅ Servidor rodando em http://localhost:{PORT}/metrics")

# ----------------------------------------------------------------------
# FUNÇÃO AUXILIAR: Inicializar conexões com retry
# ----------------------------------------------------------------------

def initialize_connections(max_retries=5, retry_delay=10):
    """Tenta conectar ao Spark e MinIO com retry em caso de falha"""
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"🔄 Tentativa {attempt}/{max_retries} - Conectando ao Spark e MinIO...")
            
            # Cria o processador: abre conexão com Spark e MinIO
            processor = CVMSilverProcessor()
            
            # Cria o validador passando a sessão Spark já aberta
            validator = SilverQualityValidator(processor.spark)
            
            logger.info("✅ Conexões estabelecidas com sucesso!")
            return processor, validator
            
        except Exception as e:
            logger.error(f"❌ Tentativa {attempt} falhou: {e}")
            if attempt < max_retries:
                logger.info(f"⏳ Aguardando {retry_delay}s antes de tentar novamente...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ Todas as tentativas falharam.")
                raise ConnectionError("Não foi possível conectar ao Spark/MinIO")
    
    return None, None

# ----------------------------------------------------------------------
# FUNÇÃO: Atualizar métricas (executada UMA VEZ)
# ----------------------------------------------------------------------

def update_metrics_once():
    """
    Lê todas as tabelas Silver e atualiza as métricas do Prometheus.
    Esta função é executada UMA VEZ quando o script roda.
    """
    
    logger.info("\n" + "="*70)
    logger.info("📊 ATUALIZANDO MÉTRICAS DO PROMETHEUS")
    logger.info("="*70)
    
    # Conecta ao Spark e MinIO
    processor, validator = initialize_connections()
    
    # ====================================================================
    # TABELAS DFP (Demonstrações Financeiras Padronizadas)
    # ====================================================================
    tabelas_dfp = [
        ("DRE_con",      "2020"),
        ("DRE_con",      "2021"),
        ("DRE_con",      "2022"),
        ("DRE_con",      "2023"),
        ("DRE_con",      "2024"),
        ("BPA_con",      "2020"),
        ("BPA_con",      "2021"),
        ("BPA_con",      "2022"),
        ("BPA_con",      "2023"),
        ("BPA_con",      "2024"),
        ("BPP_con",      "2020"),
        ("BPP_con",      "2021"),
        ("BPP_con",      "2022"),
        ("BPP_con",      "2023"),
        ("BPP_con",      "2024"),
        ("DFC_DMPL_con", "2020"),
        ("DFC_DMPL_con", "2021"),
        ("DFC_DMPL_con", "2022"),
        ("DFC_DMPL_con", "2023"),
        ("DFC_DMPL_con", "2024"),

    ]

    # ====================================================================
    # TABELAS FRE (Formulário de Referência)
    # ====================================================================
 
    tabelas_fre = [
        # ────────────────────────────────────────────────────────────────
        # 1. VOLUME_VALOR_MOBILIARIO (Preço da Ação)
        # Documento: 04_Indicador_Preco_Acao_ATUALIZADO.docx
        # ────────────────────────────────────────────────────────────────
        ("volume_valor_mobiliario", "2020"),
        ("volume_valor_mobiliario", "2021"),
        ("volume_valor_mobiliario", "2022"),
        ("volume_valor_mobiliario", "2023"),
        ("volume_valor_mobiliario", "2024"),
        
        # ────────────────────────────────────────────────────────────────
        # 2. DISTRIBUICAO_DIVIDENDOS (Dividendos e JCP)
        # Documento: 06_Indicador_Dividendos_JCP_ATUALIZADO.docx
        # ────────────────────────────────────────────────────────────────
        # ⚠️ NOME CORRETO: "distribuicao_dividendos" (não "dividendos")
        ("distribuicao_dividendos", "2020"),
        ("distribuicao_dividendos", "2021"),
        ("distribuicao_dividendos", "2022"),
        ("distribuicao_dividendos", "2023"),
        ("distribuicao_dividendos", "2024"),

        # ────────────────────────────────────────────────────────────────
        # 3. CAPITAL_SOCIAL (Composição do Capital Social)
        ("capital_social", "2020"),
        ("capital_social", "2021"),
        ("capital_social", "2022"),
        ("capital_social", "2023"),
        ("capital_social", "2024"),
    ]

    # Combinar todas as tabelas
    tabelas = tabelas_dfp + tabelas_fre

    logger.info(f"📊 Total de tabelas a monitorar: {len(tabelas)}")
    logger.info(f"   - DFP: {len(tabelas_dfp)} tabelas")
    logger.info(f"   - FRE: {len(tabelas_fre)} tabelas")

    # Processar cada tabela
    success_count = 0
    error_count = 0
    
    for tabela, ano in tabelas:
        try:
            # Lê os dados da camada Silver no MinIO
            df = processor.read_silver_table(tabela, ano=int(ano))

            # Valida os dados e atualiza as métricas Prometheus
            validator.validate_and_report_metrics(df, tabela, ano)

            logger.info(f"  ✅ Métricas atualizadas: {tabela} / {ano}")
            success_count += 1

        except Exception as e:
            # Se uma tabela falhar, loga o erro e continua
            logger.error(f"  ❌ Erro em {tabela}/{ano}: {e}")
            error_count += 1

    logger.info("\n" + "="*70)
    logger.info("📊 RESUMO DA ATUALIZAÇÃO")
    logger.info("="*70)
    logger.info(f"✅ Sucesso: {success_count}/{len(tabelas)} tabelas")
    logger.info(f"❌ Erros:   {error_count}/{len(tabelas)} tabelas")
    logger.info("="*70 + "\n")
    
    # Fecha conexões Spark (libera recursos)
    processor.spark.stop()
    logger.info("🔌 Conexão Spark encerrada")

# ----------------------------------------------------------------------
# EXECUÇÃO PRINCIPAL
# ----------------------------------------------------------------------

if __name__ == "__main__":
    try:
        # ✅ EXECUTA UMA VEZ
        update_metrics_once()
        
        # ✅ MANTÉM SERVIDOR HTTP VIVO (sem polling!)
        logger.info("🌐 Servidor de métricas permanece ativo")
        logger.info("💡 Prometheus pode acessar http://localhost:8000/metrics")
        logger.info("⏸️  Script em modo idle (sem recalcular métricas)")
        
        # Mantém o processo vivo indefinidamente
        # O Prometheus vai coletar as métricas já calculadas
        while True:
            time.sleep(3600)  # Dorme por 1 hora, apenas para manter o processo vivo
            
    except KeyboardInterrupt:
        logger.info("\n👋 Encerrando servidor de métricas...")
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")
        raise