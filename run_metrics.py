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
# FLUXO:
#   1. Sobe o servidor HTTP para o Prometheus coletar métricas
#   2. Conecta ao Spark e ao MinIO via CVMSilverProcessor
#   3. A cada 60 segundos, lê os dados Silver e atualiza as métricas

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

# Intervalo em segundos entre cada atualização das métricas
# A cada 60s o sistema lê os dados do Silver e recalcula tudo
INTERVALO_SEGUNDOS = 60

# ----------------------------------------------------------------------
# INÍCIO DO PROGRAMA
# ----------------------------------------------------------------------

logger.info(f"📊 Subindo servidor de métricas na porta {PORT}...")

# Inicia o servidor HTTP em background (em paralelo com o resto do código)
# Após esta linha, o Prometheus já consegue acessar http://ingest:8000/metrics
# As métricas aparecerão como 0 ou vazias até o primeiro ciclo de validação
start_http_server(PORT)

logger.info(f"✅ Servidor rodando em http://localhost:{PORT}/metrics")

# Variáveis globais para o processador e validador
processor = None
validator = None

# ----------------------------------------------------------------------
# FUNÇÃO AUXILIAR: Inicializar conexões com retry
# ----------------------------------------------------------------------

def initialize_connections(max_retries=5, retry_delay=10):
    """Tenta conectar ao Spark e MinIO com retry em caso de falha"""
    global processor, validator
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"🔄 Tentativa {attempt}/{max_retries} - Conectando ao Spark e MinIO...")
            
            # Cria o processador: abre conexão com Spark e MinIO
            processor = CVMSilverProcessor()
            
            # Cria o validador passando a sessão Spark já aberta
            validator = SilverQualityValidator(processor.spark)
            
            logger.info("✅ Conexões estabelecidas com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Tentativa {attempt} falhou: {e}")
            if attempt < max_retries:
                logger.info(f"⏳ Aguardando {retry_delay}s antes de tentar novamente...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ Todas as tentativas falharam. Continuando sem atualização de métricas.")
                return False
    
    return False

# Tenta inicializar as conexões
initialize_connections()

# ----------------------------------------------------------------------
# LOOP PRINCIPAL — roda para sempre, atualizando métricas periodicamente
# ----------------------------------------------------------------------

logger.info("🔄 Iniciando loop de métricas...")

while True:
    try:
        # Se não conseguimos conectar, tenta novamente
        if processor is None or validator is None:
            logger.warning("⚠️  Sem conexões ativas. Tentando reconectar...")
            initialize_connections(max_retries=3, retry_delay=30)
            
            # Se ainda assim falhou, espera o intervalo e tenta de novo
            if processor is None or validator is None:
                logger.info(f"⏳ Aguardando {INTERVALO_SEGUNDOS}s até próxima tentativa...")
                time.sleep(INTERVALO_SEGUNDOS)
                continue
        
        logger.info("📥 Lendo tabelas Silver para atualizar métricas...")

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
            ("composicao_capital", "2020"),
            ("composicao_capital", "2021"),
            ("composicao_capital", "2022"),
            ("composicao_capital", "2023"),
            ("composicao_capital", "2024"),
        ]

        # ====================================================================
        # ⚠️ ALTERAÇÃO 5: TABELAS FRE 
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
        ]

        # Combinar todas as tabelas
        tabelas = tabelas_dfp + tabelas_fre

        logger.info(f"📊 Total de tabelas a monitorar: {len(tabelas)}")
        logger.info(f"   - DFP: {len(tabelas_dfp)} tabelas")
        logger.info(f"   - FRE: {len(tabelas_fre)} tabelas")

        for tabela, ano in tabelas:
            try:
                # Lê os dados da camada Silver no MinIO
                df = processor.read_silver_table(tabela, ano=int(ano))

                # Valida os dados e atualiza as métricas Prometheus
                validator.validate_and_report_metrics(df, tabela, ano)

                logger.info(f"  ✅ Métricas atualizadas: {tabela} / {ano}")

            except Exception as e:
                # Se uma tabela falhar, loga o erro e continua
                logger.error(f"  ❌ Erro em {tabela}/{ano}: {e}")

        logger.info(f"⏳ Aguardando {INTERVALO_SEGUNDOS}s até próxima atualização...")

    except Exception as e:
        # Captura erros inesperados no loop principal
        logger.error(f"❌ Erro inesperado no loop: {e}")
        # Em caso de erro grave, marca para reconexão
        processor = None
        validator = None

    # Aguarda o intervalo definido antes de repetir o ciclo
    time.sleep(INTERVALO_SEGUNDOS)