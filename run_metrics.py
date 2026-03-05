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
logger.info("🔄 Conectando ao Spark e MinIO...")

# Cria o processador: abre conexão com Spark e MinIO
# Esta linha pode demorar alguns segundos pois inicializa o Spark
processor = CVMSilverProcessor()

# Cria o validador passando a sessão Spark já aberta pelo processor
# Assim evitamos abrir duas sessões Spark desnecessariamente
validator = SilverQualityValidator(processor.spark)

logger.info("✅ Conexões estabelecidas. Iniciando loop de métricas...")

# ----------------------------------------------------------------------
# LOOP PRINCIPAL — roda para sempre, atualizando métricas periodicamente
# ----------------------------------------------------------------------

while True:
    try:
        logger.info("📥 Lendo tabelas Silver para atualizar métricas...")

        # Lista de tabelas e anos que queremos monitorar
        # Adicione ou remova entradas conforme necessário
        tabelas = [
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

        for tabela, ano in tabelas:
            try:
                # Lê os dados da camada Silver no MinIO (formato Delta Lake)
                df = processor.read_silver_table(tabela, ano=int(ano))

                # Valida os dados e atualiza todas as métricas Prometheus:
                # silver_rows_total, silver_companies_unique,
                # silver_null_percentage, silver_validation_errors, etc.
                validator.validate_and_report_metrics(df, tabela, ano)

                logger.info(f"  ✅ Métricas atualizadas: {tabela} / {ano}")

            except Exception as e:
                # Se uma tabela falhar, apenas loga o erro e continua
                # as outras tabelas — não derruba o servidor inteiro
                logger.error(f"  ❌ Erro em {tabela}/{ano}: {e}")

        logger.info(f"⏳ Aguardando {INTERVALO_SEGUNDOS}s até próxima atualização...")

    except Exception as e:
        # Captura erros inesperados no loop principal
        # O servidor HTTP continua no ar mesmo com erro aqui
        logger.error(f"❌ Erro inesperado no loop: {e}")

    # Aguarda o intervalo definido antes de repetir o ciclo
    time.sleep(INTERVALO_SEGUNDOS)