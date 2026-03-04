
"""
Inicia servidor HTTP de métricas Prometheus.
Deve rodar EM BACKGROUND antes do pipeline, via:

    docker exec -d ingest python start_metrics_server.py

Enquanto este processo estiver vivo, o Prometheus consegue fazer
scrape em ingest:8000 e ler as métricas geradas pelo silver.py.
"""
from prometheus_client import start_http_server
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PORT = 8000

logger.info(f"📊 Iniciando servidor de métricas na porta {PORT}")
start_http_server(PORT)
logger.info(f"✅ Métricas disponíveis em http://localhost:{PORT}/metrics")
logger.info("Servidor rodando continuamente. Ctrl+C para encerrar.")

try:
    while True:
        time.sleep(5)
except KeyboardInterrupt:
    logger.info("🛑 Servidor encerrado")