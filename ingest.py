# ingest.py
from datetime import datetime
import json
import requests
from minio import Minio
from io import BytesIO
from config import MinIOConfig, DataSourceConfig, FUNDAMENTUS_CONFIG, CVM_CONFIG
import logging
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MultiSourceIngestion:
    """Classe base para ingestão de múltiplas fontes"""
    
    def __init__(self, 
                 minio_config: MinIOConfig = None,
                 source_config: DataSourceConfig = None):
        self.minio_config = minio_config or MinIOConfig()
        self.source_config = source_config
        self.minio_client = Minio(
            self.minio_config.endpoint,
            access_key=self.minio_config.access_key,
            secret_key=self.minio_config.secret_key,
            secure=self.minio_config.secure
        )
        self._ensure_bucket_exists()
        
    def _ensure_bucket_exists(self):
        """Garante que o bucket bronze existe"""
        if not self.minio_client.bucket_exists(self.minio_config.bucket_bronze):
            self.minio_client.make_bucket(self.minio_config.bucket_bronze)
            logger.info(f"Bucket {self.minio_config.bucket_bronze} criado")
    
    def _save_to_minio(self, 
                       data: bytes, 
                       path: str, 
                       content_type: str) -> None:
        """Salva dados no MinIO"""
        self.minio_client.put_object(
            bucket_name=self.minio_config.bucket_bronze,
            object_name=path,
            data=BytesIO(data),
            length=len(data),
            content_type=content_type
        )
        logger.info(f"Arquivo salvo: s3://{self.minio_config.bucket_bronze}/{path}")
    
    def _save_metadata(self, 
                       base_path: str, 
                       metadata: Dict[str, Any]) -> None:
        """Salva metadados em JSON"""
        metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2)
        metadata_bytes = metadata_json.encode("utf-8")
        
        self._save_to_minio(
            data=metadata_bytes,
            path=f"{base_path}metadata.json",
            content_type="application/json"
        )


class FundamentusIngestion(MultiSourceIngestion):
    """Ingestão específica do Fundamentus"""
    
    def __init__(self, minio_config: MinIOConfig = None):
        super().__init__(
            minio_config=minio_config,
            source_config=FUNDAMENTUS_CONFIG
        )
    
    def ingest(self, papel: str) -> Optional[Dict[str, Any]]:
        """
        Faz a ingestão de dados do Fundamentus
        
        Args:
            papel: Código do papel (ex: PETR4, VALE3)
            
        Returns:
            Dicionário com informações da ingestão ou None em caso de erro
        """
        try:
            papel = papel.upper().strip()
            logger.info(f"Iniciando ingestão Fundamentus: {papel}")
            
            # 1. Buscar dados
            url = f"https://www.fundamentus.com.br/detalhes.php?papel={papel}"
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            response.encoding = "iso-8859-1"
            
            # 2. Preparar timestamps
            timestamp_utc = datetime.utcnow()
            extraction_date = timestamp_utc.strftime("%Y-%m-%d")
            extraction_time = timestamp_utc.strftime("%H-%M-%S")
            
            # 3. Gerar caminho usando DataSourceConfig
            base_path = self.source_config.get_base_path(
                identifier_value=papel,
                extraction_date=extraction_date,
                extraction_time=extraction_time
            )
            
            # 4. Salvar HTML
            html_content = response.text.encode(response.encoding)
            self._save_to_minio(
                data=html_content,
                path=f"{base_path}raw.html",
                content_type="text/html"
            )
            
            # 5. Salvar metadados
            metadata = {
                "fonte": self.source_config.source_name,
                "tipo_ativo": self.source_config.asset_type,
                "identificador": {
                    "tipo": self.source_config.identifier_type,
                    "valor": papel
                },
                "extracao": {
                    "data": extraction_date,
                    "hora": extraction_time,
                    "timestamp_utc": timestamp_utc.isoformat()
                },
                "requisicao": {
                    "url": url,
                    "status_code": response.status_code,
                    "encoding": response.encoding,
                    "headers": headers,
                    "tempo_resposta_ms": response.elapsed.total_seconds() * 1000
                },
                "dados": {
                    "formato": self.source_config.data_format,
                    "tamanho_bytes": len(html_content)
                },
                "versao_ingestor": "2.0.0"
            }
            
            self._save_metadata(base_path, metadata)
            
            result = {
                "fonte": self.source_config.source_name,
                "bucket": self.minio_config.bucket_bronze,
                "path": base_path,
                "timestamp": timestamp_utc.isoformat(),
                "identificador": papel
            }
            
            logger.info(f"✅ Ingestão concluída: {papel}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao buscar dados de {papel}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao processar {papel}: {e}")
            return None
class CVMIngestion(MultiSourceIngestion):
    """Ingestão específica da CVM"""
    
    def __init__(self, minio_config: MinIOConfig = None):
        super().__init__(
            minio_config=minio_config,
            source_config=CVM_CONFIG
        )
    def ingest(self, ano: str) -> Optional[Dict[str, Any]]:
        """
        Faz a ingestão de dados da CVM - Formulário de Demonstrações Financeiras Padronizadas (DFP)
        
        Args:
            ano: Ano de referência (ex: 2023)
            
        Returns:
            Dicionário com informações da ingestão ou None em caso de erro
        """
        try:
            logger.info(f"Iniciando ingestão CVM: {ano}")
            
            # 1. Buscar dados
            url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ano}.zip"
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            
            # 2. Preparar timestamps
            timestamp_utc = datetime.utcnow()
            extraction_date = timestamp_utc.strftime("%Y-%m-%d")
            extraction_time = timestamp_utc.strftime("%H-%M-%S")
            
            # 3. Gerar caminho usando DataSourceConfig
            base_path = self.source_config.get_base_path(
                identifier_value=ano,
                extraction_date=extraction_date,
                extraction_time=extraction_time
            )
            # 4. Salvar ZIP
            self._save_to_minio(
                data=response.content,
                path=f"{base_path}dfp{ano}.zip",
                content_type="application/zip"
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao buscar dados de {ano}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao processar {ano}: {e}")
            return None
# Exemplo de uso
if __name__ == "__main__":
    # Ingestão de múltiplas fontes para a mesma ação
    anos = ["2020", "2021", "2022", "2023", "2024", "2025"]
    
    # Fonte 1: CVM
    cvm = CVMIngestion()
    for ano in anos:
        result1 = cvm.ingest(ano)

