# ingest.py
from datetime import datetime
import json
import requests
from minio import Minio
from io import BytesIO
from config import MinIOConfig


class FundamentusIngestion:
    
    def __init__(self, config: MinIOConfig = None):
        self.config = config or MinIOConfig()
        self.minio_client = Minio(
            self.config.endpoint,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            secure=self.config.secure
        )
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        if not self.minio_client.bucket_exists(self.config.bucket_bronze):
            self.minio_client.make_bucket(self.config.bucket_bronze)
    
    def ingest(self, papel: str) -> dict:
        url = f"https://www.fundamentus.com.br/detalhes.php?papel={papel}"
        timestamp_utc = datetime.utcnow()
        data_extracao_path = timestamp_utc.strftime("%Y%m%dT%H%M%S")
        data_extracao_iso = timestamp_utc.isoformat()
        
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = "iso-8859-1"
        
        base_path = f"fundamentus/papel={papel}/data_extracao={data_extracao_path}"
        
        # Salva HTML
        html_content = response.text.encode(response.encoding)
        self.minio_client.put_object(
            bucket_name=self.config.bucket_bronze,
            object_name=f"{base_path}/raw.html",
            data=BytesIO(html_content),
            length=len(html_content),
            content_type="text/html"
        )
        
        # Salva metadados
        metadata = {
            "fonte": "fundamentus",
            "papel": papel,
            "url": url,
            "data_extracao": data_extracao_iso,
            "status_code": response.status_code,
            "encoding": response.encoding,
            "headers": headers
        }
        
        metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2)
        metadata_bytes = metadata_json.encode("utf-8")
        
        self.minio_client.put_object(
            bucket_name=self.config.bucket_bronze,
            object_name=f"{base_path}/metadata.json",
            data=BytesIO(metadata_bytes),
            length=len(metadata_bytes),
            content_type="application/json"
        )
        
        return {
            "bucket": self.config.bucket_bronze,
            "path": base_path,
            "timestamp": data_extracao_iso
        }
# Exemplo de uso
if __name__ == "__main__":
    ingestion = FundamentusIngestion()
    result = ingestion.ingest("ABCD3")
    print(result)