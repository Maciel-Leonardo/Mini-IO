# config.py
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class MinIOConfig:
    """Configuração do MinIO"""
    endpoint: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    
    # Buckets por camada
    bucket_bronze: str = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    bucket_silver: str = os.getenv("MINIO_BUCKET_SILVER", "silver")
    bucket_gold: str = os.getenv("MINIO_BUCKET_GOLD", "gold")


@dataclass
class DataSourceConfig:
    """Configuração de fonte de dados"""
    source_name: str          # Ex: "fundamentus"
    asset_type: str           # Ex: "acoes", "opcoes", "fiis"
    identifier_type: str      # Ex: "papel", "ticker", "cnpj"
    data_format: str          # Ex: "html", "json", "csv", "parquet"
    
    def get_base_path(self, identifier_value: str, extraction_date: str, 
                      extraction_time: Optional[str] = None) -> str:
        """
        Gera o caminho base no formato padronizado
        
        Args:
            identifier_value: Valor do identificador (ex: "PETR4")
            extraction_date: Data no formato YYYY-MM-DD
            extraction_time: Hora no formato HH-MM-SS (opcional)
            
        Returns:
            Caminho completo no formato:
            {fonte}/{tipo_ativo}/{identificador}={valor}/data_extracao={data}/[hora={hora}/]
        """
        path = (
            f"{self.source_name}/"
            f"{self.asset_type}/"
            f"{self.identifier_type}={identifier_value}/"
            f"data_extracao={extraction_date}/"
        )
        
        if extraction_time:
            path += f"hora={extraction_time}/"
            
        return path


# Exemplos de configurações de fontes
FUNDAMENTUS_CONFIG = DataSourceConfig(
    source_name="fundamentus",
    asset_type="acoes",
    identifier_type="papel",
    data_format="html"
)


CVM_CONFIG = DataSourceConfig(
    source_name="gov_br_cvm",
    asset_type="demonstracoes_financeiras_padronizadas",
    identifier_type="ano",
    data_format="zip"
)