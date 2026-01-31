# config.py
from dataclasses import dataclass

@dataclass
class MinIOConfig:
    endpoint = "minio:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    secure: bool = False
    bucket_bronze: str = "bronze"