from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    access_key_id: str = "minioadmin"
    secret_access_key: str = "minioadmin"
    storage_path: str = "/data"
    region: str = "us-east-1"
    xattr_jclouds_compat: bool = False

    model_config = {"env_prefix": "S3_"}


settings = Settings()
