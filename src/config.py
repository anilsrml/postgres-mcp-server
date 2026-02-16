"""Konfigürasyon yönetimi"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """Uygulama ayarları"""
    
    # Pydantic V2 config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # LLM Provider Seçimi
    llm_provider: str = Field(default="ollama", alias="LLM_PROVIDER")  # "ollama" veya "gemini"
    
    # Ollama Ayarları
    ollama_base_url: str = Field(default="http://localhost:11434", alias="OLLAMA_BASE_URL")
    ollama_model: str = Field(default="mistral", alias="OLLAMA_MODEL")  # mistral, llama3.2, vs.
    
    # Google Gemini API (opsiyonel)
    google_api_key: Optional[str] = Field(default=None, alias="GOOGLE_API_KEY")
    
    # PostgreSQL Bağlantı Bilgileri
    db_host: str = Field(default="localhost", alias="DB_HOST")
    db_port: int = Field(default=5432, alias="DB_PORT")
    db_name: str = Field(..., alias="DB_NAME")
    db_user: str = Field(..., alias="DB_USER")
    db_password: str = Field(..., alias="DB_PASSWORD")
    
    # Güvenlik Ayarları
    max_query_timeout: int = Field(default=30, alias="MAX_QUERY_TIMEOUT")
    max_result_rows: int = Field(default=1000, alias="MAX_RESULT_ROWS")
    
    # Yazma İşlemleri Güvenlik Ayarları
    write_enabled: bool = Field(default=False, alias="WRITE_ENABLED")
    writable_tables: str = Field(default="", alias="WRITABLE_TABLES")  # virgülle ayrılmış tablo isimleri
    max_write_rows: int = Field(default=100, alias="MAX_WRITE_ROWS")
    
    @property
    def writable_tables_set(self) -> set:
        """Yazma izni verilen tablo isimlerini set olarak döndür"""
        if not self.writable_tables.strip():
            return set()
        return {t.strip() for t in self.writable_tables.split(",") if t.strip()}
    
    # Loglama
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    
    @property
    def database_url(self) -> str:
        """PostgreSQL bağlantı URL'i"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


# Global settings instance
settings = Settings()

