"""Konfigürasyon yönetimi"""

from urllib.parse import urlparse
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
    
    # PostgreSQL Bağlantı URI (tek satır)
    # Format: postgresql://username:password@host:port/dbname
    database_uri: str = Field(..., alias="DATABASE_URI")
    
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
        """PostgreSQL bağlantı URL'i (DATABASE_URI'yi döndürür)"""
        return self.database_uri
    
    # --- URI'den parse edilen yardımcı property'ler (loglama için) ---
    
    @property
    def _parsed_uri(self):
        return urlparse(self.database_uri)
    
    @property
    def db_host(self) -> str:
        return self._parsed_uri.hostname or "localhost"
    
    @property
    def db_port(self) -> int:
        return self._parsed_uri.port or 5432
    
    @property
    def db_name(self) -> str:
        return self._parsed_uri.path.lstrip("/")
    
    @property
    def db_user(self) -> str:
        return self._parsed_uri.username or ""
    
    @property
    def masked_uri(self) -> str:
        """Şifre maskelenmiş URI (loglama için güvenli)"""
        parsed = self._parsed_uri
        if parsed.password:
            return self.database_uri.replace(f":{parsed.password}@", ":****@")
        return self.database_uri


# Global settings instance
settings = Settings()


