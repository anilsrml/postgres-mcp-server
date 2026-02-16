"""
MCP (Model Context Protocol) Server for PostgreSQL Database Access

Bu sunucu, AI modellerinin (Claude, Cursor, Ollama vb.) veritabanı ile
güvenli bir şekilde etkileşim kurmasını sağlar.

KURULUM VE KULLANIM:
====================

1. Bağımlılıkları yükleyin:
   pip install -r requirements.txt

2. .env dosyasında veritabanı bağlantı bilgilerinizi ayarlayın:
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=your_database
   DB_USER=your_username
   DB_PASSWORD=your_password

3. MCP sunucusunu test edin:
   python src/mcp_server.py

4. Claude Desktop için yapılandırma:
   
   Windows: %APPDATA%/Claude/claude_desktop_config.json
   Mac: ~/Library/Application Support/Claude/claude_desktop_config.json
   
   Şu içeriği ekleyin:
   
   {
     "mcpServers": {
       "postgres-dbq": {
         "command": "python",
         "args": ["c:/Users/anil6/Desktop/dbq-copy/src/mcp_server.py"],
         "env": {}
       }
     }
   }

5. Cursor IDE için yapılandırma:
   
   Settings (Ctrl+,) -> MCP -> Add Server
   
   Name: postgres-dbq
   Command: python
   Args: ["c:/Users/anil6/Desktop/dbq-copy/src/mcp_server.py"]

GÜVENLİK:
=========
- Sadece SELECT sorguları çalıştırılabilir
- INSERT, UPDATE, DELETE, DROP gibi komutlar engellenmiştir
- Mevcut SQLValidator sınıfı kullanılarak tüm sorgular doğrulanır
- Sorgu sonuçları maksimum 1000 satır ile sınırlandırılmıştır

ÖZELLİKLER:
===========
Resource: postgres://schema - Veritabanı şeması bilgisi
Tool: query_database - Güvenli SQL sorgu çalıştırma
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Any
from fastmcp import FastMCP
# Add project directory to Python path for src package imports
project_path = Path(__file__).parent
sys.path.insert(0, str(project_path))



# Mevcut modülleri kullan (DRY prensibi) - absolute imports from src package
from src.database.connection import DatabaseConnection
from src.database.schema_manager import SchemaManager
from src.database.executor import QueryExecutor
from src.validation.sql_validator import SQLValidator, ValidationError
from src.database.executor import QueryExecutionError, TimeoutError as QueryTimeoutError
from src.utils.logger import logger
from src.config import settings

# MCP sunucusunu başlat
mcp = FastMCP("PostgreSQL Database MCP Server")

# Global değişkenler (sunucu başlatıldığında oluşturulacak)
db_connection: DatabaseConnection = None
schema_manager: SchemaManager = None
query_executor: QueryExecutor = None


@mcp.resource("postgres://schema")
async def get_database_schema() -> str:
    """
    Veritabanı şeması bilgisini döndürür.
    
    Bu resource, AI modellerinin veritabanı yapısını anlamasını sağlar.
    Tablo isimleri, kolonlar, veri tipleri, ilişkiler ve örnek değerler
    içerir.
    
    Returns:
        Formatlanmış veritabanı şeması metni
    """
    try:
        logger.info("MCP Resource requested: postgres://schema")
        
        # Mevcut SchemaManager metodunu kullan
        schema_text = schema_manager.get_schema_for_llm()
        
        logger.info("Schema resource returned successfully")
        return schema_text
        
    except Exception as e:
        error_msg = f"Şema bilgisi alınamadı: {str(e)}"
        logger.error("Schema resource error", error=str(e))
        return f"HATA: {error_msg}"


@mcp.tool()
async def query_database(sql_query: str) -> str:
    """
    Güvenli SQL sorgusu çalıştırır (sadece SELECT).
    
    Bu tool, AI modellerinin veritabanında sorgu çalıştırmasını sağlar.
    Güvenlik için sadece SELECT sorguları kabul edilir. Tüm sorgular
    mevcut SQLValidator ile doğrulanır.
    
    Args:
        sql_query: Çalıştırılacak SQL sorgusu (SELECT only)
        
    Returns:
        JSON formatında sorgu sonuçları veya hata mesajı
        
    Examples:
        query_database("SELECT * FROM customers LIMIT 5")
        query_database("SELECT COUNT(*) FROM orders WHERE status = 'completed'")
    """
    try:
        logger.info("MCP Tool called: query_database", sql=sql_query[:200])
        
        # Mevcut QueryExecutor'ı kullanarak sorguyu çalıştır
        # QueryExecutor içinde zaten SQLValidator kullanılıyor
        results = query_executor.execute_query(
            sql=sql_query,
            validate=True  # Validasyonu etkinleştir (SELECT kontrolü)
        )
        
        # Sonuçları JSON formatında döndür
        result_json = json.dumps(results, ensure_ascii=False, indent=2, default=str)
        
        logger.info(
            "Query executed successfully",
            sql=sql_query[:100],
            row_count=len(results)
        )
        
        return f"Sorgu başarılı. {len(results)} satır döndürüldü.\n\nSonuçlar:\n{result_json}"
        
    except ValidationError as e:
        # Validasyon hatası (örn: SELECT dışında bir komut)
        error_msg = f"Validasyon Hatası: {str(e)}"
        logger.warning("Query validation failed", error=str(e), sql=sql_query[:100])
        return f"❌ {error_msg}\n\n💡 İpucu: Sadece SELECT sorguları çalıştırılabilir."
        
    except QueryExecutionError as e:
        # Sorgu çalıştırma hatası (örn: syntax hatası, tablo bulunamadı)
        error_msg = f"Sorgu Hatası: {str(e)}"
        logger.error("Query execution failed", error=str(e), sql=sql_query[:100])
        return f"❌ {error_msg}\n\n💡 İpucu: SQL syntax'ını kontrol edin veya tablo/kolon isimlerini doğrulayın."
        
    except QueryTimeoutError as e:
        # Zaman aşımı hatası
        error_msg = f"Zaman Aşımı: {str(e)}"
        logger.error("Query timeout", error=str(e), sql=sql_query[:100])
        return f"❌ {error_msg}\n\n💡 İpucu: Sorguyu basitleştirin veya LIMIT kullanın."
        
    except Exception as e:
        # Beklenmeyen hatalar
        error_msg = f"Beklenmeyen Hata: {str(e)}"
        logger.error("Unexpected error in query_database", error=str(e))
        return f"❌ {error_msg}\n\n💡 İpucu: Lütfen sorgu formatınızı kontrol edin."


def initialize_database():
    """
    Veritabanı bağlantısını ve yöneticilerini başlat.
    
    Mevcut DatabaseConnection, SchemaManager ve QueryExecutor
    sınıflarını kullanır (DRY prensibi).
    """
    global db_connection, schema_manager, query_executor
    
    try:
        logger.info(
            "Initializing database connection",
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name
        )
        
        # Mevcut DatabaseConnection sınıfını kullan
        db_connection = DatabaseConnection()
        
        # Bağlantıyı test et
        if not db_connection.test_connection():
            raise Exception("Veritabanı bağlantı testi başarısız!")
        
        # Mevcut SchemaManager'ı başlat
        schema_manager = SchemaManager(db_connection)
        
        # Mevcut QueryExecutor'ı başlat (güvenlik validasyonları ile)
        validator = SQLValidator(strict_mode=True)  # Katı mod: sadece SELECT
        query_executor = QueryExecutor(
            db_connection=db_connection,
            validator=validator,
            timeout=settings.max_query_timeout,
            max_rows=settings.max_result_rows
        )
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        raise


if __name__ == "__main__":
    """
    MCP sunucusunu başlat.
    
    Bu script doğrudan çalıştırılabilir veya MCP client
    (Claude Desktop, Cursor vb.) tarafından başlatılabilir.
    """
    try:
        # Veritabanını başlat
        logger.info("Starting MCP server...")
        initialize_database()
        
        # MCP sunucusunu başlat
        logger.info(
            "MCP server ready",
            resources=["postgres://schema"],
            tools=["query_database"]
        )
        
        # Sunucuyu çalıştır
        mcp.run()
        
    except KeyboardInterrupt:
        logger.info("MCP server stopped by user")
        if db_connection:
            db_connection.disconnect()
            
    except Exception as e:
        logger.error("MCP server failed to start", error=str(e))
        if db_connection:
            db_connection.disconnect()
        raise
