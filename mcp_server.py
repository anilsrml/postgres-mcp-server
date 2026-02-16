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

3. Yazma işlemlerini etkinleştirmek için:
   WRITE_ENABLED=true
   WRITABLE_TABLES=customers,orders,products
   MAX_WRITE_ROWS=100

4. MCP sunucusunu test edin:
   python mcp_server.py

5. Claude Desktop için yapılandırma:
   
   Windows: %APPDATA%/Claude/claude_desktop_config.json
   Mac: ~/Library/Application Support/Claude/claude_desktop_config.json
   
   Şu içeriği ekleyin:
   
   {
     "mcpServers": {
       "postgres-dbq": {
         "command": "python",
         "args": ["c:/Users/anil6/Desktop/dbqa-w-mcp/mcp_server.py"],
         "env": {}
       }
     }
   }

6. Cursor IDE için yapılandırma:
   
   Settings (Ctrl+,) -> MCP -> Add Server
   
   Name: postgres-dbq
   Command: python
   Args: ["c:/Users/anil6/Desktop/dbqa-w-mcp/mcp_server.py"]

GÜVENLİK:
=========
- DDL komutları (DROP, TRUNCATE, ALTER, CREATE) her zaman engellenmiştir
- SELECT sorguları her zaman çalıştırılabilir (query_database tool'u)
- Yazma işlemleri (INSERT, UPDATE, DELETE) iki aşamalı onay mekanizması ile çalışır:
  1. modify_data → Dry-run preview (kaç satır etkileneceğini gösterir)
  2. confirm_modification → Gerçek çalıştırma (onay sonrası)
- UPDATE/DELETE sorgularında WHERE koşulu zorunludur
- Tek sorguda etkilenecek satır sayısı sınırlandırılmıştır (varsayılan: 100)
- Sadece .env'de belirtilen tablolara yazma izni verilir
- WRITE_ENABLED=false ise yazma tool'ları tamamen devre dışıdır

ÖZELLİKLER:
===========
Resource: postgres://schema - Veritabanı şeması bilgisi
Tool: query_database - Güvenli SQL sorgu çalıştırma (SELECT)
Tool: modify_data - Yazma sorgusu preview (INSERT/UPDATE/DELETE) [opsiyonel]
Tool: confirm_modification - Onaylanan yazma sorgusunu çalıştırma [opsiyonel]
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
write_executor: QueryExecutor = None  # Yazma işlemleri için ayrı executor


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


# ============================================================
# YAZMA İŞLEMLERİ (WRITE_ENABLED=true ise aktif)
# ============================================================

def register_write_tools():
    """
    Yazma tool'larını MCP sunucusuna kaydet.
    Sadece WRITE_ENABLED=true ise çağrılır.
    """
    
    @mcp.tool()
    async def modify_data(sql_query: str) -> str:
        """
        Yazma sorgusu preview'ı: Sorguyu doğrular ve kaç satır etkileneceğini gösterir.
        
        Bu tool sorguyu ÇALIŞTIRMAZ, sadece preview döndürür.
        Sorguyu gerçekten çalıştırmak için confirm_modification tool'unu kullanın.
        
        Güvenlik kuralları:
        - Sadece INSERT, UPDATE, DELETE komutları çalıştırılabilir
        - UPDATE ve DELETE sorgularında WHERE koşulu zorunludur
        - Tek sorguda etkilenecek satır sayısı sınırlıdır
        - Sadece izinli tablolara yazma yapılabilir
        - DDL komutları (DROP, CREATE, ALTER vb.) her zaman engellenir
        
        Args:
            sql_query: INSERT, UPDATE veya DELETE sorgusu
            
        Returns:
            Preview bilgisi (etkilenecek satır sayısı, hedef tablo, doğrulama durumu)
            
        Examples:
            modify_data("INSERT INTO customers (name, email) VALUES ('Ahmet', 'ahmet@example.com')")
            modify_data("UPDATE orders SET status = 'shipped' WHERE id = 42")
            modify_data("DELETE FROM logs WHERE created_at < '2024-01-01'")
        """
        try:
            logger.info("MCP Tool called: modify_data (preview)", sql=sql_query[:200])
            
            # Dry-run preview oluştur
            preview = write_executor.preview_write(
                sql=sql_query,
                validate=True,
            )
            
            if not preview["valid"]:
                error_msg = preview.get("error", "Bilinmeyen doğrulama hatası")
                logger.warning("Write preview validation failed", error=error_msg)
                return (
                    f"❌ Sorgu Doğrulama Hatası: {error_msg}\n\n"
                    f"💡 İpucu:\n"
                    f"  - UPDATE/DELETE sorgularında WHERE koşulu zorunludur\n"
                    f"  - Sadece izinli tablolara yazma yapılabilir\n"
                    f"  - DDL komutları (DROP, ALTER vb.) engellenmiştir"
                )
            
            # Preview başarılı - sonucu formatla
            preview_text = (
                f"📋 **Yazma İşlemi Preview**\n"
                f"{'=' * 40}\n"
                f"📌 Sorgu Tipi: {preview['query_type']}\n"
                f"📌 Hedef Tablo: {preview['target_table'] or 'Belirlenemedi'}\n"
                f"📌 Tahmini Etkilenen Satır: {preview['estimated_rows'] if preview['estimated_rows'] is not None else 'Hesaplanamadı'}\n"
                f"📌 Temizlenmiş SQL:\n```sql\n{preview['sanitized_sql']}\n```\n\n"
                f"⚠️ Bu sorgu henüz ÇALIŞTIRILMADI.\n"
                f"✅ Çalıştırmak için confirm_modification tool'unu aynı SQL ile çağırın."
            )
            
            logger.info(
                "Write preview generated successfully",
                query_type=preview["query_type"],
                target_table=preview["target_table"],
                estimated_rows=preview["estimated_rows"],
            )
            
            return preview_text
            
        except Exception as e:
            error_msg = f"Beklenmeyen Hata: {str(e)}"
            logger.error("Unexpected error in modify_data", error=str(e))
            return f"❌ {error_msg}"
    
    @mcp.tool()
    async def confirm_modification(sql_query: str) -> str:
        """
        Onaylanan yazma sorgusunu gerçekten çalıştırır.
        
        ÖNEMLİ: Bu tool'u çağırmadan önce mutlaka modify_data tool'u ile
        preview alınmış olmalıdır. Bu tool sorguyu doğrular ve çalıştırır.
        
        Args:
            sql_query: Daha önce modify_data ile preview alınan SQL sorgusu
            
        Returns:
            İşlem sonucu (etkilenen satır sayısı, hedef tablo)
        """
        try:
            logger.info("MCP Tool called: confirm_modification", sql=sql_query[:200])
            
            # Sorguyu çalıştır
            result = write_executor.execute_write(
                sql=sql_query,
                validate=True,
            )
            
            if result["success"]:
                success_text = (
                    f"✅ **Yazma İşlemi Başarılı**\n"
                    f"{'=' * 40}\n"
                    f"📌 Sorgu Tipi: {result['query_type']}\n"
                    f"📌 Hedef Tablo: {result['target_table'] or 'Belirlenemedi'}\n"
                    f"📌 Etkilenen Satır Sayısı: {result['affected_rows']}\n"
                )
                
                logger.info(
                    "Write query confirmed and executed",
                    query_type=result["query_type"],
                    target_table=result["target_table"],
                    affected_rows=result["affected_rows"],
                )
                
                return success_text
            
        except ValidationError as e:
            error_msg = f"Validasyon Hatası: {str(e)}"
            logger.warning("Write confirmation validation failed", error=str(e))
            return (
                f"❌ {error_msg}\n\n"
                f"💡 İpucu: Önce modify_data tool'u ile preview alın."
            )
            
        except QueryExecutionError as e:
            error_msg = f"Sorgu Hatası: {str(e)}"
            logger.error("Write confirmation execution failed", error=str(e))
            return (
                f"❌ {error_msg}\n\n"
                f"💡 İpucu: SQL syntax'ını kontrol edin."
            )
            
        except QueryTimeoutError as e:
            error_msg = f"Zaman Aşımı: {str(e)}"
            logger.error("Write confirmation timeout", error=str(e))
            return f"❌ {error_msg}"
            
        except Exception as e:
            error_msg = f"Beklenmeyen Hata: {str(e)}"
            logger.error("Unexpected error in confirm_modification", error=str(e))
            return f"❌ {error_msg}"


def initialize_database():
    """
    Veritabanı bağlantısını ve yöneticilerini başlat.
    
    Mevcut DatabaseConnection, SchemaManager ve QueryExecutor
    sınıflarını kullanır (DRY prensibi).
    """
    global db_connection, schema_manager, query_executor, write_executor
    
    try:
        logger.debug(
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
        
        # ===== OKUMA (SELECT) EXECUTOR =====
        read_validator = SQLValidator(strict_mode=True)  # Varsayılan: sadece SELECT
        query_executor = QueryExecutor(
            db_connection=db_connection,
            validator=read_validator,
            timeout=settings.max_query_timeout,
            max_rows=settings.max_result_rows,
        )
        
        # ===== YAZMA EXECUTOR (opsiyonel) =====
        if settings.write_enabled:
            writable_tables = settings.writable_tables_set or None  # Boş set → None (tüm tablolar)
            
            write_validator = SQLValidator(
                strict_mode=True,
                allowed_operations={"INSERT", "UPDATE", "DELETE"},
                writable_tables=writable_tables,
            )
            write_executor = QueryExecutor(
                db_connection=db_connection,
                validator=write_validator,
                timeout=settings.max_query_timeout,
                max_write_rows=settings.max_write_rows,
            )
            
            logger.debug(
                "Write executor initialized",
                writable_tables=list(writable_tables) if writable_tables else "ALL",
                max_write_rows=settings.max_write_rows,
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
        
        # Yazma tool'larını kaydet (eğer etkinse)
        tools_list = ["query_database"]
        if settings.write_enabled:
            register_write_tools()
            tools_list.extend(["modify_data", "confirm_modification"])
            logger.info(
                "Write tools registered",
                writable_tables=settings.writable_tables or "ALL",
                max_write_rows=settings.max_write_rows,
            )
        else:
            logger.info("Write tools DISABLED (WRITE_ENABLED=false)")
        
        # MCP sunucusunu başlat
        logger.info(
            "MCP server ready",
            resources=["postgres://schema"],
            tools=tools_list,
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
