"""PostgreSQL bağlantı yönetimi"""

import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, Generator
from ..config import settings
from ..utils.logger import logger


class DatabaseConnection:
    """PostgreSQL veritabanı bağlantı yöneticisi"""
    
    def __init__(self):
        """Bağlantı parametrelerini ayarla"""
        self.connection_params = {
            "host": settings.db_host,
            "port": settings.db_port,
            "database": settings.db_name,
            "user": settings.db_user,
            "password": settings.db_password,
        }
        self._connection: Optional[psycopg2.extensions.connection] = None
        logger.debug("DatabaseConnection initialized", params={
            "host": settings.db_host,
            "port": settings.db_port,
            "database": settings.db_name,
        })
    
    def connect(self) -> psycopg2.extensions.connection:
        """
        Veritabanına bağlan
        
        Returns:
            PostgreSQL bağlantı nesnesi
        
        Raises:
            psycopg2.Error: Bağlantı hatası durumunda
        """
        try:
            if self._connection is None or self._connection.closed:
                self._connection = psycopg2.connect(**self.connection_params)
                logger.info("Database connection established")
            return self._connection
        except psycopg2.Error as e:
            logger.error("Database connection failed", error=str(e))
            raise
    
    def disconnect(self):
        """Veritabanı bağlantısını kapat"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.info("Database connection closed")
            self._connection = None
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = True) -> Generator:
        """
        Veritabanı cursor'u için context manager
        
        Args:
            dict_cursor: True ise RealDictCursor kullan (sonuçlar dict olarak döner)
        
        Yields:
            PostgreSQL cursor nesnesi
        """
        conn = self.connect()
        cursor_factory = RealDictCursor if dict_cursor else None
        cursor = conn.cursor(cursor_factory=cursor_factory)
        
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("Database operation failed, rolled back", error=str(e))
            raise
        finally:
            cursor.close()
    
    def test_connection(self) -> bool:
        """
        Veritabanı bağlantısını test et
        
        Returns:
            True ise bağlantı başarılı
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                logger.info("Database connection test successful", result=result)
                return True
        except Exception as e:
            logger.error("Database connection test failed", error=str(e))
            return False
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

