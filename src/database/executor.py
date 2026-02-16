"""Güvenli SQL sorgu çalıştırma"""

import signal
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from .connection import DatabaseConnection
from ..validation.sql_validator import SQLValidator, ValidationError
from ..config import settings
from ..utils.logger import logger


class QueryExecutionError(Exception):
    """Sorgu çalıştırma hatası"""
    pass


class TimeoutError(Exception):
    """Sorgu zaman aşımı hatası"""
    pass


class QueryExecutor:
    """Güvenli SQL sorgu çalıştırıcı"""
    
    def __init__(
        self,
        db_connection: DatabaseConnection,
        validator: Optional[SQLValidator] = None,
        timeout: int = None,
        max_rows: int = None,
    ):
        """
        Query executor'ı başlat
        
        Args:
            db_connection: Veritabanı bağlantısı
            validator: SQL validator (None ise yeni oluşturulur)
            timeout: Sorgu zaman aşımı (saniye)
            max_rows: Maksimum döndürülecek satır sayısı
        """
        self.db = db_connection
        self.validator = validator or SQLValidator(strict_mode=True)
        self.timeout = timeout or settings.max_query_timeout
        self.max_rows = max_rows or settings.max_result_rows
        logger.info(
            "QueryExecutor initialized",
            timeout=self.timeout,
            max_rows=self.max_rows,
        )
    
    def execute_query(
        self,
        sql: str,
        validate: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        SQL sorgusunu güvenli şekilde çalıştır
        
        Args:
            sql: Çalıştırılacak SQL sorgusu
            validate: True ise önce validasyon yap
        
        Returns:
            Sorgu sonuçları (dict listesi)
        
        Raises:
            ValidationError: Validasyon hatası
            QueryExecutionError: Sorgu çalıştırma hatası
            TimeoutError: Zaman aşımı hatası
        """
        # Validasyon
        if validate:
            is_valid, error_msg = self.validator.validate(sql)
            if not is_valid:
                logger.warning("Query validation failed", error=error_msg)
                raise ValidationError(error_msg)
        
        # SQL'i temizle ve formatla
        sql = self.validator.sanitize_sql(sql)
        
        # LIMIT ekle (yoksa)
        sql = self._ensure_limit(sql)
        
        logger.info("Executing query", sql=sql[:200])
        
        try:
            # Sorguyu çalıştır (timeout ile)
            results = self._execute_with_timeout(sql)
            
            logger.info("Query executed successfully", row_count=len(results))
            return results
            
        except Exception as e:
            logger.error("Query execution failed", error=str(e), sql=sql[:200])
            raise QueryExecutionError(f"Sorgu çalıştırma hatası: {str(e)}")
    
    def _ensure_limit(self, sql: str) -> str:
        """
        SQL sorgusuna LIMIT ekle (yoksa)
        
        Args:
            sql: SQL sorgusu
        
        Returns:
            LIMIT eklenmiş SQL
        """
        sql_upper = sql.upper()
        
        # Zaten LIMIT varsa dokunma
        if 'LIMIT' in sql_upper:
            return sql
        
        # LIMIT ekle
        return f"{sql.rstrip(';')} LIMIT {self.max_rows};"
    
    def _execute_with_timeout(self, sql: str) -> List[Dict[str, Any]]:
        """
        Sorguyu timeout ile çalıştır
        
        Args:
            sql: SQL sorgusu
        
        Returns:
            Sorgu sonuçları
        
        Raises:
            TimeoutError: Zaman aşımı durumunda
        """
        try:
            with self.db.get_cursor() as cursor:
                # PostgreSQL statement timeout ayarla
                cursor.execute(f"SET statement_timeout = {self.timeout * 1000};")
                
                # Sorguyu çalıştır
                cursor.execute(sql)
                results = cursor.fetchall()
                
                # Dict listesine çevir
                return [dict(row) for row in results]
                
        except Exception as e:
            error_msg = str(e).lower()
            
            # Timeout hatası kontrolü
            if 'timeout' in error_msg or 'canceling statement' in error_msg:
                raise TimeoutError(
                    f"Sorgu {self.timeout} saniye içinde tamamlanamadı."
                )
            
            # Diğer hatalar
            raise
    
    def execute_and_format(
        self,
        sql: str,
        format_type: str = "dict",
    ) -> Any:
        """
        Sorguyu çalıştır ve belirtilen formatta döndür
        
        Args:
            sql: SQL sorgusu
            format_type: Sonuç formatı ("dict", "list", "count")
        
        Returns:
            Formatlanmış sonuçlar
        """
        results = self.execute_query(sql)
        
        if format_type == "dict":
            return results
        elif format_type == "list":
            # Sadece değerleri döndür
            return [list(row.values()) for row in results]
        elif format_type == "count":
            return len(results)
        else:
            return results
    
    def test_query(self, sql: str) -> Dict[str, Any]:
        """
        Sorguyu test et (çalıştırmadan validasyon ve analiz)
        
        Args:
            sql: Test edilecek SQL sorgusu
        
        Returns:
            Test sonuçları
        """
        test_result = {
            "valid": False,
            "error": None,
            "sanitized_sql": None,
            "estimated_complexity": None,
            "tables": [],
        }
        
        # Validasyon
        is_valid, error_msg = self.validator.validate(sql)
        test_result["valid"] = is_valid
        test_result["error"] = error_msg
        
        if is_valid:
            # SQL'i temizle
            test_result["sanitized_sql"] = self.validator.sanitize_sql(sql)
            
            # Tablo isimlerini çıkar
            test_result["tables"] = self.validator.extract_table_names(sql)
            
            # Karmaşıklık tahmini
            test_result["estimated_complexity"] = self._estimate_complexity(sql)
        
        return test_result
    
    def _estimate_complexity(self, sql: str) -> str:
        """
        Sorgu karmaşıklığını tahmin et
        
        Args:
            sql: SQL sorgusu
        
        Returns:
            Karmaşıklık seviyesi ("low", "medium", "high")
        """
        sql_upper = sql.upper()
        
        complexity_score = 0
        
        # JOIN sayısı
        complexity_score += sql_upper.count('JOIN') * 2
        
        # Alt sorgu sayısı
        complexity_score += sql_upper.count('SELECT') - 1
        
        # Aggregate fonksiyonlar
        complexity_score += sql_upper.count('GROUP BY')
        complexity_score += sql_upper.count('HAVING')
        
        # UNION
        complexity_score += sql_upper.count('UNION') * 3
        
        if complexity_score <= 2:
            return "low"
        elif complexity_score <= 6:
            return "medium"
        else:
            return "high"
    
    def get_query_stats(self, sql: str) -> Dict[str, Any]:
        """
        Sorgu istatistiklerini getir (EXPLAIN kullanarak)
        
        Args:
            sql: Analiz edilecek SQL sorgusu
        
        Returns:
            Sorgu istatistikleri
        """
        try:
            explain_sql = f"EXPLAIN (FORMAT JSON) {sql}"
            
            with self.db.get_cursor() as cursor:
                cursor.execute(explain_sql)
                result = cursor.fetchone()
                
                if result:
                    return result[0] if isinstance(result, tuple) else result
                
                return {}
                
        except Exception as e:
            logger.error("Failed to get query stats", error=str(e))
            return {"error": str(e)}

