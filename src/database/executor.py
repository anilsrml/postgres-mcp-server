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
        max_write_rows: int = None,
    ):
        """
        Query executor'ı başlat
        
        Args:
            db_connection: Veritabanı bağlantısı
            validator: SQL validator (None ise yeni oluşturulur)
            timeout: Sorgu zaman aşımı (saniye)
            max_rows: Maksimum döndürülecek satır sayısı (SELECT)
            max_write_rows: Yazma işlemlerinde etkilenecek maksimum satır sayısı
        """
        self.db = db_connection
        self.validator = validator or SQLValidator(strict_mode=True)
        self.timeout = timeout or settings.max_query_timeout
        self.max_rows = max_rows or settings.max_result_rows
        self.max_write_rows = max_write_rows or settings.max_write_rows
        logger.debug(
            "QueryExecutor initialized",
            timeout=self.timeout,
            max_rows=self.max_rows,
            max_write_rows=self.max_write_rows,
        )
    
    def execute_query(
        self,
        sql: str,
        validate: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        SQL sorgusunu güvenli şekilde çalıştır (SELECT)
        
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
    
    def preview_write(
        self,
        sql: str,
        validate: bool = True,
    ) -> Dict[str, Any]:
        """
        Yazma sorgusunun dry-run preview'ını döndür (çalıştırmadan).
        
        EXPLAIN kullanarak kaç satır etkileneceğini tahmin eder,
        sorguyu valide eder, ve onay için özet döndürür.
        
        Args:
            sql: INSERT, UPDATE veya DELETE sorgusu
            validate: True ise önce validasyon yap
        
        Returns:
            Preview bilgisi dictionary'si
        """
        preview = {
            "valid": False,
            "query_type": None,
            "sanitized_sql": None,
            "target_table": None,
            "estimated_rows": None,
            "error": None,
        }
        
        # Validasyon
        if validate:
            is_valid, error_msg = self.validator.validate(sql)
            if not is_valid:
                preview["error"] = error_msg
                return preview
        
        preview["valid"] = True
        
        # SQL'i temizle
        sanitized = self.validator.sanitize_sql(sql)
        preview["sanitized_sql"] = sanitized
        
        # Sorgu tipini belirle
        query_type = self.validator._get_query_type(sql)
        preview["query_type"] = query_type
        
        # Hedef tabloyu belirle
        target_table = self.validator._extract_write_target_table(sql, query_type)
        preview["target_table"] = target_table
        
        # EXPLAIN ile etkilenecek satır sayısını tahmin et
        try:
            estimated = self._estimate_affected_rows(sanitized)
            preview["estimated_rows"] = estimated
            
            # Satır limiti kontrolü
            if estimated is not None and estimated > self.max_write_rows:
                preview["valid"] = False
                preview["error"] = (
                    f"Bu sorgu tahminen {estimated} satırı etkileyecek. "
                    f"Maksimum izin verilen: {self.max_write_rows} satır. "
                    f"Lütfen WHERE koşulunu daraltın."
                )
        except Exception as e:
            logger.warning("Could not estimate affected rows", error=str(e))
            preview["estimated_rows"] = None
        
        logger.info(
            "Write preview generated",
            query_type=query_type,
            target_table=target_table,
            estimated_rows=preview["estimated_rows"],
        )
        
        return preview
    
    def execute_write(
        self,
        sql: str,
        validate: bool = True,
    ) -> Dict[str, Any]:
        """
        Yazma sorgusunu çalıştır (INSERT, UPDATE, DELETE).
        
        Önce preview_write() ile onay alındıktan sonra bu metot
        çağrılmalıdır.
        
        Args:
            sql: INSERT, UPDATE veya DELETE sorgusu
            validate: True ise önce validasyon yap
        
        Returns:
            Sonuç dictionary'si (affected_rows, query_type, target_table)
        
        Raises:
            ValidationError: Validasyon hatası
            QueryExecutionError: Sorgu çalıştırma hatası
        """
        # Validasyon
        if validate:
            is_valid, error_msg = self.validator.validate(sql)
            if not is_valid:
                raise ValidationError(error_msg)
        
        # SQL'i temizle
        sanitized = self.validator.sanitize_sql(sql)
        query_type = self.validator._get_query_type(sql)
        target_table = self.validator._extract_write_target_table(sql, query_type)
        
        # Etkilenecek satır kontrolü (son güvenlik katmanı)
        estimated = self._estimate_affected_rows(sanitized)
        if estimated is not None and estimated > self.max_write_rows:
            raise ValidationError(
                f"Bu sorgu tahminen {estimated} satırı etkileyecek. "
                f"Maksimum izin verilen: {self.max_write_rows} satır."
            )
        
        logger.info(
            "Executing write query",
            query_type=query_type,
            target_table=target_table,
            sql=sanitized[:200],
        )
        
        try:
            with self.db.get_cursor() as cursor:
                # Timeout ayarla
                cursor.execute(f"SET statement_timeout = {self.timeout * 1000};")
                
                # Sorguyu çalıştır
                cursor.execute(sanitized)
                affected_rows = cursor.rowcount
            
            result = {
                "success": True,
                "affected_rows": affected_rows,
                "query_type": query_type,
                "target_table": target_table,
            }
            
            logger.info(
                "Write query executed successfully",
                affected_rows=affected_rows,
                query_type=query_type,
                target_table=target_table,
            )
            
            return result
            
        except Exception as e:
            error_msg = str(e).lower()
            
            if 'timeout' in error_msg or 'canceling statement' in error_msg:
                raise TimeoutError(
                    f"Sorgu {self.timeout} saniye içinde tamamlanamadı."
                )
            
            logger.error("Write query failed", error=str(e), sql=sanitized[:200])
            raise QueryExecutionError(f"Yazma sorgusu hatası: {str(e)}")
    
    def _estimate_affected_rows(self, sql: str) -> Optional[int]:
        """
        EXPLAIN kullanarak etkilenecek satır sayısını tahmin et.
        
        Args:
            sql: SQL sorgusu
        
        Returns:
            Tahmini etkilenecek satır sayısı (None ise tahmin yapılamadı)
        """
        try:
            explain_sql = f"EXPLAIN (FORMAT JSON) {sql}"
            
            with self.db.get_cursor() as cursor:
                cursor.execute(explain_sql)
                result = cursor.fetchone()
                
                if result:
                    # EXPLAIN JSON çıktısından Plan Rows'u al
                    plan_data = result.get('QUERY PLAN', result) if isinstance(result, dict) else result
                    if isinstance(plan_data, list) and len(plan_data) > 0:
                        plan = plan_data[0].get('Plan', {})
                        return plan.get('Plan Rows', None)
                    elif isinstance(plan_data, dict):
                        plan = plan_data.get('Plan', {})
                        return plan.get('Plan Rows', None)
            
            return None
            
        except Exception as e:
            logger.warning("Failed to estimate affected rows", error=str(e))
            return None
    
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
