"""SQL sorgu validasyonu ve güvenlik kontrolü"""

import re
import sqlparse
from sqlparse.sql import Token, TokenList
from sqlparse.tokens import Keyword, DML
from typing import Tuple, List, Optional, Set
from .rules import (
    DDL_FORBIDDEN_KEYWORDS,
    DML_WRITE_KEYWORDS,
    FORBIDDEN_KEYWORDS,
    FORBIDDEN_FUNCTIONS,
    ALLOWED_KEYWORDS,
    WRITE_ALLOWED_KEYWORDS,
    MAX_JOINS,
    MAX_SUBQUERIES,
    MAX_UNIONS,
    MAX_QUERY_LENGTH,
)
from ..utils.logger import logger


class ValidationError(Exception):
    """SQL validasyon hatası"""
    pass


class SQLValidator:
    """SQL sorgu güvenlik validatörü"""
    
    def __init__(
        self,
        strict_mode: bool = True,
        allowed_operations: Optional[Set[str]] = None,
        writable_tables: Optional[Set[str]] = None,
    ):
        """
        SQL validator'ı başlat
        
        Args:
            strict_mode: True ise daha katı kontroller uygula
            allowed_operations: İzin verilen SQL işlem tipleri.
                None veya {"SELECT"} → sadece SELECT
                {"SELECT", "INSERT", "UPDATE", "DELETE"} → okuma + yazma
            writable_tables: Yazma izni verilen tablo isimleri (None = tümü izinli)
        """
        self.strict_mode = strict_mode
        self.allowed_operations = allowed_operations or {"SELECT"}
        self.writable_tables = writable_tables
        
        # Yazma modu aktif mi?
        self.write_enabled = bool(self.allowed_operations - {"SELECT"})
        
        logger.debug(
            "SQLValidator initialized",
            strict_mode=strict_mode,
            allowed_operations=list(self.allowed_operations),
            write_enabled=self.write_enabled,
        )
    
    def validate(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        SQL sorgusunu doğrula
        
        Args:
            sql: Doğrulanacak SQL sorgusu
        
        Returns:
            (is_valid, error_message) tuple'ı
        """
        try:
            # Temel kontroller
            self._check_length(sql)
            self._check_ddl_forbidden(sql)
            self._check_forbidden_functions(sql)
            self._check_allowed_operations(sql)
            
            # Yazma işlemlerinde ek güvenlik kontrolleri
            if self._is_write_query(sql):
                self._check_write_safety(sql)
            
            # Karmaşıklık kontrolleri
            if self.strict_mode:
                self._check_complexity(sql)
            
            # SQL syntax kontrolü
            self._check_syntax(sql)
            
            logger.info("SQL validation passed", sql=sql[:100])
            return True, None
            
        except ValidationError as e:
            logger.warning("SQL validation failed", error=str(e), sql=sql[:100])
            return False, str(e)
        except Exception as e:
            logger.error("Unexpected validation error", error=str(e))
            return False, f"Beklenmeyen doğrulama hatası: {str(e)}"
    
    def _check_length(self, sql: str):
        """Sorgu uzunluğunu kontrol et"""
        if len(sql) > MAX_QUERY_LENGTH:
            raise ValidationError(
                f"Sorgu çok uzun. Maksimum {MAX_QUERY_LENGTH} karakter olmalı."
            )
    
    def _check_ddl_forbidden(self, sql: str):
        """DDL ve tehlikeli komutları kontrol et (her zaman engellenir)"""
        sql_upper = sql.upper()
        
        for keyword in DDL_FORBIDDEN_KEYWORDS:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, sql_upper):
                raise ValidationError(
                    f"Yasaklı komut tespit edildi: {keyword}. "
                    f"Bu komut güvenlik nedeniyle her zaman engellenmiştir."
                )
        
        # Yazma modu kapalıysa DML write komutlarını da engelle
        if not self.write_enabled:
            for keyword in DML_WRITE_KEYWORDS:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, sql_upper):
                    raise ValidationError(
                        f"Yasaklı komut tespit edildi: {keyword}. "
                        f"Sadece SELECT sorguları çalıştırılabilir."
                    )
    
    def _check_forbidden_functions(self, sql: str):
        """Tehlikeli fonksiyonları kontrol et"""
        sql_lower = sql.lower()
        
        for func in FORBIDDEN_FUNCTIONS:
            if func.lower() in sql_lower:
                raise ValidationError(
                    f"Tehlikeli fonksiyon tespit edildi: {func}. "
                    f"Bu fonksiyon güvenlik nedeniyle yasaklanmıştır."
                )
    
    def _is_write_query(self, sql: str) -> bool:
        """Sorgunun yazma işlemi olup olmadığını kontrol et"""
        sql_upper = sql.strip().upper()
        return any(sql_upper.startswith(kw) for kw in ('INSERT', 'UPDATE', 'DELETE'))
    
    def _get_query_type(self, sql: str) -> str:
        """Sorgunun tipini döndür (SELECT, INSERT, UPDATE, DELETE)"""
        sql_upper = sql.strip().upper()
        for kw in ('INSERT', 'UPDATE', 'DELETE', 'SELECT', 'WITH'):
            if sql_upper.startswith(kw):
                return kw
        return "UNKNOWN"
    
    def _check_allowed_operations(self, sql: str):
        """İzin verilen işlem tiplerini kontrol et"""
        parsed = sqlparse.parse(sql)
        
        if not parsed:
            raise ValidationError("Geçersiz SQL sorgusu.")
        
        for statement in parsed:
            first_token = None
            for token in statement.tokens:
                if not token.is_whitespace:
                    first_token = token
                    break
            
            if first_token is None:
                raise ValidationError("Boş SQL sorgusu.")
            
            # İlk keyword'ü bul
            query_type = self._get_query_type(sql)
            
            if query_type == "UNKNOWN":
                raise ValidationError(
                    f"Tanınmayan SQL komutu. "
                    f"İzin verilen komutlar: {', '.join(sorted(self.allowed_operations))}"
                )
            
            # WITH (CTE) her durumda izinliyse SELECT gibi davran
            if query_type == "WITH":
                if "SELECT" not in self.allowed_operations:
                    raise ValidationError("SELECT sorguları izinli değil.")
                continue
            
            if query_type not in self.allowed_operations:
                raise ValidationError(
                    f"{query_type} komutu izinli değil. "
                    f"İzin verilen komutlar: {', '.join(sorted(self.allowed_operations))}"
                )
    
    def _check_write_safety(self, sql: str):
        """
        Yazma işlemleri için ek güvenlik kontrolleri:
        - UPDATE/DELETE'de WHERE zorunluluğu
        - Tablo izin listesi kontrolü
        """
        sql_upper = sql.upper().strip()
        query_type = self._get_query_type(sql)
        
        # 1. UPDATE ve DELETE'de WHERE zorunluluğu
        if query_type in ('UPDATE', 'DELETE'):
            if 'WHERE' not in sql_upper:
                raise ValidationError(
                    f"{query_type} sorgusunda WHERE koşulu zorunludur. "
                    f"Tüm satırları etkileyen sorgular güvenlik nedeniyle engellenmiştir."
                )
        
        # 2. Tablo izin listesi kontrolü
        if self.writable_tables is not None:
            target_table = self._extract_write_target_table(sql, query_type)
            if target_table and target_table.lower() not in {t.lower() for t in self.writable_tables}:
                raise ValidationError(
                    f"'{target_table}' tablosuna yazma izni yok. "
                    f"İzinli tablolar: {', '.join(sorted(self.writable_tables))}"
                )
    
    def _extract_write_target_table(self, sql: str, query_type: str) -> Optional[str]:
        """Yazma sorgusundan hedef tablo adını çıkar"""
        sql_clean = ' '.join(sql.split())  # Fazla boşlukları temizle
        
        try:
            if query_type == 'INSERT':
                # INSERT INTO table_name ...
                match = re.search(r'INSERT\s+INTO\s+(\w+)', sql_clean, re.IGNORECASE)
                return match.group(1) if match else None
            
            elif query_type == 'UPDATE':
                # UPDATE table_name SET ...
                match = re.search(r'UPDATE\s+(\w+)', sql_clean, re.IGNORECASE)
                return match.group(1) if match else None
            
            elif query_type == 'DELETE':
                # DELETE FROM table_name ...
                match = re.search(r'DELETE\s+FROM\s+(\w+)', sql_clean, re.IGNORECASE)
                return match.group(1) if match else None
        except Exception:
            pass
        
        return None
    
    def _check_complexity(self, sql: str):
        """Sorgu karmaşıklığını kontrol et"""
        sql_upper = sql.upper()
        
        # JOIN sayısını kontrol et
        join_count = len(re.findall(r'\bJOIN\b', sql_upper))
        if join_count > MAX_JOINS:
            raise ValidationError(
                f"Çok fazla JOIN kullanıldı ({join_count}). "
                f"Maksimum {MAX_JOINS} JOIN kullanabilirsiniz."
            )
        
        # Alt sorgu sayısını kontrol et
        subquery_count = sql.count('(') - sql.count(')')
        if abs(subquery_count) > MAX_SUBQUERIES:
            raise ValidationError(
                f"Çok fazla alt sorgu kullanıldı. "
                f"Maksimum {MAX_SUBQUERIES} alt sorgu kullanabilirsiniz."
            )
        
        # UNION sayısını kontrol et
        union_count = len(re.findall(r'\bUNION\b', sql_upper))
        if union_count > MAX_UNIONS:
            raise ValidationError(
                f"Çok fazla UNION kullanıldı ({union_count}). "
                f"Maksimum {MAX_UNIONS} UNION kullanabilirsiniz."
            )
    
    def _check_syntax(self, sql: str):
        """Temel SQL syntax kontrolü"""
        parsed = sqlparse.parse(sql)
        
        if not parsed:
            raise ValidationError("SQL sorgusu parse edilemedi.")
        
        # Parantez dengesini kontrol et
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            raise ValidationError(
                f"Parantez dengesi hatalı. "
                f"Açılan: {open_parens}, Kapanan: {close_parens}"
            )
        
        # Tırnak dengesini kontrol et
        single_quotes = sql.count("'")
        if single_quotes % 2 != 0:
            raise ValidationError("Tek tırnak dengesi hatalı.")
    
    def sanitize_sql(self, sql: str) -> str:
        """
        SQL sorgusunu temizle ve formatla
        
        Args:
            sql: Temizlenecek SQL sorgusu
        
        Returns:
            Temizlenmiş ve formatlanmış SQL
        """
        # Fazla boşlukları temizle
        sql = ' '.join(sql.split())
        
        # SQL'i formatla
        formatted = sqlparse.format(
            sql,
            reindent=True,
            keyword_case='upper',
            strip_comments=True,
        )
        
        return formatted.strip()
    
    def extract_table_names(self, sql: str) -> List[str]:
        """
        SQL sorgusundan tablo isimlerini çıkar
        
        Args:
            sql: SQL sorgusu
        
        Returns:
            Tablo isimleri listesi
        """
        parsed = sqlparse.parse(sql)
        tables = []
        
        for statement in parsed:
            for token in statement.tokens:
                if isinstance(token, TokenList):
                    tables.extend(self._extract_tables_from_token(token))
        
        return list(set(tables))  # Tekrarları kaldır
    
    def _extract_tables_from_token(self, token: TokenList) -> List[str]:
        """Token'dan tablo isimlerini çıkar (yardımcı metod)"""
        tables = []
        
        if token.ttype is None:
            for item in token.tokens:
                if isinstance(item, TokenList):
                    tables.extend(self._extract_tables_from_token(item))
                elif item.ttype is not Keyword and not item.is_whitespace:
                    # FROM veya JOIN'den sonraki identifier'ları yakala
                    value = item.value.strip('`"[]')
                    if value and not value.upper() in ALLOWED_KEYWORDS:
                        tables.append(value)
        
        return tables
