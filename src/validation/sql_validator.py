"""SQL sorgu validasyonu ve güvenlik kontrolü"""

import re
import sqlparse
from sqlparse.sql import Token, TokenList
from sqlparse.tokens import Keyword, DML
from typing import Tuple, List, Optional
from .rules import (
    FORBIDDEN_KEYWORDS,
    FORBIDDEN_FUNCTIONS,
    ALLOWED_KEYWORDS,
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
    
    def __init__(self, strict_mode: bool = True):
        """
        SQL validator'ı başlat
        
        Args:
            strict_mode: True ise daha katı kontroller uygula
        """
        self.strict_mode = strict_mode
        logger.info("SQLValidator initialized", strict_mode=strict_mode)
    
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
            self._check_forbidden_keywords(sql)
            self._check_forbidden_functions(sql)
            self._check_only_select(sql)
            
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
    
    def _check_forbidden_keywords(self, sql: str):
        """Yasaklı anahtar kelimeleri kontrol et"""
        sql_upper = sql.upper()
        
        for keyword in FORBIDDEN_KEYWORDS:
            # Kelime sınırlarını kontrol et (örn: "SELECT" != "SELECTED")
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
    
    def _check_only_select(self, sql: str):
        """Sadece SELECT sorgusu olduğunu doğrula"""
        parsed = sqlparse.parse(sql)
        
        if not parsed:
            raise ValidationError("Geçersiz SQL sorgusu.")
        
        for statement in parsed:
            # İlk anlamlı token'ı bul
            first_token = None
            for token in statement.tokens:
                if not token.is_whitespace:
                    first_token = token
                    break
            
            if first_token is None:
                raise ValidationError("Boş SQL sorgusu.")
            
            # SELECT veya WITH ile başlamalı (CTE için)
            if first_token.ttype is Keyword and first_token.value.upper() in ('SELECT', 'WITH'):
                continue
            elif isinstance(first_token, TokenList):
                # TokenList içindeki ilk keyword'ü kontrol et
                found_valid = False
                for token in first_token.tokens:
                    if token.ttype is Keyword:
                        if token.value.upper() in ('SELECT', 'WITH'):
                            found_valid = True
                            break
                        else:
                            raise ValidationError(
                                f"Sadece SELECT sorguları izinlidir. "
                                f"Tespit edilen: {token.value}"
                            )
                
                # Eğer hiç keyword bulunamadıysa da hata verme, devam et
                if not found_valid:
                    # Basit bir kontrol: SQL'de SELECT var mı?
                    if 'SELECT' not in sql.upper():
                        raise ValidationError(
                            "Sorgu SELECT veya WITH ile başlamalıdır."
                        )
            # Eğer DML keyword ise kontrol et
            elif first_token.ttype is not None:
                # Diğer durumlarda basit kontrol
                if 'SELECT' not in sql.upper() and 'WITH' not in sql.upper():
                    raise ValidationError(
                        "Sorgu SELECT veya WITH ile başlamalıdır."
                    )
    
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

