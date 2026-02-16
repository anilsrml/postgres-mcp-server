"""SQL validasyon kuralları"""

from typing import Set

# ==========================================
# DDL Yasaklı Komutlar (her zaman engellenir)
# ==========================================
DDL_FORBIDDEN_KEYWORDS: Set[str] = {
    # Veritabanı yapısı değişikliği
    'CREATE', 'ALTER', 'DROP', 'RENAME', 'TRUNCATE',
    
    # Yetki ve kullanıcı yönetimi
    'GRANT', 'REVOKE', 'DENY',
    
    # Transaction kontrolü (güvenlik için)
    'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'BEGIN', 'START TRANSACTION',
    
    # Tehlikeli çalıştırma komutları
    'EXECUTE', 'EXEC', 'CALL',
    
    # Dosya işlemleri
    'COPY', 'LOAD', 'IMPORT', 'EXPORT',
}

# ==========================================
# DML Yazma Komutları (izin kontrolüne tabi)
# ==========================================
DML_WRITE_KEYWORDS: Set[str] = {
    'INSERT', 'UPDATE', 'DELETE',
    'REPLACE', 'MERGE', 'UPSERT',
}

# Geriye uyumluluk: Eski FORBIDDEN_KEYWORDS (sadece okuma modunda hepsi yasak)
FORBIDDEN_KEYWORDS: Set[str] = DDL_FORBIDDEN_KEYWORDS | DML_WRITE_KEYWORDS

# Tehlikeli fonksiyonlar
FORBIDDEN_FUNCTIONS: Set[str] = {
    'pg_read_file',
    'pg_write_file',
    'pg_ls_dir',
    'pg_sleep',
    'lo_import',
    'lo_export',
    'dblink',
    'dblink_exec',
}

# İzin verilen SQL komutları (whitelist - okuma işlemleri)
ALLOWED_KEYWORDS: Set[str] = {
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
    'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL',
    'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'DISTINCT',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CAST', 'CASE', 'WHEN', 'THEN',
    'ELSE', 'END', 'UNION', 'INTERSECT', 'EXCEPT', 'WITH', 'RECURSIVE',
}

# Yazma modunda ek izin verilen komutlar
WRITE_ALLOWED_KEYWORDS: Set[str] = ALLOWED_KEYWORDS | {
    'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
    'RETURNING', 'DEFAULT', 'ON CONFLICT', 'DO',
}

# Maksimum sorgu karmaşıklığı limitleri
MAX_JOINS = 10  # Maksimum JOIN sayısı
MAX_SUBQUERIES = 5  # Maksimum alt sorgu sayısı
MAX_UNIONS = 3  # Maksimum UNION sayısı
MAX_QUERY_LENGTH = 5000  # Maksimum sorgu uzunluğu (karakter)

# Zaman aşımı ve sonuç limitleri
DEFAULT_TIMEOUT = 30  # Saniye
DEFAULT_ROW_LIMIT = 1000  # Maksimum döndürülecek satır sayısı

# Yazma işlemi limitleri
MAX_WRITE_ROWS = 100  # Tek sorguda etkilenebilecek maksimum satır sayısı
