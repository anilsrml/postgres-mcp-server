"""SQL validasyon kuralları"""

from typing import Set

# Yasaklı SQL komutları (blacklist)
FORBIDDEN_KEYWORDS: Set[str] = {
    # Veri değiştirme komutları
    'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'DROP', 'CREATE', 'ALTER',
    'REPLACE', 'MERGE', 'UPSERT',
    
    # Veritabanı yapısı değişikliği
    'CREATE', 'ALTER', 'DROP', 'RENAME', 'TRUNCATE',
    
    # Yetki ve kullanıcı yönetimi
    'GRANT', 'REVOKE', 'DENY',
    
    # Transaction kontrolü (güvenlik için)
    'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'BEGIN', 'START TRANSACTION',
    
    # Tehlikeli fonksiyonlar
    'EXECUTE', 'EXEC', 'CALL',
    
    # Dosya işlemleri
    'COPY', 'LOAD', 'IMPORT', 'EXPORT',
}

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

# İzin verilen SQL komutları (whitelist)
ALLOWED_KEYWORDS: Set[str] = {
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
    'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL',
    'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'DISTINCT',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CAST', 'CASE', 'WHEN', 'THEN',
    'ELSE', 'END', 'UNION', 'INTERSECT', 'EXCEPT', 'WITH', 'RECURSIVE',
}

# Maksimum sorgu karmaşıklığı limitleri
MAX_JOINS = 10  # Maksimum JOIN sayısı
MAX_SUBQUERIES = 5  # Maksimum alt sorgu sayısı
MAX_UNIONS = 3  # Maksimum UNION sayısı
MAX_QUERY_LENGTH = 5000  # Maksimum sorgu uzunluğu (karakter)

# Zaman aşımı ve sonuç limitleri
DEFAULT_TIMEOUT = 30  # Saniye
DEFAULT_ROW_LIMIT = 1000  # Maksimum döndürülecek satır sayısı

