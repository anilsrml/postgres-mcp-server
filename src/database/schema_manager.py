"""Veritabanı schema analizi ve metadata yönetimi"""

from typing import List, Dict, Any, Optional
from .connection import DatabaseConnection
from ..utils.logger import logger


class SchemaManager:
    """Veritabanı schema'sını analiz eden ve metadata sağlayan sınıf"""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Schema manager'ı başlat
        
        Args:
            db_connection: Veritabanı bağlantı nesnesi
        """
        self.db = db_connection
        self._schema_cache: Optional[Dict[str, Any]] = None
        logger.info("SchemaManager initialized")
    
    def get_all_tables(self) -> List[str]:
        """
        Veritabanındaki tüm tabloları listele
        
        Returns:
            Tablo isimleri listesi
        """
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            tables = [row['table_name'] for row in results]
            logger.info("Retrieved tables", count=len(tables), tables=tables)
            return tables
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Belirli bir tablonun kolonlarını ve özelliklerini getir
        
        Args:
            table_name: Tablo adı
        
        Returns:
            Kolon bilgileri listesi
        """
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            logger.info("Retrieved columns", table=table_name, count=len(columns))
            return [dict(col) for col in columns]
    
    def get_table_comment(self, table_name: str) -> Optional[str]:
        """
        Tablo açıklamasını getir
        
        Args:
            table_name: Tablo adı
        
        Returns:
            Tablo açıklaması (varsa)
        """
        query = """
            SELECT obj_description(oid) as comment
            FROM pg_class
            WHERE relname = %s AND relnamespace = 'public'::regnamespace;
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()
            return result['comment'] if result and result['comment'] else None
    
    def get_column_comments(self, table_name: str) -> Dict[str, str]:
        """
        Tablo kolonlarının açıklamalarını getir
        
        Args:
            table_name: Tablo adı
        
        Returns:
            Kolon adı -> açıklama dictionary'si
        """
        query = """
            SELECT 
                a.attname as column_name,
                col_description(a.attrelid, a.attnum) as comment
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            WHERE c.relname = %s 
            AND a.attnum > 0 
            AND NOT a.attisdropped
            AND col_description(a.attrelid, a.attnum) IS NOT NULL;
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (table_name,))
            results = cursor.fetchall()
            return {row['column_name']: row['comment'] for row in results}
    
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Tablonun foreign key ilişkilerini getir
        
        Args:
            table_name: Tablo adı
        
        Returns:
            Foreign key bilgileri listesi
        """
        query = """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = %s;
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (table_name,))
            results = cursor.fetchall()
            return [dict(row) for row in results]
    
    def get_primary_key(self, table_name: str) -> Optional[str]:
        """
        Tablonun primary key kolonunu getir
        
        Args:
            table_name: Tablo adı
        
        Returns:
            Primary key kolon adı
        """
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary;
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()
            return result['attname'] if result else None
    
    def get_sample_values(self, table_name: str, column_name: str, limit: int = 5) -> List[Any]:
        """
        Bir kolondan örnek değerler getir
        
        Args:
            table_name: Tablo adı
            column_name: Kolon adı
            limit: Maksimum örnek sayısı
        
        Returns:
            Örnek değerler listesi
        """
        # SQL injection'a karşı tablo ve kolon adlarını kontrol et
        if not table_name.replace('_', '').isalnum() or not column_name.replace('_', '').isalnum():
            logger.warning("Invalid table or column name", table=table_name, column=column_name)
            return []
        
        query = f"""
            SELECT DISTINCT "{column_name}"
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            LIMIT %s;
        """
        
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (limit,))
                results = cursor.fetchall()
                return [row[column_name] for row in results]
        except Exception as e:
            logger.error("Failed to get sample values", error=str(e))
            return []
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Tablodaki satır sayısını getir
        
        Args:
            table_name: Tablo adı
        
        Returns:
            Satır sayısı
        """
        if not table_name.replace('_', '').isalnum():
            return 0
        
        query = f'SELECT COUNT(*) as count FROM "{table_name}";'
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result['count'] if result else 0
    
    def get_full_schema(self, include_samples: bool = True) -> Dict[str, Any]:
        """
        Tüm veritabanı schema'sını detaylı şekilde getir
        
        Args:
            include_samples: Örnek değerleri dahil et
        
        Returns:
            Tam schema bilgisi
        """
        if self._schema_cache:
            return self._schema_cache
        
        schema = {}
        tables = self.get_all_tables()
        
        for table_name in tables:
            table_info = {
                "name": table_name,
                "comment": self.get_table_comment(table_name),
                "row_count": self.get_table_row_count(table_name),
                "primary_key": self.get_primary_key(table_name),
                "columns": [],
                "foreign_keys": self.get_foreign_keys(table_name),
            }
            
            columns = self.get_table_columns(table_name)
            column_comments = self.get_column_comments(table_name)
            
            for col in columns:
                col_info = {
                    "name": col['column_name'],
                    "type": col['data_type'],
                    "nullable": col['is_nullable'] == 'YES',
                    "default": col['column_default'],
                    "comment": column_comments.get(col['column_name']),
                }
                
                if include_samples:
                    col_info['sample_values'] = self.get_sample_values(
                        table_name, col['column_name'], limit=3
                    )
                
                table_info['columns'].append(col_info)
            
            schema[table_name] = table_info
        
        self._schema_cache = schema
        logger.info("Full schema retrieved", table_count=len(schema))
        return schema
    
    def get_schema_for_llm(self) -> str:
        """
        LLM için optimize edilmiş schema açıklaması oluştur
        
        Returns:
            LLM'e verilecek schema metni
        """
        schema = self.get_full_schema(include_samples=True)
        
        schema_text = "# Veritabanı Schema Bilgisi\n\n"
        
        for table_name, table_info in schema.items():
            schema_text += f"## Tablo: {table_name}\n"
            
            if table_info['comment']:
                schema_text += f"Açıklama: {table_info['comment']}\n"
            
            schema_text += f"Satır Sayısı: {table_info['row_count']}\n"
            
            if table_info['primary_key']:
                schema_text += f"Primary Key: {table_info['primary_key']}\n"
            
            schema_text += "\n### Kolonlar:\n"
            for col in table_info['columns']:
                schema_text += f"- **{col['name']}** ({col['type']})"
                
                if not col['nullable']:
                    schema_text += " [NOT NULL]"
                
                if col['comment']:
                    schema_text += f" - {col['comment']}"
                
                if col.get('sample_values'):
                    samples = ", ".join(str(v) for v in col['sample_values'][:3])
                    schema_text += f"\n  Örnek değerler: {samples}"
                
                schema_text += "\n"
            
            if table_info['foreign_keys']:
                schema_text += "\n### İlişkiler:\n"
                for fk in table_info['foreign_keys']:
                    schema_text += f"- {fk['column_name']} → {fk['foreign_table_name']}.{fk['foreign_column_name']}\n"
            
            schema_text += "\n---\n\n"
        
        return schema_text
    
    def clear_cache(self):
        """Schema cache'ini temizle"""
        self._schema_cache = None
        logger.info("Schema cache cleared")

