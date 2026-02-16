# PostgreSQL DBQA with MCP (Model Context Protocol)

Bu proje, **PostgreSQL** veritabanınızı **Yapay Zeka (AI)** modellerine (Claude, Cursor vb.) bağlayan güvenli bir köprüdür. 

**Model Context Protocol (MCP)** kullanarak, AI modellerinin veritabanı şemasını anlamasını ve güvenli bir şekilde sorgulamasını sağlar.

## 🚀 Özellikler

*   **Şema Analizi:** Veritabanı tablolarını, sütunlarını ve ilişkilerini AI'ya otomatik olarak tanıtır.
*   **Doğal Dil Sorgulama:** AI, doğal dildeki soruları SQL'e çevirir ve sonuçları getirir.
*   **Güvenlik:** 
    *   Sadece `SELECT` sorgularına izin verir.
    *   `INSERT`, `UPDATE`, `DELETE`, `DROP` gibi veri değiştiren komutları engeller.
    *   Sorgu karmaşıklığını ve satır sayısını sınırlar.

## 🛠️ Kurulum

1.  Gerekli bağımlılıkları yükleyin:
    ```bash
    pip install -r requirements.txt
    ```

2.  `.env` dosyasını oluşturun ve veritabanı bilgilerinizi girin:
    ```env
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=veritabani_adi
    DB_USER=kullanici_adi
    DB_PASSWORD=sifre
    ```

## 💻 Kullanım

MCP sunucusunu başlatmak için:

```bash
python mcp_server.py
```

### Cursor veya Claude Desktop ile Bağlantı

AI asistanınızın konfigürasyon dosyasına (örneğin `claude_desktop_config.json` veya Cursor ayarları) şu bilgileri ekleyin:

```json
{
  "mcpServers": {
    "postgres-dbq": {
      "command": "python",
      "args": ["/tam/yol/mcp_server.py"]
    }
  }
}
```

## 📂 Proje Yapısı

*   `mcp_server.py`: Ana MCP sunucu dosyası.
*   `src/database/`: Veritabanı bağlantısı ve şema yönetimi.
*   `src/validation/`: SQL güvenlik kontrolleri (sadece SELECT izni vb.).
*   `src/utils/`: Yardımcı araçlar ve loglama.

## ⚠️ Güvenlik Notu

Bu araç sadece **okuma amaçlı** (read-only) kullanım için tasarlanmıştır. Kritik veritabanlarında kullanmadan önce bir salt okunur (read-only) veritabanı kullanıcısı ile bağlanmanız önerilir.
