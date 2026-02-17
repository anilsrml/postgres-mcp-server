FROM python:3.12-slim

WORKDIR /app

# Bağımlılıkları kopyala ve yükle (cache katmanı)
COPY requirements.docker.txt .
RUN pip install --no-cache-dir -r requirements.docker.txt

# Proje dosyalarını kopyala
COPY . .

# MCP sunucusunu başlat
CMD ["python", "mcp_server.py"]
