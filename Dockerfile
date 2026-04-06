# Build Stage for Frontend
FROM node:18-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Final Stage for Backend and Serving
FROM python:3.11-slim
WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências do Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do servidor
COPY server.py .
COPY .env.example .env

# Copia os arquivos estáticos do frontend
COPY --from=frontend-builder /app/frontend/dist ./static

# Script para rodar o servidor servindo o frontend
RUN echo "from fastapi.staticfiles import StaticFiles\nfrom server import app\napp.mount('/', StaticFiles(directory='static', html=True), name='static')\nif __name__ == '__main__':\n    import uvicorn\n    import os\n    port = int(os.environ.get('PORT', 8080))\n    uvicorn.run(app, host='0.0.0.0', port=port)" > main.py

EXPOSE 8080
CMD ["python", "main.py"]
