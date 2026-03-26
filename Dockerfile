# ── Base ──────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── Variáveis de ambiente ──────────────────────────────────────────────────
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    DISPLAY=:99 \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# ── Chromium + ChromeDriver + dependências do sistema ─────────────────────
# Usamos chromium do apt em vez do google-chrome-stable para garantir
# que o chromedriver instalado seja sempre compatível com o browser.
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    xvfb \
    fonts-liberation \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libxss1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Diretório de trabalho ──────────────────────────────────────────────────
WORKDIR /app

# ── Dependências Python ────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Código da aplicação ────────────────────────────────────────────────────
COPY bot_streamlit.py .
COPY config/ config/

# ── Script de inicialização ────────────────────────────────────────────────
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── Porta do Streamlit ─────────────────────────────────────────────────────
EXPOSE 8571

ENTRYPOINT ["/entrypoint.sh"]