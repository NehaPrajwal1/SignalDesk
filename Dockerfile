FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 SIGNALDESK_DB_PATH=/data/signaldesk.db
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home appuser && mkdir /data && chown appuser /data
COPY --chown=appuser:appuser api.py core.py app.py platform_ops.py mcp_server.py slack_bot.py simulate.py access.py alertmanager.py manage.py database.py ./
USER appuser
EXPOSE 8000 8501 8001
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
