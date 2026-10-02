FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir Flask==3.1.2
COPY catalog_service/app.py app.py
USER 10001:10001
EXPOSE 8000
CMD ["python", "-m", "flask", "--app", "app", "run", "--host=0.0.0.0", "--port=8000"]
