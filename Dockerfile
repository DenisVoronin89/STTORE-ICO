# Используем официальный образ Python 3.13
FROM python:3.13-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Устанавливаем curl, build-essential и другие зависимости, включая libpq-dev для psycopg2
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*


# Копируем файл с зависимостями (requirements.txt)
COPY requirements.txt /app/

# Устанавливаем зависимости с помощью pip
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все остальные файлы проекта
COPY . /app/

# Открываем порты
EXPOSE 5432 8000

# Запускаем приложение
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--forwarded-allow-ips", "*"]