# Указываем базовый образ
FROM python:3.9-slim

# Устанавливаем необходимые пакеты (например, GPG)
RUN apt-get update && apt-get install -y gnupg && rm -rf /var/lib/apt/lists/*

# Добавляем рабочую директорию в контейнере
WORKDIR /app

# Копируем локальные файлы в контейнер
COPY . /app

# Указываем команду, которая выполнится при запуске контейнера
CMD ["python", "app.py"]
