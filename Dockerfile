# Используем официальный образ Python 3.9 (полный, не slim)
FROM python:3.9

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы проекта
COPY bot.py .
COPY .env .
COPY finance.db .

# Указываем команду для запуска бота
CMD ["python", "bot.py"]