# -*- coding: utf-8 -*-
from flask import Flask

# Создаем объект Flask
app = Flask(__name__)

# Настраиваем маршрут для корневого URL
@app.route("/")
def home():
    return "Hello from Docker!"

# Запускаем приложение
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
