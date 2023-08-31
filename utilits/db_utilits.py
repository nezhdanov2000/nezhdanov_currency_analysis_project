import psycopg2
from utilits.enums import SettingKeys
import os

def read_settings():
    result = {
        SettingKeys.INTERVAL_MINUTES.value: 0,
        SettingKeys.JAR_PATH.value: '',
        SettingKeys.SYMBOLS.value: '',
        SettingKeys.OBJECT_STORAGE.value: ''
    }

    # Чтение параметров подключения к вашей базе данных из окружающих переменных
    connection_params = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': int(os.environ.get('DB_PORT', 9001)),
        'user': os.environ.get('DB_USER', 'your_user'),
        'password': os.environ.get('DB_PASSWORD', 'your_password'),
        'database': os.environ.get('DB_DATABASE', 'your_database')
    }

    # Устанавливаем соединение с ClickHouse
    connection = psycopg2.connect(**connection_params)

    try:
        with connection.cursor() as cursor:
            # Выполняем SQL-запрос для получения настроек
            cursor.execute("SELECT key, value FROM de.settings")

            # Получаем результат запроса
            settings = cursor.fetchall()

            # Обновляем результат в соответствии с настройками из базы данных
            for s in settings:
                result[s[0]] = s[1]
    finally:
        # Закрываем соединение
        connection.close()

    return result
