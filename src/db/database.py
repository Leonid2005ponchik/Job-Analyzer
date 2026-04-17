import os #для работы с ОС
import sqlite3 #библиотека для кодировки БД

DB_PATH = 'data/database/vacancies.db' # определим сущесвует ли 

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) # берем путь к папке 
    return sqlite3.connect(DB_PATH) # открываем соединения с файлом 


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn: #управление транзакциями 
        cursor = conn.cursor() #инструмент для взаимодействия с базой данных
        # создаем таблицу с именем cities, создаем уникальный идентификатор, 
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cities(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT, 
                hh_code INTEGER
            )
        ''')
        # cоздаем таблицу работодателей, -//-
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS employers(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vacancies(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,   
                city_id INTEGER, 
                employer_id INTEGER,  
                salary_mid REAL, 
                currency TEXT,
                profession TEXT,
                published_at TEXT, 
                FOREIGN KEY (city_id) REFERENCES cities(id),
                FOREIGN KEY (employer_id) REFERENCES employers(id)
            )
        ''')

        try:
            cursor.execute("ALTER TABLE vacancies ADD COLUMN salary_from REAL")
        except Exception as e:
            print(f"Ошибка: {e}")

        try:
            cursor.execute("ALTER TABLE vacancies ADD COLUMN salary_to REAL")
        except Exception as e:
            print(f"Ошибка: {e}")

        cursor.execute("""
            UPDATE vacancies 
            SET salary_mid = CASE 
                WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2
                WHEN salary_from IS NOT NULL THEN salary_from
                WHEN salary_to IS NOT NULL THEN salary_to
                ELSE salary_mid
            END
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_vacancies_profession ON vacancies(profession)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_vacancies_city ON vacancies(city_id);")
        
        try: 
            cursor.execute("ALTER TABLE vacancies ADD COLUMN experience INTEGER DEFAULT 1")
        except Exception as e:
            print(f"Ошибка: {e}")
        
        conn.commit()


def get_or_create_city(cursor, city_name, hh_code = None):
    cursor.execute("SELECT id FROM cities WHERE name = ?", (city_name, ))
    result = cursor.fetchone() #пытаемся найти нужный курсор

    if result:
        return result[0]
    # если результат пустой вставляем новую запись 
    cursor.execute("INSERT INTO cities (name, hh_code) VALUES (?, ?)", (city_name, hh_code))
    return cursor.lastrowid

def get_or_create_employer(cursor ,employer_name):
    cursor.execute("SELECT id FROM employers WHERE name = ?", (employer_name, )) 
    result = cursor.fetchone()

    if result:
        return result[0]
    
    cursor.execute("INSERT INTO employers (name) VALUES (?)", (employer_name, ))
    return cursor.lastrowid

if __name__ == '__main__':
    init_db()