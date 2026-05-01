import requests #Библиотека http - клиента 
import time #для ожидания 
import numpy as np #для научных вычислений 
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from src.db.database import get_connection, get_or_create_city, get_or_create_employer #импортируем базу данных 
import traceback
from src.ml.utils import extract_exp
url = 'https://api.hh.ru/vacancies' #url hh ru 


EXCHANGE_RATES = {
            'USD': 90,    # 1 доллар ≈ 90 руб
            'EUR': 98,    # 1 евро ≈ 98 руб
            'KZT': 0.18,  # 1 тенге ≈ 0.18 руб
            'UZS': 0.008, 'uzs': 0.008, 'Uzs': 0.008,
            'BYR': 28,    # Белорусский рубль (старый)
            'BYN': 28,    # Белорусский рубль (новый)
            'KGS': 1.05,  # Киргизский сом
            'RUB': 1, # российский рубль 
            'RUR': 1
        }



def safe_convert(amount, from_currency): # функция конвертации
    
    # Проверка на пустые значения
    if not amount:
        return amount
    
    # Если не надо конвертировать рубли, возвращаем значение 
    if from_currency in ['RUB', 'RUR']:
        return amount
    
    # Пробуем словарь
    rate = EXCHANGE_RATES.get(from_currency) # берем значения из словаря для конвертации 
    if rate:
        result = amount * rate # конвертируем 
        return result # возвращаем результат конвертации 
    
    # Если валюты нет в словаре
    return amount


def fetch_vacancies_profession(profession, page): # функция для динамического получения профессии 
    params = {
        'text': profession, # динамически получаем профессию 
        'per_page': 100, # верни 100 вакансий на одной странице 
        'page': page # страница 
    }

    response = requests.get(url, params=params) #сетевой запрос через метод get для получения данных в json
    response.raise_for_status() #автоматически проверяет статус
    return response.json() # возвращаем результат json 

    

def parser(profession, limit=2000): 
    conn = get_connection() #подключаемся к БД 

    cursor = conn.cursor() #для работы с БД
    current_page = 0 #страница для отсчета (текущая страница)
    total_saved = 0 #количество записей 

    try:
        while total_saved < limit: 


            try:
                data = fetch_vacancies_profession(profession, current_page) # получение данных в формате json в случае успешного подключения (словарь)


                vacancies = data.get('items', []) #получаем список вакансий 


                if not vacancies: #если нет вакансий, то выходим из цикла 
                    break

                if current_page > data.get('pages', 0): #предохранитель 
                    break

                # извлекаем нужные поля 
                # нуж проверять наличие ключей через get()


                #структура запииси
                for item in vacancies: # проходим по каждому значению из вакансий
                    if total_saved < limit:

                        salary_data = item.get('salary') or {} #получаем значение salaries 
                        raw_currency = salary_data.get('currency', 'RUB') #получаем значения валют 
                        currency = raw_currency.strip().upper() if raw_currency else 'RUR'

                        from_val = salary_data.get('from') #безопасное извлечение по ключу - до 
                        to_val = salary_data.get('to') #безопасное извлечение по ключу - после 

                        if from_val and currency not in ['RUB', 'RUR']: # конвертация внутри функции парсера 
                            from_val = safe_convert(from_val, currency) # минимальная планка 
                            if to_val:
                                to_val = safe_convert(to_val, currency) # максимальная планка 
                            currency = "RUR"

                        converted_salaries = [v for v in [from_val, to_val] if v] #собираем только сущесвутющие числа

                        city_name = item.get('area', {}).get('name', '')
                        city_id = get_or_create_city(cursor, city_name, None)

                        employer_name = item.get("employer", {}).get('name', '')  
                        employer_id = get_or_create_employer(cursor, employer_name)
                        vacancy_name = item['name']

                        experience = extract_exp(vacancy_name)

                        cursor.execute('''
                            INSERT OR REPLACE INTO vacancies 
                            (id, name,  city_id, employer_id, salary_mid, currency, profession, published_at, experience)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            item.get('id'),
                            item.get('name'),
                            city_id,
                            employer_id,
                            np.mean(converted_salaries) if converted_salaries else None, 
                            currency,
                            profession,
                            item.get('published_at'),
                            experience
                        ))

                        total_saved += 1 #количество файлов 

                        if total_saved % 100 == 0:
                            conn.commit()
                            print(f"Финальный commit: сохранено {total_saved} записей")
                            
                    else:
                        print(f"Ошибка в цикле сохранения")
                        break

                current_page += 1 # прибавляем на каждой итерации страницу 
                print(f"Обработана страница {current_page}, собрано: {total_saved}") #какая страница сейчас была пройдена и количество сохранений

                time.sleep(0.2) #уважение к API, сделаем ожидание 
            except requests.exceptions.HTTPError as err: # ошибка hhtp запросов 
                print(f"Ошибка Http: {err}")
                break
            except requests.exceptions.RequestException as err: # любая проблема с сетевым запросом 
                print(f"Ошибка: {err}")
                break


    except Exception as e: # против ошибок 
        print(f"Ошибка: {e}") 
        traceback.print_exc()
    finally:
        cursor.execute("SELECT DISTINCT profession FROM vacancies")
        print("Все профессии в БД:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}")
        conn.close()  #закрываем соединение в конце
        print("Соединение с БД закрыто")


if __name__ == "__main__": #запуск проекта 
    parser(profession=None) #функция 




