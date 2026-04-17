import requests #библиотека для подключения по http запросу 
import time # для обработки времени 



url = 'https://api.hh.ru/vacancies' # url 


def format_salary(salary_data):
    if not salary_data:
        return "З/П не указана"
    salary_from = salary_data.get('from')
    salary_to = salary_data.get('to')
    currency = salary_data.get('currency')

    if salary_data and salary_to:
        return f"{salary_from:,} - {salary_to:,} {currency}".replace(',', ' ')
    elif salary_from:
        return f"от {salary_from:,} {currency}".replace(',', ' ')
    elif salary_to:
        return f"до {salary_to:,} {currency}".replace(',', ' ')
    else:
        return 'не указана'

def fetch_latest_vacancies(limit=5):
    url = 'https://api.hh.ru/vacancies'
    params = {
        'per_page': limit,
        'order_by': 'publication_time'
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    vacancies = []
    for item in data.get('items', []):
        vacancy = {
            'name': item.get('name'),
            'salary': format_salary(item.get('salary')),
            'city': item.get('area', {}).get('name', ''),
            'employer': item.get('employer', {}).get('name', ''),
            'url': item.get('alternate_url')
        }
        vacancies.append(vacancy)
    
    return vacancies