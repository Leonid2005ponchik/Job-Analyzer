import numpy as np
import re # подключение регулярных выражений 
# кодировка уровней для работников
def extract_level(title):
    title = title.lower()
    if 'junior' in title or 'стажер' in title:
        return 0  # junior
    elif 'middle' in title:
        return 1  # middle
    elif 'senior' in title or 'lead' in title:
        return 2  # senior
    else:
        return 1  # middle по умолчанию
        
# категоризация работодателей 
def get_employer_category(employer, employer_avg_salary):
    if employer in employer_avg_salary:
        avg = employer_avg_salary[employer]
        if avg < 100000:
            return 1
        elif avg < 200000:
            return 2
        else:
            return 3
    else:
        return 2

# категоризация горододов
def get_cities(city, city_encoder):
    if city in city_encoder.classes_:
        return city_encoder.transform([city])[0]
    else:
        return -1
# валюта    
def encoded_currency(currency, currency_encoder):
    if currency == "RUB":
        currency = "RUR"
    if currency in currency_encoder.classes_:
        return currency_encoder.transform([currency])[0]
    else:
        return -1

# профессия 
def encoded_profession(profession, prof_encoded):
    profession = profession.lower().strip()
    
    if profession in prof_encoded.categories_[0]:
        return prof_encoded.transform([[profession]]).flatten()
    else:
        # Если профессия не найдена, возвращаем вектор для "другой" профессии
        # Создаём вектор из нулей и ставим 1 для "средней" профессии
        n_cats = len(prof_encoded.categories_[0])
        default = np.zeros(n_cats)
        # Можно поставить 1 для самой частой профессии
        # или оставить все нули
        return default

def extract_exp(name):

    name = name.lower()
    
    patterns = [
        (r'без опыта|нет опыта|стажер|intern', 0),
        (r'1\s*год|1\s*года|1-3', 1),
        (r'2\s*года|2\s*лет', 2),
        (r'3\s*года|3\s*лет|3-6', 3),
        (r'[4-5]\s*лет|4-5', 4),
        (r'[6-9]\s*лет|более\s*\d+\s*лет|от 6', 5),
    ]
    
    for pattern, value in patterns:
        if re.search(pattern, name):
            return value
    
    return 1  # значение по умолчанию