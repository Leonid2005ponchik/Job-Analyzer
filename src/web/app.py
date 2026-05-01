from flask import Flask, render_template, request, redirect, url_for, jsonify, make_response # библиотека для создания веб приложения
# отображение html страниц, забираем данные от user, перенаправление между страницами, словари в формат json
import joblib # сохранение моделей
import sys # система
import os # пути 
os.environ['PATH'] = r'C:\msys64\mingw64\bin' + os.pathsep + os.environ.get('PATH', '')
os.environ['WEASYPRINT_DLL_DIRECTORIES'] = r'C:\msys64\mingw64\bin'
import threading #для работы с потоками данных 
import subprocess # нужно чтобы из 1 го скрипта загрузить другой 
import pandas as pd # библиотека для обработки 
import requests
from news_fetcher import fetch_news
import gc # сборщик мусора 
import traceback
import random
import pdfkit ##################################################
from weasyprint import HTML 
from datetime import datetime
from urllib.parse import quote # библиотека для кодировки 

# Добавляем путь к src
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.ml.predict_v2 import SalaryPredict
from src.db.database import get_connection
from fresh_vacancies import get_recent_vacancies
from src.web.vacancy_fetcher import fetch_latest_vacancies
from src.analyze import generate_all_plots

app = Flask(__name__) #создаем экземпляр приложения 


predictor = SalaryPredict(
    models_path='src/ml/models/models.pkl',
    encoders_path='src/ml/models/encoders.pkl'
)

#кодировка городов
CITY_CODES = {
    'Москва': 1,
    'санкт-петербург': 2,
    'екатеринбург': 3,
    'новосибирск': 4,
    'казань': 88,
    'нижний новгород': 66,
    'красноярск': 53,
    'челябинск': 56,
    'самара': 78,
    'уфа': 99,
    'ростов-на-дону': 76,
    'омск': 68,
    'краснодар': 53,  
    'воронеж': 70,
    'пермь': 57,
    'волгоград': 34,
    'саратов': 55,
    'тюмень': 77,
    'тольятти': 75,
    'ижевск': 43,
    'барнаул': 38,
    'иркутск': 44,
    'ульяновск': 87,
    'хабаровск': 79,
    'владивосток': 75,
    'ярославль': 89,
    'махачкала': 54,
    'томск': 80,
    'оренбург': 60,
    'кемерово': 49,
    'новокузнецк': 52,
    'рязань': 64,
    'астрахань': 37,
    'пенза': 61,
    'липецк': 50,
    'киров': 45,
    'чебоксары': 73,
    'калининград': 46,
    'тула': 83,
    'курск': 47,
    'ставрополь': 72,
    'севастополь': 113,
    'симферополь': 114,
}



@app.route('/') #определяем маршрут для главной страницы 
def index():
    try: 
        news = fetch_news("https://lenta.ru/rss", limit=5) # парсим лента ру с лимитом 5 новостей 
        recent_vacancies = get_recent_vacancies(limit=5) # получаем 5 вакансий из БД 
        available_prof = list(predictor.models.keys()) # берем для нашей модели 

        try: 
            metrics_data = joblib.load('src/ml/models/metrics.pkl')
            print(f"Метрики загружены: {metrics_data}")
            mae_values = [m['mae_train'] for m in metrics_data.values() if 'mae_train' in m]
            avg_mae = sum(mae_values) / len(mae_values)
        except Exception as e: 
            print(f"Ошибка: {e}")
            metrics_data = {}

        print(f"=== В index() ===")
        print(f"news: {len(news)}")
        print(f"recent_vacancies: {len(recent_vacancies)}")
        print(f"available_professions: {len(available_prof)}")

        return render_template('index.html', news=news, 
                            recent_vacancies=recent_vacancies,
                            available_prof = available_prof,
                            avg_mae = round(avg_mae))
    except Exception as e: 
        print(f"Ошибка: {e}")
        traceback.print_exc()

@app.route('/api/refresh_vacancies')
def refresh_vacancies():
    vacancies = fetch_latest_vacancies(limit=5) 
    return jsonify(vacancies)






def run_parser(profession):
    profession = profession.lower()
    cmd = [
        sys.executable,
        '-c',
        f'import sys; sys.path.append(r"{os.path.dirname(os.path.dirname(__file__))}"); '
        f'from src.parser import parser; '
        f'parser(profession="{profession}", limit=2000); '
        f'from src.ml.train_v2 import train_model_v2, create_features, load_data; '
        f'df = load_data(); '
        f'data, encoders = create_features(df); '
        f'models, metrics = train_model_v2(data); '
        f'import joblib; '
        f'joblib.dump(models, "src/ml/models/models.pkl"); '
        f'joblib.dump(encoders, "src/ml/models/encoders.pkl"); '
        f'from src.analyze import generate_all_plots; '
        f'generate_all_plots(profession="{profession}");'
    ]
    print(f"Комманда: {cmd}")


    # Запускаем фоновый процесс, чтобы сайт не лег 
    process = subprocess.run(cmd, capture_output=True, text=True)

    print(f"{process.stdout}")
    print(f"{process.stderr}")
    if process.returncode == 0: 
        reload_model()
        print(f"Завершение работы для: {profession}")
    else: 
        print(f"Ошибка выполнения: {process.returncode}")





def reload_model():
    global predictor 

    predictor = SalaryPredict(
        models_path='src/ml/models/models.pkl',
        encoders_path='src/ml/models/encoders.pkl'
    )
    print("Модель перезагружена в память")






@app.route('/check_ready/<profession>')
def check_ready(profession):
    try:

        profession = profession.lower().strip()

        ready = profession in predictor.models
        print(f"Профессия: {profession} {'найдена' if ready else 'не найдена'}")
        print(f"Доступные профессии: {list(predictor.models.keys())[:10]}")
        return jsonify({'ready': ready})
    except:
        ready = False
        traceback.print_exc()
        return jsonify({'ready': False})
        

@app.route('/check_profession', methods=['POST'])
def check_profession(): 
    profession = request.form['profession'].lower() # Берем профессию из формы

    thread = threading.Thread(target=run_parser, args=(profession,))
    thread.daemon = True # делаем поток фоновым 
    thread.start()
    return render_template('loading.html', profession=profession)
    



    
def get_city_code(city_name):

    city_name = city_name.strip().lower()
    if city_name in CITY_CODES: # проверяем словарь кодов городов 
        return CITY_CODES[city_name]


    try:
        # если нету, то запрос API 
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
         'Accept': 'application/json',
        }
        url ='https://api.hh.ru/areas'
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        areas = response.json()

        def find_city(area_list, target_name): # рекурсивный поиск 
            for area in area_list:
                if area['name'].lower() == target_name.lower():
                    return area['id']
                if area.get('areas'):
                    result = find_city(area['areas'], target_name)

                    if result:
                        return result
            return None
        city_code = find_city(areas, city_name)

        # запасной вариант fallback 

        if city_code:
            CITY_CODES[city_name] = city_code

            return city_code
        
        else:
            return 1

    except Exception as e:
        print(f"Ошибка: {e}")
        return 1


def fetch_vacancies(profession, city_name, limit=20):
    code_city = get_city_code(city_name) or 1
    vacancies = []
    # Вернись к API, но убери search_field
    url = "https://api.hh.ru/vacancies"
    params = {
        'text': profession,
        'area': code_city,
        'per_page': limit,
        # 'search_field': 'description'  # ← УБЕРИ ЭТО
    }
    
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'application/json',
        'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        print(f"API вернул {len(data.get('items', []))} вакансий")
        
        for item in data.get('items', [])[:limit]:
            vacancies.append({
                'name': item.get('name', 'Без названия'),
                'salary': format_salary(item.get('salary')),
                'employer': item.get('employer', {}).get('name', 'Не указано'),
                'url': item.get('alternate_url')
            })
            
    except Exception as e:
        print(f"Ошибка API: {e}")
        return []
    
    return vacancies


@app.route('/vacancies', methods=['GET', 'POST'])
def vacancies_page():
    vacancies = None
    # получаем из request профессию и город
    if request.method == 'POST':
        profession = request.form.get('profession', '').lower()
        city = request.form.get('city', 'Москва')
        
        print(f"Поиск вакансий: profession={profession}, city={city}")
        
        vacancies = fetch_vacancies(profession, city, limit=20)

        print(f"Найдено вакансий по запросу: {profession} -- {len(vacancies)}")
        
    return render_template('vacancies.html', vacancies=vacancies)





@app.route('/ml', methods=['GET', 'POST'])
def ml_page():
    salary = None
    profession = ''
    city = 'Москва'
    employer = ''
    currency = 'RUR'
    job_title = ''

    if request.method == 'POST':
        job_title = request.form.get('job_title', '')
        city = request.form.get('city', 'Москва')
        employer = request.form.get('employer', '')
        currency = request.form.get('currency', 'RUR')
        profession = job_title.lower().strip()

        print(f"=== ML PAGE PREDICT ===")
        print(f"profession: {profession}")
        print(f"city: {city}")
        print(f"employer: {employer}")
        print(f"currency: {currency}")

        if profession:
            try:
                salary = predictor._predict(profession, city, employer, currency)
                if salary:
                    salary = round(salary)
                    print(f"Предсказанная зарплата: {salary}")
            except Exception as e:
                print(f"Ошибка: {e}")
                traceback.print_exc()
                salary = None
    
    return render_template('ml.html',
                         salary=salary,
                         profession=profession,
                         city=city,
                         employer=employer,
                         currency=currency,
                         job_title=job_title)





# Главная страница аналитики — только здесь
@app.route('/plots/<profession>', methods=['GET', 'POST'])
def show_plots(profession):

    
    # префикс для название профессии
    prefix = f"{profession}_"
    plots_link = "src/web/static/plots"
    need_plots = [
        f'{prefix}top_cities.png',
        f'{prefix}top_employers.png',
        f'{prefix}salary_distribution.png',
        f'{prefix}salary_date.png',
        f'{prefix}salary_by_city.png',
        f'{prefix}salary_by_employer.png'
    ]

    plots_exist = all(os.path.exists(os.path.join(plots_link, plot)) for plot in need_plots)

    if not plots_exist:
        print(f"Графики не сгенерированы")
        generate_all_plots(profession=profession)
      
    
    return render_template('plots.html',
                           profession=profession)




def format_salary(salary_data):
    if not salary_data:
        return "З/П не указана"
    
    salary_from = salary_data.get('from')
    salary_to = salary_data.get('to')
    currency = salary_data.get('currency', '')

    if currency == 'RUR': 
        currency = "руб."
    if currency == "USD": 
        currency = "$"
    if currency == 'EUR':
        currency = '€'

    if salary_from is not None and salary_to is not None:
        try:
            return f"{salary_from:,.0f} - {salary_to:,.0f} {currency}".replace(',', ' ')
        except (TypeError, ValueError):
            return f"{salary_from} - {salary_to} {currency}"
    elif salary_from is not None:
        try:
            return f"от {salary_from:,.0f} {currency}".replace(',', ' ')
        except (TypeError, ValueError):
            return f"{salary_from} {currency}"
    elif salary_to is not None:
        try:
            return f"до {salary_to:,} {currency}".replace(',', ' ')
        except (TypeError, ValueError):
            return f"{salary_to} {currency}"
    else:
        return 'не указана'
    
def get_report_data(profession): 
    # загружаем данные
    conn = get_connection()
    if profession:
        df = pd.read_sql_query('''
            SELECT v.*, c.name as city_name, e.name as employer_name
            FROM vacancies v
            LEFT JOIN cities c ON v.city_id = c.id
            LEFT JOIN employers e ON v.employer_id = e.id
            WHERE v.profession = ?
        ''', conn, params=(profession,))
    else:
        df = pd.read_sql_query('''
            SELECT v.*, c.name as city_name, e.name as employer_name
            FROM vacancies v
            LEFT JOIN cities c ON v.city_id = c.id
            LEFT JOIN employers e ON v.employer_id = e.id
        ''', conn)
    conn.close()

    data = df.copy() #создаем копию БД 

    if len(data) == 0: 
        print(f"Нет данных для: {profession}")
        return None
    if len(data) < 10: 
        print(f"Слишком мало данных для: {profession}")
        return None

    data= data.dropna(subset=['salary_mid'])
    data = data[data['salary_mid'] > 0]

    count = len(data) # количество вакансий 
    salary_mid_for_profession = data['salary_mid'].mean() # Среднее
    mediana_salary = data['salary_mid'].median() # среднее по медиане 
    min_salary = data['salary_mid'].min() # минимум 
    filtered_data = data[data['salary_mid'] <= 1_000_000]
    max_salary = filtered_data['salary_mid'].max() # максимум

    # Загружаем данные MAE
    metrics_data = joblib.load('src/ml/models/metrics.pkl')
    print(f"Метрики загружены: {metrics_data}")
    mae_values = [m['mae_train'] for m in metrics_data.values() if 'mae_train' in m]
    avg_mae = sum(mae_values) / len(mae_values)


    if profession:
            try:
                salary = predictor._predict(profession, 'Москва', 'Other', 'RUB')
                if salary:
                    salary = round(salary)
                    print(f"Предсказанная зарплата: {salary}")
            except Exception as e:
                print(f"Ошибка: {e}")
                traceback.print_exc()
                salary = None

    plots = ['top_cities', 'top_employers', 'salary_distribution', 
         'salary_date', 'salary_by_city', 'salary_by_employer']
    return {
        'profession': profession,
        'count': count,
        'mean_salary': round(salary_mid_for_profession),
        'median_salary': round(mediana_salary),
        'min_salary': round(min_salary),
        'max_salary': round(max_salary),
        'mae': round(avg_mae, 2),
        'predicted_salary': salary,
        'plots': plots
    }

@app.route('/export_pdf/<profession>')
def export_pdf(profession):
    report_data = get_report_data(profession)
    if report_data is None: 
        return "Нет данных для отчета"
    
    render = render_template('pdf_report.html',
                             profession=profession,
                             date=datetime.now().strftime('%Y-%m-%d %H:%M'),
                             data=report_data)
    
    #config = pdfkit.configuration(wkhtmltopdf=r'C:\wkhtmltopdf\bin\wkhtmltopdf.exe')
    html_doc = HTML(string=render)
    pdf_file = html_doc.write_pdf(compress=False, presentational_hints=True) #генерируем файл 
    response = make_response(pdf_file) # создаем ответ сервера 
    response.headers['Content-Type'] = 'application/pdf' # установка типа контента для pdf 
    # инструкция для браузера где и как сохранить 
    response.headers['Content-Disposition'] = f"inline; filename*=UTF-8''{quote(f'report_{profession}.pdf')}"

    return response
if __name__ == "__main__":
    app.run(debug=True) #запускаем локальный сервер разработки
    for rule in app.url_map.iter_rules():
        print(rule)