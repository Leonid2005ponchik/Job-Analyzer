import pandas as pd # библиотека для обработки 
import os, sys
import matplotlib
import matplotlib.pyplot as plt # визуализация данных 
matplotlib.use('Agg') 
import numpy as np
import seaborn as sns
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from src.db.database import get_connection #импортируем базу данных 
# берем сырые данные из файла, и сохраняем обработанные в файл 
from src.ml.utils import extract_exp


try: 
    OUTPUT_DIR ='src/web/static/plots'
    # создаем папку, если есть пропускаем 
    os.makedirs(OUTPUT_DIR, exist_ok=True)
except Exception as e: 
    print(f"Ошибка: {e}")

def load_data(profession=None):
        # Загружаем данные

        print(f"Загрузка данных для профессии: {profession}")
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
            return None, None
        if len(data) < 10: 
            print(f"Слишком мало данных для: {profession}")
            return None, None

        data= data.dropna(subset=['salary_mid'])
        data = data[data['salary_mid'] > 0]
        prefix = f"{profession}_" if profession else ""

        print(f"Загружено количество: {len(data)} вакансий")

        salary_mid_for_profession = data['salary_mid'].mean()
        print(f"Средняя зарплата по профессии {profession}: {salary_mid_for_profession:.0f}")
        mediana_salary = data['salary_mid'].median()
        print(f"Средняя зарплата (медиана): {mediana_salary:.0f}")
        uniq_city = data['city_name'].nunique()
        print(f"Топ городов по профессии {profession}: {uniq_city}")
        uniq_employer = data['employer_name'].nunique()
        print(f"Уникальных работодателей по профессии {profession}: {uniq_employer}")

        return data, prefix 
    


def plot_top_cities(data, profession, prefix): 
    """Строит топ-N городов по количеству вакансий."""
    if data is None or len(data) == 0:
        print(f"Нет данных для построения графика городов: {profession}")
        return
    
    print(f" == Логирование для топ городов == ")
    city_counts = data['city_name'].value_counts().head(10)
    for city, count in city_counts.head(10).items():
        print(f"{city}: {count} вакансий")

    # Топ городов
    plt.figure(figsize=(10, 6))
    data['city_name'].value_counts().head(10).plot(kind='bar') # строим столбчатую диаграмму по уникальным городам
    plt.title(f"Топ 10 городов для {profession}")
    plt.xlabel("Топ городов")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f'{prefix}top_cities.png'))
    plt.close()

    print(f"== График по топ городам сохранен ==")




def plot_top_employer(data, profession, prefix): 
    """Строит топ-N работодателей по количеству вакансий."""
    if data is None or len(data) == 0:
        print(f"Нет данных для построения графика городов: {profession}")
        return
    employer_counts = data['employer_name'].value_counts().head(10)
    for emp, count in employer_counts.head(5).items():
        print(f"       - {emp}: {count} вакансий")
    # Топ работодателей
    plt.figure(figsize=(10, 6))
    data['employer_name'].value_counts().head(10).plot(kind='bar') # cтроим столбчатую диаграмму для уникальны[] работодаталей
    
    plt.title(f"Топ 10 работодателей для {profession}")
    plt.xlabel("Работодатели")
    plt.xticks(
        rotation=45,      # Угол 45 обычно читается лучше всего
        ha='right',       # Привязываем КОНЕЦ слова к делению 
        rotation_mode='anchor' # Гарантирует, что поворот идет точно от точки привязки
    )
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f'{prefix}top_employers.png')) # сохраняем в файл
    plt.close()

    print(f"== График по топ работодателям сохранен ==")





def plot_salary_distribution(data, profession, prefix):
    """Строит гистограмму распределения зарплат."""
    if data is None or len(data) == 0:
        print(f"Нет данных для построения графика городов: {profession}")
        return


    # Распределение зарплат (без аномалий)
    salaries = data['salary_mid'].dropna()
    normal_salaries = salaries[salaries < 1_000_000] # распределение зарплат до 1 млн 

    if len(normal_salaries) == 0:
        print(f"Нет зарплат до 1 млн. рублей")

    mean_salary = normal_salaries.mean()
    median_salary = normal_salaries.median()
    min_salary = normal_salaries.min()
    max_salary = normal_salaries.max()

    print(f"Средняя: {mean_salary:.0f} руб.")
    print(f"Медиана: {median_salary:.0f} руб.")
    print(f"Мин: {min_salary:.0f} руб.")
    print(f"Макс: {max_salary:.0f} руб.")
    
    if len(normal_salaries) > 0: # если нет аномальных зарплат больше 0 
        plt.figure(figsize=(10, 6))
        plt.hist(normal_salaries, bins=20, edgecolor='black', alpha=0.7, color='skyblue')
        plt.axvline(normal_salaries.mean(), color='red', linestyle='dashed', 
                    label=f'Средняя: {normal_salaries.mean():.0f}') # вертикальная средняя арифметическая 
        plt.axvline(normal_salaries.median(), color='green', linestyle='dashed', 
                    label=f'Медиана: {normal_salaries.median():.0f}') # середина рынка 
        plt.title(f"Распределение зарплат для {profession}")
        plt.xlabel("Зарплата (руб)")
        plt.ylabel("Количество вакансий")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f'{prefix}salary_distribution.png'))
        plt.close()
        
        print(f"== График распределения зарплат сохранен ==")





def plot_salary_trend(data, profession, prefix): 
    if data is None or len(data) == 0:
        print(f"Нет данных для построения графика городов: {profession}")
        return
    
    if 'published_at' not in data.columns:
        print(f"Колонка published_at отсутствует в данных")
        return
    
    print(f"Уникальных месяцев:{data['published_at'].nunique()}")
    
    data['published_at'] = pd.to_datetime(data['published_at'])
    data['month'] = data['published_at'].dt.month
    data['weekday'] = data['published_at'].dt.dayofweek

    pivot = data.pivot_table(
        values='salary_mid', 
        index='weekday', 
        columns='month', 
        aggfunc='median'
    )
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot, annot=True, fmt='.0f', cmap='coolwarm')
    print(f"Префикс профессиии: {prefix}")
    plt.title(f"Распределение зарплат по времени для: {profession}")
    plt.xlabel("Месяц")
    plt.ylabel('День недели')

    plt.xticks(
        rotation=45,      # Угол 45 обычно читается лучше всего
        ha='right',       # Привязываем КОНЕЦ слова к делению 
        rotation_mode='anchor' # Гарантирует, что поворот идет точно от точки привязки
    )

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f'{prefix}salary_date.png')) # сохраняем в файл
    plt.close()


    print(f"== График распределения зарплат по времени сохранен==")






def plot_salary_by_city(data, profession, prefix): 
    if data is None or len(data) == 0:
        print(f"Нет данных для построения графика городов: {profession}")
        return

    # city_stats = data.groupby('city_name')['salary_mid'].agg(['mean', 'median', 'count', 'std'])
    # city_stats = city_stats.sort_values(by='mean', ascending=False)
    # city_stats = city_stats[city_stats['count'] > 5]
    # city_stats = city_stats[city_stats['mean'] < 1000000]
    # city_stats = city_stats.sort_values(by='count', ascending=False).head(10)

    top_cities = data['city_name'].value_counts().head(10).index
    data_top = data[data['city_name'].isin(top_cities)]
    plt.figure(figsize=(10, 6))
    sns.boxplot(
        data=data_top, 
        x='salary_mid', 
        y='city_name', 
        hue='city_name',  # Указываем, что красим по городам
        palette='viridis', 
        legend=False      # Отключаем легенду, так как названия уже есть на оси Y
    )
    plt.title(f"Распределение зарплат по городам: {profession}")
    plt.xlabel("Зарплата")
    plt.ylabel('Города')

    plt.xticks(
        rotation=45,      # Угол 45 обычно читается лучше всего
        ha='right',       # Привязываем КОНЕЦ слова к делению 
        rotation_mode='anchor' # Гарантирует, что поворот идет точно от точки привязки
    )

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f'{prefix}salary_by_city.png')) # сохраняем в файл
    plt.close()


    print(f"== График распределения зарплат по городам сохранен ==")





def plot_salary_by_employer(data, profession, prefix):
    if data is None or len(data) == 0:
        print(f"Нет данных для построения графика городов: {profession}")
        return
    
    top_employer = data['employer_name'].value_counts().head(10).index
    data_top = data[data['employer_name'].isin(top_employer)]

    plt.figure(figsize=(10, 6))
    sns.boxplot(
        data=data_top, 
        x='salary_mid', 
        y='employer_name', 
        hue='employer_name',  # Указываем, что красим по городам
        palette='viridis', 
        legend=False      # Отключаем легенду, так как названия уже есть на оси Y
    )

    plt.title(f"Распределение зарплат по работодателям: {profession}")
    plt.xlabel("Зарплата")
    plt.ylabel('Работодатель')

    plt.xticks(
        rotation=45,      # Угол 45 обычно читается лучше всего
        ha='right',       # Привязываем КОНЕЦ слова к делению 
        rotation_mode='anchor' # Гарантирует, что поворот идет точно от точки привязки
    )

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f'{prefix}salary_by_employer.png')) # сохраняем в файл
    plt.close()


    print(f"== График распределения зарплат по работодателям сохранен ==")



def check_bd(data): 
    conn = get_connection()
    try: 
        data = pd.read_sql_query("SELECT experience, COUNT(*) FROM vacancies GROUP BY experience", conn)
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        conn.close()

    print(data)

def generate_all_plots(profession): 

    data, prefix = load_data(profession)

    if data is None: 
        return False 
    
    plot_top_cities(data, profession, prefix)
    plot_top_employer(data, profession, prefix)
    plot_salary_distribution(data, profession, prefix)
    plot_salary_trend(data, profession, prefix)
    plot_salary_by_city(data, profession, prefix)
    plot_salary_by_employer(data, profession, prefix)

    return True


if __name__ == "__main__":
    generate_all_plots()