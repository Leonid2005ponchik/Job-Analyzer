import pandas as pd # Для анализа и манипуляции данными 
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, TargetEncoder #превращаем текст в числа для машинного обучения 
import sys # взаимодействие с интерпретатором и системой 
import os # работа с файловой системой 
from sklearn.ensemble import  GradientBoostingRegressor# Импорт лес решений для сложных зависимостей
from sklearn.model_selection import train_test_split # функция для обучения и проверки 
import numpy as np #библиотека Data Science 
import traceback # библиотека детектирования ошибок 
import matplotlib #библиотека создание графиков 
matplotlib.use('Agg') # генерация изображений в памяти
from sklearn.model_selection import cross_val_score, KFold # импорт стратегии разделения и проверки 
from sklearn.metrics import mean_absolute_error
from datetime import datetime # время, для метаданных 
import joblib


project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root) 

from src.db.database import get_connection



def load_data(profession=None):
    try:

        os.makedirs('src/ml/models', exist_ok=True) # создаем папку, если есть пропускаем


        conn = get_connection() #подключаемся к базе данных 
        cursor = conn.cursor() #добавляем управление базой данных 
        cursor.execute("SELECT COUNT(*) FROM vacancies") #запрос на подсчет 
        count_vacancies_in_data_base = cursor.fetchone()[0]
        print(f"== Логирование load_data ==")
        print(f"Количество вакансий в функции load_data: {count_vacancies_in_data_base}")

        try:
            if profession: # если есть профессия 
                df = pd.read_sql_query('''
                    SELECT v.*, c.name as city_name, e.name as employer_name
                    FROM vacancies v
                    LEFT JOIN cities c ON v.city_id = c.id
                    LEFT JOIN employers e ON v.employer_id = e.id
                    WHERE v.profession = ?
                ''', conn, params=(profession,))

                #берем все колонки из таблицы, переименовываем, добавляем данные о городах и компаниях 
            else:
                df = pd.read_sql_query('''
                SELECT v.*, c.name as city_name, e.name as employer_name
                FROM vacancies v
                LEFT JOIN cities c ON v.city_id = c.id
                LEFT JOIN employers e ON v.employer_id = e.id
            ''', conn)
        except Exception as e:
            print(f"Ошибка чтения данных БД (load_data): {e}")
        finally:
            conn.close() # закрываем БД

        profession_value_counts = df['profession'].value_counts()
        print(f"Количество уникальных вакансй в БД: {profession_value_counts}")

        # также выведем их всех 

        return df 

    except Exception as e:
        print(f"Ошибка в load_data: {e}")
        traceback.print_exc()
        return pd.DataFrame()
    


def create_features(df):
    try:
        data = df.copy() #создаем копию БД 

        data['profession'] = data['profession'].str.lower().str.strip()
 

        def filter_for_salary(data): # создаем функциб обработки зарплаты 

            if 'salary_mid' not in data.columns: # если нету None
                data['salary_mid'] = np.nan

            return data 

                

        data = filter_for_salary(data)
        data = data.dropna(subset=['salary_mid']) # убираем пустые значения 
        data = data.reset_index(drop=True)

        print(f"== Логирование == ")
        print(f"До удаления дубликатов: {len(data)}")
        print(f"Не None средняя зарплата {data['salary_mid'].notna().sum()}")
        print(f"Пример: {data['salary_mid'].head(10)}")
        data = data.drop_duplicates(subset=['city_name', 'employer_name', 'salary_mid', 'profession'])
        data = data.reset_index(drop=True)
        print(f"После удаления дубликатов (create_features): {data}")

        encoders = {}

        print(f"Данные после фильтрации (create_features): {data}")

        if len(data) == 0:
            print(f"Нет данных! (create_features)")
            return None
        elif len(data) < 200:
            print(f"Мало данных! (create_features)")
        else:
            print(f"Достаточное количество данных! (create_features)")




        le_city = LabelEncoder() #создаем объект 
        data['city_encoded'] = le_city.fit_transform(data['city_name']) # каждому городу присваиваем номер и кодируем 
        encoders['city'] = le_city # добаляем в энкодер 


        te = TargetEncoder()
        data['profession_target'] = te.fit_transform(data[['profession']], data['salary_mid'])
        encoders['profession'] = te



        employer_counts = data['employer_name'].value_counts()
        rare_employer = employer_counts[employer_counts < 5].index
        data['employer_encoded_fix'] = data['employer_name'].apply(
            lambda x: 'other' if x in rare_employer else x
        )
        le_emp_fixed = LabelEncoder() # объект для превращения в числа 
        data['employer_encoded'] = le_emp_fixed.fit_transform(data['employer_encoded_fix']) # кодировка
        encoders['employer'] = le_emp_fixed

        le_currency = LabelEncoder()
        data['currency_encoded'] = le_currency.fit_transform(data['currency'])
        encoders['currency'] = le_currency


        X = data[['city_encoded', 'employer_encoded', 'currency_encoded', 'profession_target']]
        y = data['salary_mid'].values 


        feature_columns = X.columns.tolist() 
        X_final = X[feature_columns] # три колонки с числами

        print("Колонки в X_final:", X_final.columns.tolist())



        return data, encoders 
    except Exception as e:
        print(f"Ошибка (create_features): {e}")
        traceback.print_exc()


def train_model_v2(data): 
    models = {}
    metrics = {}

    for profession in data['profession'].unique():
        prof_data = data[data['profession'] == profession]


        if len(prof_data) < 50:
            print(f"Пропускаем {profession}: {len(prof_data)} вакансий")
            continue

        X = prof_data[['city_encoded', 'employer_encoded', 'currency_encoded']]
        y = np.log1p(prof_data['salary_mid'].values)

        model = GradientBoostingRegressor(
            n_estimators=250, # количество деревьев 
            max_depth=5, # глубина леса 
            learning_rate=0.1,
            min_samples_leaf=10,
            random_state=42
        )#создаем модель лес решений


        model.fit(X, y)

        y_pred_test_v2 = model.predict(X)
        mae_test_v2 = mean_absolute_error(np.expm1(y), np.expm1(y_pred_test_v2))

        models[profession] = model
        metrics[profession] = {
            "value_profession": len(prof_data),
            'mae_train': mae_test_v2
        }


    return models, metrics

# def train_model(X_final, y_log):
#     try: 
#         # разбиваем данные 20 данныз на проверку, 80 на обучение 
#         X_train, X_test, y_train, y_test = train_test_split(
#             X_final, y_log, test_size = 0.2, random_state = 42
#         )

#         model_rf = GradientBoostingRegressor(
#                 n_estimators=250, # количество деревьев 
#                 max_depth=5, # глубина леса 
#                 learning_rate=0.1,
#                 min_samples_leaf=10,
#                 random_state=42
#             )#создаем модель лес решений
        
#         # добавляем кросс - валидацию (делим на пять равных частей, 4 части на обучение - 1 на проверку)

#         kfold = KFold(n_splits=5, shuffle=True, random_state=42)
#         cros_val = cross_val_score(model_rf, X_train, y_train, cv=kfold, scoring='r2')

#         model_rf.fit(X_train, y_train) # обучение финальной модели

#         y_pred_train = model_rf.predict(X_train) # делаем предсказание на тех же данных для диагностики переобучения 
#         mae_train = mean_absolute_error(np.expm1(y_train), np.expm1(y_pred_train)) # ошибка на обучении - возращаем значение


#         y_pred_test = model_rf.predict(X_test)
#         mae_test = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred_test))


#         baseline_pred_log = np.full_like(y_test, np.median(y_train)) # прогноз пустышка 
#         mae_baseline = mean_absolute_error(np.expm1(y_test), np.expm1(baseline_pred_log)) # также смотрим ошибку 


#         importance = model_rf.feature_importances_
#         feature_names = X_final.columns.tolist()

#         print(f"Важные признаки модели: ")

#         sorted_list = np.argsort(importance)[::-1]
#         for sorted_values in sorted_list: 
#             if importance[sorted_values] > 0.1:
#                 print(f"{feature_names[sorted_values]} : {importance[sorted_values]:.2f}")
            

#         print(f"\nМетрики:")

#         print(f"Средний R2 на кросс валидации: {cros_val.mean():.2f}")
#         print(f"Разброс точности на кросс валидации: {cros_val.std():.2f}")
#         print(f"MAE на обучении: {mae_train:.0f} руб.")
#         print(f"MAE на тесте: {mae_test:.0f} руб.")
#         print(f"MAE baseline (среднее): {mae_baseline:.0f} руб.")
#         print(f"Улучшение относительно baseline: {(1 - mae_test/mae_baseline)*100:.1f}%")


#         return model_rf, mae_train, mae_test, mae_baseline

#     except Exception as e:
#         print(f" Ошибка в train model:{e}")
#         traceback.print_exc() ###

def save_artifacts(models, encoders, metrics, path='src/ml/models/'):
    try: 

        os.makedirs(path, exist_ok=True)


        joblib.dump(models, 'src/ml/models/models.pkl') #сохраняем данные модели 
        joblib.dump(encoders, 'src/ml/models/encoders.pkl') #сохраняем данные модели 
        joblib.dump(metrics, 'src/ml/models/metrics.pkl') #сохраняем данные модели 
        
        print(f"Сохранено: {len(models)} моделей")
        print(f"Сохранено: {len(encoders.keys())}")
        return True
    except Exception as e: 
        print(f"Ошибка save_artificats: {e}")
        traceback.print_exc()


def quality_test(metrics):
    print("=" * 60)
    print(f"Отчет по качеству моделей")

    for prof, m in sorted(metrics.items(), key=lambda x: x[1]['mae_train']):
        mae = m['mae_train']
        count = m['value_profession']

        if mae < 40000: 
            answer = "Отлично"
        elif mae < 60000: 
            answer = "Хорошо"
        else: 
            answer = "Плохо"

        print(f"{prof:<20} {count:<10} {mae:<15.0f} {answer:<10}")

    avg_mae = np.mean([m['mae_train'] for m in metrics.values()])
    print(f"Средний Mae: {avg_mae:0f}")

if __name__ == "__main__":

    #Загрузка данных 
    df = load_data(profession=None)
    
    # создание признаков 
    result = create_features(df)

    if result is None: 
        print(f"Ошибка: нет данных")

        sys.exit(1)

    data, encoders = result 
    
    #обучениие модели 
    models, metrics = train_model_v2(data)

    #Сохранение модели 
    save_artifacts(models, encoders, metrics)
    
    #вывод результатов модели 
    quality_test(metrics)

    print(f"Модель сохранена!")