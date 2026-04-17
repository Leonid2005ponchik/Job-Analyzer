# зачем нужен предикт
# 1. модель обучилась
# 2. Теперь нужно использовать к данным, которые она не знает 
# 3. Предикт преобразует в тот же формат, который был при обучении 

# загружаем данные 
import joblib
import numpy as np
import pandas as pd


class SalaryPredict: 
    def __init__(self, models_path = 'src/ml/models/models.pkl', encoders_path='src/ml/models/encoders.pkl'):
        self.models = joblib.load(models_path)
        self.encoders = joblib.load(encoders_path)


    def _normalize_profession(self, profession): 
        if isinstance(profession, list):
            profession = profession[0] if profession else ''

        if not isinstance(profession, str):
            profession = str(profession)

        return profession.lower().strip()

    def _prepare_features(self, city, employer, currency="RUR"): 
        try: 
            city = city.strip().title()
            currency = currency.upper().strip()
            if currency == "RUB":
                currency = "RUR"

            city_encoded = self.encoders['city'].transform([city])[0]

            if employer not in self.encoders['employer'].classes_:
                employer = 'other'

            employer_encoded = self.encoders['employer'].transform([employer])[0]

            currency_encoded = self.encoders['currency'].transform([currency])[0]


            return [city_encoded, employer_encoded, currency_encoded]

        except Exception as e: 
            print(f"Ошибка: {e}")

    def _predict(self, profession, city, employer, currency="RUR"):

        if isinstance(profession, list):
            profession = profession[0] if profession else ''
    
        if not isinstance(profession, str):
            profession = str(profession)
        prof = self._normalize_profession(profession)

        if profession not in self.models: 
            return self._fallback(profession)
        

        features = self._prepare_features(city, employer, currency)
        model = self.models[prof]
        pred_log = model.predict([features])[0]
        salary = np.expm1(pred_log)

        if any(pd.isna(features)): 
            print(f"Ошибка кодировки признаков")
            return self._fallback(profession)

        return int(salary)
    
    def _fallback(self, profession): 

        print(f"Профессия не найдена: {profession}")

        return 50000


if __name__ == "__main__":
    predictor = SalaryPredict()
