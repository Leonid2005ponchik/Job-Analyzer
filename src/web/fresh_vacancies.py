import os, sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


from src.db.database import get_connection


def get_recent_vacancies(limit=None):
    vacancies = []
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT 
            v.name,
            v.id,
            v.salary_mid,
            v.published_at,
            c.name AS city_name,
            e.name AS employer_name
        FROM vacancies v
        JOIN cities c ON v.city_id = c.id 
        JOIN employers e ON v.employer_id = e.id
        ORDER BY v.published_at DESC
        LIMIT ?;     
    ''', (limit, ))

    rows = cursor.fetchall()


    for row in rows: 
        salary = rows[2]
        if salary is not None and isinstance(salary, (int, float)): 
            salary_str = f"{int(salary):,} руб."
        else:
            salary_str = "Не указана"
        vacancies.append({
            'name': row[0],
            'id': row[1],
            'salary_mid':salary_str,
            'published_at': row[3],
            'city': row[4],
            'employer': row[5],
            'url': f"https://hh.ru/vacancy/{row[1]}"
        })

    conn.close()
    return vacancies
if __name__ == "__main__":
    get_recent_vacancies()
