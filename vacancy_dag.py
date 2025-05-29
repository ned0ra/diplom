import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from time import sleep
import ast
import hashlib
# import spacy
from typing import List, Dict, Optional
from psycopg2.extras import execute_batch
import hashlib
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager


DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '11111',
    # 'host': 'localhost',
    'host':'host.docker.internal',
    'port': '5432'
}

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    except psycopg2.Error as e:
        print(f"Ошибка подключения к БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

def execute_query(query, params=None, fetch=False):

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Ошибка выполнения запроса: {e}")
                raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 21),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

# nlp = spacy.load('ru_core_news_sm')

def get_vacancies_batch(offset: int = 0, limit: int = 100) -> List[Dict]:
    """Получает одну партию вакансий с API"""
    url = "https://opendata.trudvsem.ru/api/v1/vacancies"
    params = {"offset": offset, "limit": limit}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("results", {}).get("vacancies", [])
    except Exception as e:
        print(f"Ошибка при запросе: {e}")
        return []

def collect_vacancies(max_vacancies: int = 2000) -> pd.DataFrame:
    """Собирает вакансии с API"""
    all_vacancies = []
    offset = 0
    batch_size = 100
    
    while len(all_vacancies) < max_vacancies:
        print(f"Собрано {len(all_vacancies)} вакансий...")
        vacancies = get_vacancies_batch(offset, batch_size)
        if not vacancies:
            break
        all_vacancies.extend(vacancies)
        offset += batch_size
        sleep(1)
    
    return pd.DataFrame(all_vacancies[:max_vacancies])

def expand_vacancy_data(df: pd.DataFrame) -> pd.DataFrame:
    """Преобразует вложенные структуры вакансий в плоский DataFrame"""
    if isinstance(df['vacancy'].iloc[0], str):
        df['vacancy'] = df['vacancy'].apply(ast.literal_eval)
    
    all_rows = []
    for vacancy_dict in df['vacancy']:
        flat_row = {}
        
        def flatten_dict(d, prefix=''):
            for key, value in d.items():
                new_key = f"{prefix}{key}"
                if isinstance(value, dict):
                    flatten_dict(value, f"{new_key}_")
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            flatten_dict(item, f"{new_key}_{i}_")
                        else:
                            flat_row[f"{new_key}_{i}"] = item
                else:
                    flat_row[new_key] = value
        
        flatten_dict(vacancy_dict)
        all_rows.append(flat_row)
    
    return pd.DataFrame(all_rows)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка и предобработка данных"""
    # Удаление ненужных столбцов
    columns_to_drop = [
        'duty', 'company_site', 'term_text', 'typicalPosition',
        *[f'skills_{i}' for i in range(32)],
        *[f'contact_list_{i}_contact_type' for i in range(1,4)],
        *[f'contact_list_{i}_contact_value' for i in range(1,4)],
        'medicalDocuments', 'shift_0', 'shift_1',
        'requirement_qualification', 'hireDate', 'benefit',
        'scheduleTypeComment', 'addressOffice', 'medicalCertificate'
    ]
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
    
    # Заполнение пропусков
    df['code_profession'].fillna(0, inplace = True)
    df.fillna('Нет данных', inplace=True)
    
    # Извлечение города и адреса
    df[["city", "address"]] = df["addresses_address_0_location"].str.split(",", n=2, expand=True).iloc[:, 1:3]
    df["city"] = df["city"].str.strip()
    df["address"] = df["address"].str.strip()
    
    # Обработка названий вакансий
    # df['job-name'] = df['job-name'].str.lower().apply(extract_profession)
    df['city'] = df['city'].str.replace('^г', '', regex=True)
    
    return df

# def extract_profession(text: str) -> str:
#     """Извлекает профессию из текста"""
#     doc = nlp(text)
#     return ' '.join([token.text for token in doc if token.pos_ == 'NOUN'])
def extract_profession(text: str) -> str:
    """Упрощенная версия без spacy"""
    return text.split()[0]

def calculate_hash(row: Dict) -> str:
    """Вычисляет хеш строки для сравнения"""
    return hashlib.md5(str(row).encode()).hexdigest()

def prepare_vacancies(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Полный цикл обработки сырых данных"""
    df = expand_vacancy_data(df_raw)
    df = clean_data(df)
    return df

def prepare_region_data(df: pd.DataFrame) -> pd.DataFrame:
    """Подготавливает данные регионов"""
    df_region = pd.DataFrame({
        'код региона': df['region_region_code'],
        'название': df['region_name'],
        'город': df['city']
    }).drop_duplicates(subset=['код региона'])
    
    return df_region

def prepare_company_data(df: pd.DataFrame) -> pd.DataFrame:
    """Подготавливает данные компаний"""
    df_company = pd.DataFrame({
        'company_code': df['company_companycode'],
        'region_code': df['region_region_code'],
        'source': df['source'],
        'company_email': df['company_email'],
        'company_hr_agency': df['company_hr-agency'],
        'company_inn': df['company_inn'],
        'company_kpp': df['company_kpp'],
        'company_name': df['company_name'],
        'company_ogrn': df['company_ogrn'],
        'company_url': df['company_url']
    }).drop_duplicates(subset=['company_code'])
    
    return df_company

def prepare_vacancy_data(df: pd.DataFrame) -> pd.DataFrame:
    """Подготавливает данные вакансий"""
    df_vacancy = pd.DataFrame({
        'id': df['id'],
        'company_code': df['company_companycode'],
        'salary_min': df['salary_min'],
        'salary_max': df['salary_max'],
        'job_name': df['job-name'],
        'vac_url': df['vac_url'],
        'employment': df['employment'],
        'schedule': df['schedule'],
        'category_specialisation': df['category_specialisation'],
        'requirement_education': df['requirement_education'],
        'requirement_experience': df['requirement_experience']
    })
    
    # Добавляем хеш данных для отслеживания изменений
    df_vacancy['data_hash'] = df_vacancy.apply(
        lambda x: calculate_data_hash(x.to_dict()), 
        axis=1
    )
    
    return df_vacancy

def calculate_data_hash(data: Dict) -> str:
    """Вычисляет хеш данных вакансии"""
    hash_fields = [
        'company_code', 'salary_min', 'salary_max', 'job_name',
        'employment', 'schedule', 'category_specialisation',
        'requirement_education', 'requirement_experience'
    ]
    hash_str = ''.join(str(data.get(field, '')) for field in hash_fields)
    return hashlib.md5(hash_str.encode()).hexdigest()

def insert_regions_batch(df_region: pd.DataFrame):
    """Пакетная вставка регионов"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            insert_query = """
            INSERT INTO region (region_code, region_name, city)
            VALUES (%s, %s, %s)
            ON CONFLICT (region_code) DO NOTHING;
            """
            data = [
                (row['код региона'], row['название'], row['город'])
                for _, row in df_region.iterrows()
            ]
            execute_batch(cursor, insert_query, data)
            conn.commit()
    print(f"Вставлено {len(df_region)} регионов")

def insert_companies_batch(df_company: pd.DataFrame):
    """Пакетная вставка компаний"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            insert_query = """
            INSERT INTO company (
                company_code, region_code, source, company_email,
                company_hr_agency, company_inn, company_kpp,
                company_name, company_ogrn, company_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_code) DO NOTHING;
            """
            data = [
                (
                    row['company_code'], row['region_code'], row['source'],
                    row['company_email'], row['company_hr_agency'],
                    row['company_inn'], row['company_kpp'], row['company_name'],
                    row['company_ogrn'], row['company_url']
                )
                for _, row in df_company.iterrows()
            ]
            execute_batch(cursor, insert_query, data)
            conn.commit()
    print(f"Вставлено {len(df_company)} компаний")

def insert_vacancies_batch(df_vacancy: pd.DataFrame):
    """Пакетная вставка вакансий"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Вставка новых вакансий
            insert_query = """
            INSERT INTO vacancy (
                id, company_code, salary_min, salary_max,
                job_name, vac_url, employment, schedule,
                category_specialisation, requirement_education, 
                requirement_experience, data_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """
            
            data = [
                (
                    row['id'], row['company_code'], row['salary_min'],
                    row['salary_max'], row['job_name'], row['vac_url'],
                    row['employment'], row['schedule'], 
                    row['category_specialisation'],
                    row['requirement_education'], 
                    row['requirement_experience'],
                    row['data_hash']
                )
                for _, row in df_vacancy.iterrows()
            ]
            execute_batch(cursor, insert_query, data)
            conn.commit()
    print(f"Вставлено {len(df_vacancy)} вакансий")

def update_vacancies_batch(df_vacancy: pd.DataFrame):
    """Обновление существующих вакансий"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            update_query = """
            UPDATE vacancy SET
                company_code = %s,
                salary_min = %s,
                salary_max = %s,
                job_name = %s,
                vac_url = %s,
                employment = %s,
                schedule = %s,
                category_specialisation = %s,
                requirement_education = %s,
                requirement_experience = %s,
                data_hash = %s,
                last_updated = CURRENT_TIMESTAMP
            WHERE id = %s AND data_hash != %s;
            """
            
            data = [
                (
                    row['company_code'], row['salary_min'], row['salary_max'],
                    row['job_name'], row['vac_url'], row['employment'],
                    row['schedule'], row['category_specialisation'],
                    row['requirement_education'], row['requirement_experience'],
                    row['data_hash'], row['id'], row['data_hash']
                )
                for _, row in df_vacancy.iterrows()
            ]
            execute_batch(cursor, update_query, data)
            conn.commit()
    print(f"Обновлено {cursor.rowcount} вакансий")

def fetch_and_prepare_data():
    """Задача для сбора и подготовки данных"""
    raw_df = collect_vacancies(max_vacancies=500)
    processed_df = prepare_vacancies(raw_df)
    
    return {
        'regions': prepare_region_data(processed_df).to_dict('records'),
        'companies': prepare_company_data(processed_df).to_dict('records'),
        'vacancies': prepare_vacancy_data(processed_df).to_dict('records')
    }

def update_database(**context):
    """Обновление данных в БД"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    
    # Преобразуем обратно в DataFrame
    df_region = pd.DataFrame(data['regions'])
    df_company = pd.DataFrame(data['companies'])
    df_vacancy = pd.DataFrame(data['vacancies'])
    
    # Вставляем/обновляем данные
    insert_regions_batch(df_region)
    insert_companies_batch(df_company)
    insert_vacancies_batch(df_vacancy)
    update_vacancies_batch(df_vacancy)

with DAG(
    'vacancy_pipeline_dag',
    default_args=default_args,
    schedule_interval='*/12 * * * *',
    catchup=False,
    tags=['vacancies']
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_and_prepare_data
    )
    
    update_task = PythonOperator(
        task_id='update_database',
        python_callable=update_database,
        provide_context=True
    )
    
    fetch_task >> update_task