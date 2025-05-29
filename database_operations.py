import pandas as pd
from typing import List, Dict
from psycopg2.extras import execute_batch
from database import execute_query, get_db_connection
import hashlib

def create_tables():
    """Создает все необходимые таблицы в БД"""
    queries = [
        '''CREATE TABLE IF NOT EXISTS region (
            region_code VARCHAR(20) PRIMARY KEY,
            region_name VARCHAR(100) NOT NULL,
            city VARCHAR(100) NOT NULL
        );''',
        '''CREATE TABLE IF NOT EXISTS company (
            company_code VARCHAR(50) PRIMARY KEY,
            region_code VARCHAR(20) NOT NULL,
            source VARCHAR(50) NOT NULL,
            company_email VARCHAR(100),
            company_hr_agency BOOLEAN NOT NULL,
            company_inn VARCHAR(20) NOT NULL,
            company_kpp VARCHAR(20) NOT NULL,
            company_name VARCHAR(255) NOT NULL,
            company_ogrn VARCHAR(20) NOT NULL,
            company_url VARCHAR(255),
            FOREIGN KEY (region_code) REFERENCES region(region_code)
        );''',
        '''CREATE TABLE IF NOT EXISTS vacancy (
            id VARCHAR(36) PRIMARY KEY,
            company_code VARCHAR(50) NOT NULL,
            salary_min INTEGER NOT NULL,
            salary_max INTEGER NOT NULL,
            job_name VARCHAR(255) NOT NULL,
            vac_url VARCHAR(255) NOT NULL,
            employment VARCHAR(50) NOT NULL,
            schedule VARCHAR(50) NOT NULL,
            category_specialisation VARCHAR(100) NOT NULL,
            requirement_education VARCHAR(100) NOT NULL,
            requirement_experience VARCHAR(100) NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_hash VARCHAR(32),
            FOREIGN KEY (company_code) REFERENCES company(company_code)
        );'''
    ]
    
    for query in queries:
        execute_query(query)

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