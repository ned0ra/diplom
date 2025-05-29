import pandas as pd
import requests
from time import sleep
import ast
import hashlib
import spacy
from typing import List, Dict, Optional

# Инициализация NLP модели один раз при загрузке модуля
nlp = spacy.load('ru_core_news_sm')

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
    df['job-name'] = df['job-name'].str.lower().apply(extract_profession)
    df['city'] = df['city'].str.replace('^г', '', regex=True)
    
    return df

def extract_profession(text: str) -> str:
    """Извлекает профессию из текста"""
    doc = nlp(text)
    return ' '.join([token.text for token in doc if token.pos_ == 'NOUN'])

def calculate_hash(row: Dict) -> str:
    """Вычисляет хеш строки для сравнения"""
    return hashlib.md5(str(row).encode()).hexdigest()

def prepare_vacancies(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Полный цикл обработки сырых данных"""
    df = expand_vacancy_data(df_raw)
    df = clean_data(df)
    return df