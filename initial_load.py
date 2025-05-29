import pandas as pd
from vacancy_processor import collect_vacancies, prepare_vacancies
from database_operations import (
    create_tables,
    prepare_region_data,
    prepare_company_data,
    prepare_vacancy_data,
    insert_regions_batch,
    insert_companies_batch,
    insert_vacancies_batch
)

def main():
    # 1. Сбор данных
    print("Сбор вакансий...")
    raw_df = collect_vacancies(max_vacancies=2000)
    processed_df = prepare_vacancies(raw_df)
    
    # 2. Подготовка данных
    print("Подготовка данных...")
    df_region = prepare_region_data(processed_df)
    df_company = prepare_company_data(processed_df)
    df_vacancy = prepare_vacancy_data(processed_df)
    
    # 3. Загрузка в БД
    print("Загрузка в БД...")
    create_tables()
    insert_regions_batch(df_region)
    insert_companies_batch(df_company)
    insert_vacancies_batch(df_vacancy)
    
    print("Первоначальная загрузка завершена!")

if __name__ == "__main__":
    main()