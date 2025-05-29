Для запуска проекта требуется:
1. В корне диска С: создать папку airflow_dags, поместить туда файлы docker-compose.yml(это файл, который описывает структуру и настройку многоконтейнерного приложения) и Dockerfile(конфигурационный файл)
2. Не выходя из airflow_dags создать следующие папки -./dags ./logs ./plugins ./config
3. В папку dags помещаем файл vacancy_dag.py
    vacancy_dag.py - автоматизированный сбор новых вакансий, обработка и обновление данных в БД
4. В отдельную папку помещаем файлы vacancy_processor.py, database.py, database_operations.py, initial_load.py, dashboard.py
   vacancy_processor.py - модуль для сбора и обработки данных с сайта https://trudvsem.ru
       - get_vacancies_batch(): Получает порцию вакансий с API
       - collect_all_vacancies(): Собирает все доступные вакансии, объединяя результаты
       - expand_vacancy_data(): "Разворачивает" вложенные JSON-структуры в плоскую таблицу(pandas dataframe)
       - clean_data(): Очищает и преобразует данные (удаление лишних столбцов, обработка текста)
       - extract_profession(): NLP-обработка названий вакансий с помощью Spacy
   database.py - подключение к базе данных и базовые запросы(для корректной работы можно изменить DB_CONFIG на свои значения)
       - get_db_connection(): Устанавливает соединение с PostgreSQL
       - execute_query(): Универсальная функция для выполнения SQL-запросов
   database_operations.py - операции с вакансиями в БД
       - create_tables(): Создает все таблицы 
       - prepare_*_data(): Преобразует сырые данные в формат для БД
       - insert_*_batch(): Пакетная вставка данных
       - update_vacancies_batch(): Обновление существующих записей
       - calculate_data_hash(): Генерация хеша для отслеживания изменений
   initial_load.py - файл для запуска всего проекта (собирает данные с API, обрабатывает их через vacancy_processor, подготавливает к вставке через database_operations, загружает в PostgreSQL)
   dashboard.py - загрузка данных из БД, построение графиков, фильтрация данных
   
