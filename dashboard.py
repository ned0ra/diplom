import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns

# Настройки подключения к БД
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '11111',
    'host': '127.0.0.1',
    'port': '5432'
}

@st.cache_data(ttl=3600)
def load_data():
    """Загрузка данных из БД"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    queries = {
        'vacancies': """
            SELECT v.*, c.company_name, c.region_code, r.region_name, r.city 
            FROM vacancy v
            LEFT JOIN company c ON v.company_code = c.company_code
            LEFT JOIN region r ON c.region_code = r.region_code
        """,
    }
    
    df = pd.read_sql(queries['vacancies'], conn)
    conn.close()

    df['city'] = df['city'].str.strip()
    
    df['salary_max'] = df.apply(
        lambda x: x['salary_min'] if x['salary_max'] == 0 else x['salary_max'],
        axis=1
    )
    # Преобразуем данные
    df['salary_avg'] = (df['salary_min'] + df['salary_max']) / 2
    df['date'] = pd.to_datetime(df['last_updated'])
    # df['experience'] = df['requirement_experience'].fillna('Не указан')

    df['region_name'] = pd.Categorical(
        df['region_name'], 
        categories=sorted(df['region_name'].unique()),
        ordered=True
    )
    df['requirement_experience'] = df['requirement_experience'].replace(regex=r'^(?!([0-3]|$)).*', value=0).astype(int)

    def map_experience(value):
        value = int(float(value))  # Двойное преобразование для надежности
        if value == 0: return 'Без опыта'
        elif value == 1: return '1-3 года'
        elif value == 2: return '3-6 лет'
        elif value == 3: return '6+ лет'

    df['experience'] = df['requirement_experience'].apply(map_experience)
    
    return df

def show_vacancies_table(df):
    st.header("Таблица вакансий")
    
    # Фильтры в сайдбаре
    st.sidebar.subheader("Фильтры таблицы")
    
    selected_jobs = st.sidebar.multiselect(
        "Направления",
        df['category_specialisation'].unique()
    )
    
    selected_regions = st.sidebar.multiselect(
        "Регионы",
        df['region_name'].unique()
    )
    
    salary_range = st.sidebar.slider(
        "Диапазон зарплат (средних)",
        int(df['salary_avg'].min()),
        int(df['salary_avg'].max()),
        (int(df['salary_avg'].min()), int(df['salary_avg'].max()))
    )
    
    employment_types = st.sidebar.multiselect(
        "Тип занятости",
        df['employment'].unique(),
        default=df['employment'].unique()
    )
    
    # Применяем фильтры
    filtered_df = df[
        (df['category_specialisation'].isin(selected_jobs) if selected_jobs else True) &
        (df['region_name'].isin(selected_regions) if selected_regions else True) &
        (df['salary_avg'].between(salary_range[0], salary_range[1])) &
        (df['employment'].isin(employment_types))
    ]
    
    # Показываем таблицу с возможностью сортировки
    st.dataframe(
        filtered_df[[
            'job_name', 'region_name',
            'salary_min', 'salary_max', 'salary_avg',
            'employment', 'category_specialisation', 'city', 'date'
        ]].sort_values('date', ascending=False),
        height=600,
        use_container_width=True
    )
    
    # Кнопка экспорта
    st.download_button(
        "Экспорт в CSV",
        filtered_df.to_csv(index=False),
        "vacancies.csv",
        "text/csv"
    )

def show_visualizations(df):
    st.header("📊 Визуализации и аналитика")
    
    # Выбор типа визуализации
    viz_type = st.sidebar.selectbox(
        "Выберите тип анализа",
        ["География вакансий", "Анализ зарплат", "Топы по категориям"]
    )
    
    if viz_type == "География вакансий":
        st.subheader("Распределение вакансий по регионам")
        
        # Карта России с вакансиями
        region_stats = df.groupby('region_name').agg({
            'id': 'count',
            'salary_avg': 'mean'
        }).reset_index()
        
        fig1 = px.choropleth(
            region_stats,
            geojson="https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/russia.geojson",
            locations='region_name',
            featureidkey="properties.name",
            color='id',
            hover_name='region_name',
            hover_data=['salary_avg'],
            # color_continuous_scale='Blues',
            title='<b>Количество вакансий по регионам</b>',
            labels={'id': 'Вакансий'},
            scope='europe',
            height=1500,
            width = 5000,
            center={'lat': 62, 'lon': 94},
            projection='mercator'
        )
        # fig1.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig1, use_container_width=True)
        
        # Топ-15 городов
        st.subheader("Топ-15 регионов по количеству вакансий")
        city_counts = df['region_name'].value_counts().nlargest(15)
        fig2 = px.bar(
            city_counts,
            x=city_counts.values,
            y=city_counts.index,
            orientation='h',
            color=city_counts.values,
            color_continuous_scale='Blues',
            text=city_counts.values,
            labels={'x': 'Количество вакансий', 'y': 'Регион'},
            height=600
        )
        fig2.update_traces(textposition='outside')
        st.plotly_chart(fig2, use_container_width=True)
    
    elif viz_type == "Анализ зарплат":
        st.subheader("Анализ зарплатных предложений")
        possible_categories = ['Без опыта', '1-3 года', '3-6 лет', '6+ лет']
        existing_categories = [cat for cat in possible_categories 
                            if cat in df['experience'].unique()]
        
        # Если нет ни одной категории опыта
        if not existing_categories:
            st.warning("В данных не найдены категории опыта работы")
            return
        
        df['experience'] = pd.Categorical(
            df['experience'], 
            categories=existing_categories,
            ordered=True
        )
        
        # Создаем селектор для выбора региона 
        if 'region_name' in df.columns:
            all_regions = ['Все регионы'] + sorted(df['region_name'].dropna().unique().tolist())
            selected_region = st.selectbox(
                '📍 Выберите регион:', 
                all_regions,
                index=0
            )
            
            # Фильтруем данные по региону
            if selected_region != 'Все регионы':
                filtered_df = df[df['region_name'] == selected_region]
            else:
                filtered_df = df.copy()
        else:
            filtered_df = df.copy()
            selected_region = 'Все регионы'
        
        # Группируем данные по средним зарплатам
        salary_stats = filtered_df.groupby('experience', observed=True) \
            .agg(avg_salary=('salary_avg', 'mean')) \
            .reset_index()
        
        # Проверяем, есть ли данные для отображения
        if salary_stats.empty:
            st.warning("Нет данных для отображения по выбранным параметрам")
            return
        
        # Создаем интерактивный график
        fig = px.bar(
            salary_stats,
            x='experience',
            y='avg_salary',
            color='experience',
            title=f'Средняя зарплата по опыту работы {"в регионе " + selected_region if selected_region != "Все регионы" else ""}',
            labels={'avg_salary': 'Средняя зарплата', 'experience': 'Опыт работы'},
            text_auto='.2f'
        )
        
        # Настраиваем отображение графика
        fig.update_layout(
            xaxis_title='Опыт работы',
            yaxis_title='Средняя зарплата',
            showlegend=False,
            hovermode='x unified'
        )
        fig.update_traces(
            textfont_size=12,
            textangle=0,
            textposition='outside'
        )
        
        # Отображаем график
        st.plotly_chart(fig, use_container_width=True)
        
        # Показываем таблицу с данными
        st.subheader('📈 Статистика по зарплатам')
        st.dataframe(
            salary_stats.rename(columns={
                'avg_salary': 'Средняя зарплата',
                'experience': 'Опыт работы'
            }),
            hide_index=True
        )  


        # Зарплаты по опыту
        st.subheader("Зарплаты в зависимости от требуемого образования")
        fig2 = px.box(
            df,
            x='requirement_education',
            y='salary_avg',
            color='requirement_education',
            points=False,
            labels={'salary_avg': 'Зарплата (руб)', 'requirement_education': 'Требуемый опыт'},
            height=500
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    elif viz_type == "Топы по категориям":
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Топ-10 профессий")
            top_jobs = df['job_name'].value_counts().nlargest(10)
            fig1 = px.bar(
                top_jobs,
                x=top_jobs.values,
                y=top_jobs.index,
                orientation='h',
                color=top_jobs.values,
                color_continuous_scale='Teal',
                text=top_jobs.values,
                labels={'x': 'Количество', 'y': ''},
                height=500
            )
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            st.subheader("Топ-10 компаний")
            top_companies = df['company_name'].value_counts().nlargest(10)
            fig2 = px.bar(
                top_companies,
                x=top_companies.values,
                y=top_companies.index,
                orientation='h',
                color=top_companies.values,
                color_continuous_scale='Peach',
                text=top_companies.values,
                labels={'x': 'Количество', 'y': ''},
                height=500
            )
            st.plotly_chart(fig2, use_container_width=True)
        
        st.subheader("Топ-10 по средней зарплате")
        top_salary = df.groupby('company_name')['salary_avg'].mean().nlargest(10).reset_index()
        fig3 = px.bar(
            top_salary,
            x='salary_avg',
            y='company_name',
            orientation='h',
            color='salary_avg',
            color_continuous_scale='Purp',
            text=top_salary['salary_avg'].apply(lambda x: f"{int(x):,} ₽"),
            labels={'salary_avg': 'Средняя зарплата', 'company_name': ''},
            height=500
        )
        fig3.update_traces(textposition='outside')
        fig3.update_layout(xaxis_title="Средняя зарплата (руб)")
        st.plotly_chart(fig3, use_container_width=True)

def show_metrics_analysis(df):
    st.header("Анализ ключевых метрик")
    
    # Выбор временного периода
    time_period = st.sidebar.selectbox(
        "Временной период",
        ["Последние 7 дней", "Последний месяц", "Последний год", "Всё время"]
    )
    
    # Фильтрация по времени
    now = datetime.now()
    if time_period == "Последние 7 дней":
        time_filter = now - pd.Timedelta(days=7)
    elif time_period == "Последний месяц":
        time_filter = now - pd.Timedelta(days=30)
    elif time_period == "Последний год":
        time_filter = now - pd.Timedelta(days=365)
    else:
        time_filter = df['date'].min()
    
    time_df = df[df['date'] >= time_filter]
    
    # Показываем метрики
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Всего вакансий", time_df.shape[0])
    with col2:
        st.metric("Средняя зарплата", f"{int(time_df['salary_avg'].mean())} ₽")
    with col3:
        st.metric("Уникальных компаний", time_df['company_name'].nunique())
    
    # Графики динамики
    st.subheader("Динамика вакансий")
    
    freq = st.radio(
        "Группировка",
        ["По дням", "По неделям", "По месяцам"],
        horizontal=True
    )
    
    if freq == "По дням":
        freq_param = 'D'
    elif freq == "По неделям":
        freq_param = 'W'
    else:
        freq_param = 'M'
    
    dynamic_df = time_df.groupby(pd.Grouper(key='date', freq=freq_param)).agg({
        'id': 'count',
        'salary_avg': 'mean'
    }).reset_index()
    
    fig = px.line(
        dynamic_df,
        x='date',
        y='id',
        title='Количество вакансий',
        labels={'id': 'Количество', 'date': 'Дата'}
    )
    st.plotly_chart(fig, use_container_width=True)
    

def main():
    st.set_page_config(layout="wide", page_title="Аналитика вакансий")
    
    # Загрузка данных
    df = load_data()
    
    # Навигация в сайдбаре
    st.sidebar.title("Навигация")
    page = st.sidebar.radio(
        "Выберите раздел",
        ["Таблица вакансий", "Визуализации", "Анализ метрик"]
    )
    
    # Отображение выбранной страницы
    if page == "Таблица вакансий":
        show_vacancies_table(df)
    elif page == "Визуализации":
        show_visualizations(df)
    elif page == "Анализ метрик":
        show_metrics_analysis(df)

if __name__ == "__main__":
    main()