import os
import zipfile
import io
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://s3.amazonaws.com/tripdata"

def get_first_monday(year: int, month: int) -> datetime.date:
    first_day = datetime(year, month, 1)
    days_ahead = 0 - first_day.weekday()
    if days_ahead < 0:
        days_ahead += 7
    result = (first_day + timedelta(days=days_ahead)).date()
    return result

@dag(
    dag_id="dag_incremental_load",
    start_date=datetime(2015, 9, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["citibike", "taskflow"],
)
def dag_incremental_load():

    @task
    def download_and_process_data(**kwargs):
        logger.info("Начало выполнение таска download_and_process_data")
        
        execution_date = kwargs["ds_nodash"]
        logger.info(f"Дата выполнения (ds_nodash): {execution_date}")
        
        current_date = datetime.strptime(execution_date, "%Y%m%d")
        year = current_date.year
        month = current_date.month
        year_month = current_date.strftime("%Y%m")
        
        logger.info(f"Обработка периода: {year_month}")
        logger.info(f"Год: {year}, Месяц: {month}")

        # Названия файлов на S3 имеют опечатки и вариации
        possible_filenames = [
            f"JC-{year_month}-citibike-tripdata.csv.zip",
            f"JC-{year_month}-citibike-tripdata.zip",
            f"JC-{year_month}-citbike-tripdata.csv.zip"
        ]

        response = None
        target_filename = None
        
        for idx, filename in enumerate(possible_filenames, 1):
            url = f"{BASE_URL}/{filename}"
            try:
                res = requests.get(url, timeout=30)
                if res.status_code == 200:
                    logger.info(f"Файл найден: {filename}")
                    response = res
                    target_filename = filename
                    break
            except Exception as e:
                logger.error(f"Ошибка при запросе к {url}: {e}")

        if not response:
            error_msg = f"Файл для периода {year_month} не найден после проверки всех вариантов"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info(f"Начало распаковки архива: {target_filename}")
        
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                logger.info("ZIP-архив открыт успешно")
                
                all_files = z.namelist()
                logger.info(f"Содержимое архива ({len(all_files)} файлов): {all_files}")
                
                csv_files = [f for f in all_files if f.endswith('.csv') or not f.endswith('/')]
                
                logger.info(f"Чтение файла: {csv_files[0]}")
                with z.open(csv_files[0]) as csv_file:
                    df = pd.read_csv(csv_file)
                    logger.info(f"CSV прочитан успешно. Размер DataFrame: {df.shape}")
                    logger.info(f"Количество строк: {len(df)}, Количество колонок: {len(df.columns)}")
                    
        except Exception as e:
            logger.error(f"Ошибка при распаковке архива: {e}")
            raise

        logger.info("Стандартизация колонок начинается")
        original_columns = df.columns.tolist()
        
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        logger.info(f"Колонки после стандартизации: {df.columns.tolist()}")

        logger.info("Определение схемы данных")
        
        new_schema_indicators = ['ride_id', 'started_at', 'ended_at', 'rideable_type', 'member_casual']
        old_schema_indicators = ['tripduration', 'trip_duration', 'starttime', 'start_time', 'stoptime', 'stop_time', 'bikeid', 'bike_id']
        
        has_new_indicators = any(col in df.columns for col in new_schema_indicators)
        has_old_indicators = any(col in df.columns for col in old_schema_indicators)
        
        if has_new_indicators and not has_old_indicators:
            is_new_schema = True
        elif has_old_indicators and not has_new_indicators:
            is_new_schema = False
        elif has_new_indicators and has_old_indicators:
            if 'ride_id' in df.columns:
                is_new_schema = True
            elif 'tripduration' in df.columns or 'trip_duration' in df.columns:
                is_new_schema = False
            else:
                error_msg = f"Не удалось однозначно определить схему данных. Колонки: {df.columns.tolist()}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        else:
            error_msg = f"Не удалось определить схему данных. Колонки: {df.columns.tolist()}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if is_new_schema:
            expected_columns = [
                'ride_id', 'rideable_type', 'started_at', 'ended_at',
                'start_station_name', 'start_station_id', 'end_station_name',
                'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual'
            ]
            column_renames = {
                'start_longitude': 'start_lng',
                'end_longitude': 'end_lng',
                'start_latitude': 'start_lat',
                'end_latitude': 'end_lat',
                'start_lon': 'start_lng',
                'end_lon': 'end_lng'
            }
            target_table = "citibike_trips_new_schema"
            date_col = "started_at"
        else:
            expected_columns = [
                'tripduration', 'starttime', 'stoptime', 'start_station_id',
                'start_station_name', 'start_station_latitude', 'start_station_longitude',
                'end_station_id', 'end_station_name', 'end_station_latitude',
                'end_station_longitude', 'bikeid', 'usertype', 'birth_year', 'gender'
            ]
            column_renames = {
                'trip_duration': 'tripduration',
                'start_time': 'starttime',
                'stop_time': 'stoptime',
                'bike_id': 'bikeid',
                'user_type': 'usertype',
                'birth_year': 'birth_year',
                'gender': 'gender'
            }
            target_table = "citibike_trips_old_schema"
            date_col = "starttime"
        
        for old_name, new_name in column_renames.items():
            if old_name in df.columns and new_name not in df.columns:
                logger.info(f"Переименование: {old_name} -> {new_name}")
                df.rename(columns={old_name: new_name}, inplace=True)

        if date_col not in df.columns:
            logger.warning(f"Колонка '{date_col}' не найдена. Поиск альтернатив...")
            alternative_dates = ['start_time', 'started_at']
            for alt_date in alternative_dates:
                if alt_date in df.columns:
                    logger.info(f"Найдена альтернативная колонка: {alt_date} -> {date_col}")
                    df.rename(columns={alt_date: date_col}, inplace=True)
                    break
            else:
                logger.error(f"Доступные: {df.columns.tolist()}")

        logger.info("Обработка колонок")
        available_columns = [col for col in expected_columns if col in df.columns]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        logger.info(f"Доступные колонки ({len(available_columns)}): {available_columns}")
        
        if missing_columns:
            logger.warning(f"Отсутствующие колонки ({len(missing_columns)}): {missing_columns}")
            for col in missing_columns:
                df[col] = None
        
        # Приводим к единому набору колонок
        df = df[expected_columns]
        logger.info(f"Финальный набор колонок: {expected_columns}")
        logger.info(f"Размер DataFrame после обработки колонок: {df.shape}")

        # Проверяем наличие колонки с датой
        if date_col not in df.columns:
            error_msg = f"Колонка с датой '{date_col}' не найдена. Доступные: {df.columns.tolist()}"
            logger.error(error_msg)
            raise KeyError(error_msg)

        logger.info("Конвертация даты")
        logger.info(f"Конвертация колонки '{date_col}' в datetime")
        
        before_convert = len(df)
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        null_dates = df[date_col].isna().sum()
        
        if null_dates > 0:
            logger.warning(f"Некорректных дат: {null_dates} из {before_convert}")

        before_dropna = len(df)
        df = df.dropna()
        after_dropna = len(df)
        print(f"Удалено NULL значений: {before_dropna - after_dropna}")

        logger.info("Фильтрация: первый понедельник месяца")
        first_monday = get_first_monday(year, month)
        
        before_filter = len(df)
        df = df[df[date_col].dt.date == first_monday]
        after_filter = len(df)
        
        logger.info(f"Строк до фильтрации: {before_filter}")
        logger.info(f"Строк после фильтрации: {after_filter}")
        logger.info(f"Отфильтровано строк: {before_filter - after_filter}")

        logger.info("Загрузка в базу данных")
        row_count = len(df)
        logger.info(f"Количество строк для загрузки: {row_count}")
        logger.info(f"Целевая таблица: {target_table}")
        
        if row_count > 0:
            try:
                logger.info("Подключение к базе данных...")
                pg_hook = PostgresHook(postgres_conn_id="greenplum_conn")
                engine = pg_hook.get_sqlalchemy_engine()
                logger.info("Подключение установлено успешно")
                
                df_to_save = df.copy()
                datetime_cols = df_to_save.select_dtypes(include=['datetime64']).columns
                
                for col in datetime_cols:
                    df_to_save[col] = df_to_save[col].astype(str)
                
                logger.info(f"Вставка {len(df_to_save)} строк в таблицу {target_table}...")
                start_time = datetime.now()
                
                df_to_save.to_sql(target_table, con=engine, if_exists="append", index=False)
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.info(f"Данные успешно загружены в таблицу {target_table}")
                
            except Exception as e:
                logger.error(f"Ошибка при загрузке в БД: {e}")
                raise
        else:
            logger.warning("Нет данных для загрузки после фильтрации")
        
        return {"row_count": row_count, "period": year_month}

    @task
    def update_airflow_variable(processing_result: dict):
        logger.info("Обновление переменной")
        
        period = processing_result["period"]
        count = processing_result["row_count"]
        
        variable_key = f"citibike_loaded_records_{period}"
        logger.info(f"Установка переменной {variable_key} = {count}")
        
        try:
            Variable.set(key=variable_key, value=str(count))
            logger.info(f"Переменная {variable_key} успешно обновлена")
        except Exception as e:
            logger.error(f"Ошибка при обновлении переменной: {e}")
            raise

    data_res = download_and_process_data()
    update_airflow_variable(data_res)

dag_incremental_load()