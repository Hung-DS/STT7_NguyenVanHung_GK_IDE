from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import pandas as pd
import os
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'read_and_insert_data_to_db',
    default_args=default_args,
    description='Đọc dữ liệu từ CSV và insert vào PostgreSQL',
    schedule_interval='0 09 * * *',
    start_date=datetime(2025, 4, 5),
    catchup=False,
)

def get_db_connection():
    return psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )

def create_table_if_not_exists():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tintuc (
            id SERIAL PRIMARY KEY,
            title TEXT,
            content TEXT,
            category TEXT,
            source_url TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Đã tạo bảng 'tintuc' (nếu chưa tồn tại).")
    except Exception as e:
        print(f"❌ Lỗi khi tạo bảng: {e}")
        raise

def clean_data(df):
    # Làm sạch dữ liệu
    for column in ['title', 'summary']:
        df[column] = df[column].astype(str).apply(lambda x: re.sub(r'<.*?>', '', x))  # Loại bỏ thẻ HTML
        df[column] = df[column].apply(lambda x: x.lower().strip())  # Chuyển thành chữ thường và loại bỏ khoảng trắng thừa
    return df

def insert_data_from_csv_to_db():
    file_path = '/input_data/vnexpress_ai_articles.csv'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Không tìm thấy file: {file_path}")

    df = pd.read_csv(file_path)
    df = clean_data(df)

    print("📄 Dữ liệu sau khi làm sạch:")
    print(df.head())

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            try:
                # Kiểm tra trùng bằng title và url
                cursor.execute(
                    "SELECT id FROM tintuc WHERE title = %s AND source_url = %s",
                    (row['title'], row['url'])
                )
                if cursor.fetchone():
                    print(f"⚠️ Bỏ qua do bản ghi đã tồn tại: {row['title']} ({row['url']})")
                    continue

                cursor.execute("""
                    INSERT INTO tintuc (title, content, category, source_url)
                    VALUES (%s, %s, %s, %s)
                """, (row['title'], row['summary'], "AI", row['url']))
                conn.commit()
                print(f"✅ Đã chèn: {row['title']}")

            except Exception as e:
                print(f"❌ Lỗi khi chèn dữ liệu: {e}")
                conn.rollback()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Lỗi kết nối hoặc chèn dữ liệu: {e}")
        raise

# Khai báo các task
create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data_from_csv_to_db_task',
    python_callable=insert_data_from_csv_to_db,
    dag=dag,
)

create_table_task >> insert_data_task
