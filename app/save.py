import pandas as pd
import psycopg2
from psycopg2.extensions import connection
import os
from typing import Optional

def get_db_connection() -> connection:
    """
    Tạo kết nối đến PostgreSQL database
    
    Returns:
        connection: Đối tượng kết nối PostgreSQL
        
    Raises:
        Exception: Nếu không thể kết nối
    """
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        print("Đã kết nối thành công đến PostgreSQL")
        return conn
    except Exception as e:
        print(f"Không thể kết nối đến PostgreSQL: {e}")
        raise

def create_articles_table(conn: connection) -> None:
    """
    Tạo bảng articles nếu chưa tồn tại
    
    Args:
        conn: Kết nối PostgreSQL
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id SERIAL PRIMARY KEY,
            article_id VARCHAR(50),
            title TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            summary TEXT,
            time VARCHAR(100),
            author VARCHAR(255),
            keywords TEXT,
            crawled_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        print("Đã tạo bảng articles nếu chưa tồn tại")
    except Exception as e:
        print(f"Lỗi khi tạo bảng: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def save_to_postgres(input_file: str = "transformed_articles.csv") -> bool:
    """
    Đọc dữ liệu từ CSV và lưu vào PostgreSQL
    
    Args:
        input_file: Đường dẫn đến file CSV đã transform
        
    Returns:
        bool: True nếu thành công, False nếu có lỗi
    """
    try:
        print(f"Bắt đầu lưu dữ liệu từ {input_file} vào PostgreSQL")
        
        # Kiểm tra file đầu vào tồn tại
        if not os.path.exists(input_file):
            print(f"File đầu vào {input_file} không tồn tại")
            return False
        
        # Đọc dữ liệu từ file CSV
        df = pd.read_csv(input_file)
        print(f"Đã đọc {len(df)} dòng dữ liệu")
        
        # Kết nối database và tạo bảng
        conn = get_db_connection()
        create_articles_table(conn)
        
        # Chèn dữ liệu vào bảng
        cursor = conn.cursor()
        
        success_count = 0
        error_count = 0
        
        for _, row in df.iterrows():
            try:
                # Chuẩn bị dữ liệu
                article_data = {
                    'article_id': row.get('article_id', ''),
                    'title': row['title'],
                    'url': row['url'],
                    'summary': row.get('summary', ''),
                    'time': row.get('time', ''),
                    'author': row.get('author', 'Không xác định'),
                    'keywords': row.get('keywords', ''),
                    'crawled_at': row.get('crawled_at', pd.Timestamp.now())
                }
                
                # Kiểm tra xem bài viết đã tồn tại chưa
                cursor.execute("SELECT id FROM articles WHERE url = %s", (article_data['url'],))
                existing = cursor.fetchone()
                
                if existing:
                    # Cập nhật bài viết đã tồn tại
                    cursor.execute("""
                    UPDATE articles 
                    SET 
                        title = %s,
                        summary = %s,
                        time = %s,
                        author = %s,
                        keywords = %s,
                        crawled_at = %s
                    WHERE url = %s
                    """, (
                        article_data['title'],
                        article_data['summary'],
                        article_data['time'],
                        article_data['author'],
                        article_data['keywords'],
                        article_data['crawled_at'],
                        article_data['url']
                    ))
                else:
                    # Chèn bài viết mới
                    cursor.execute("""
                    INSERT INTO articles 
                    (article_id, title, url, summary, time, author, keywords, crawled_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        article_data['article_id'],
                        article_data['title'],
                        article_data['url'],
                        article_data['summary'],
                        article_data['time'],
                        article_data['author'],
                        article_data['keywords'],
                        article_data['crawled_at']
                    ))
                
                success_count += 1
            
            except Exception as e:
                print(f"Lỗi khi chèn dữ liệu cho URL {row.get('url')}: {e}")
                error_count += 1
                continue
        
        conn.commit()
        print(f"Đã lưu {success_count} bản ghi vào PostgreSQL (lỗi: {error_count})")
        
        # Đóng kết nối
        cursor.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"Lỗi khi lưu dữ liệu vào PostgreSQL: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    # Lưu dữ liệu vào PostgreSQL
    success = save_to_postgres()
    
    if success:
        print("Dữ liệu đã được lưu vào PostgreSQL thành công")
    else:
        print("Không thể lưu dữ liệu vào PostgreSQL")