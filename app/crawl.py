import os
import requests
from bs4 import BeautifulSoup
import pandas as pd

def crawl_vnexpress_ai():
    url = "https://vnexpress.net/cong-nghe/ai"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Không thể lấy trang. Mã trạng thái: {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    articles = []
    
    # Tìm tất cả các mục bài viết
    article_items = soup.select('.item-news')
    
    for item in article_items[:10]:  # Lấy 5 bài viết đầu tiên
        article = {}
        
        # Trích xuất tiêu đề và URL
        title_element = item.select_one('.title-news a')
        if title_element:
            article['title'] = title_element.text.strip()
            article['url'] = title_element['href']
        
        # Trích xuất tóm tắt
        summary_element = item.select_one('.description')
        if summary_element:
            article['summary'] = summary_element.text.strip()
        
        # Trích xuất thời gian
        time_element = item.select_one('.time-public')
        if time_element:
            article['time'] = time_element.text.strip()
        
        # Trích xuất tác giả - thường được tìm thấy ở cuối bài viết
        author_element = item.select_one('.author')
        if author_element:
            article['author'] = author_element.text.strip()
        else:
            article['author'] = "Không xác định"  # Mặc định nếu không tìm thấy tác giả
            
        articles.append(article)
    
    return articles

if __name__ == "__main__":
    articles = crawl_vnexpress_ai()
    df = pd.DataFrame(articles)
    print(f"Đã crawl {len(df)} bài viết:")
    print(df)
    
    # Đảm bảo thư mục input_data tồn tại
    input_data_dir = './input_data'  # Đảm bảo là thư mục đúng theo môi trường của bạn
    if not os.path.exists(input_data_dir):
        os.makedirs(input_data_dir)
    
    # Lưu vào thư mục input_data
    df.to_csv(f"{input_data_dir}/vnexpress_ai_articles.csv", index=False)
    print(f"Dữ liệu đã được lưu tại {input_data_dir}/vnexpress_ai_articles.csv")
