import pandas as pd
import re
import os
from datetime import datetime
from typing import Optional

def clean_text(text: str) -> str:
    """
    Làm sạch văn bản: xóa ký tự đặc biệt, khoảng trắng thừa
    
    Args:
        text: Văn bản cần làm sạch
        
    Returns:
        str: Văn bản đã làm sạch
    """
    if not isinstance(text, str):
        return ""
    
    # Loại bỏ HTML tags nếu có
    text = re.sub(r'<[^>]+>', '', text)
    
    # Loại bỏ ký tự đặc biệt và giữ lại chữ, số, dấu câu cơ bản
    text = re.sub(r'[^\w\s.,;:?!()-]', '', text)
    
    # Loại bỏ khoảng trắng thừa
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def standardize_time(time_text: str) -> str:
    """
    Chuẩn hóa chuỗi thời gian sang định dạng ISO
    
    Args:
        time_text: Chuỗi thời gian cần chuẩn hóa
        
    Returns:
        str: Chuỗi thời gian đã chuẩn hóa hoặc chuỗi gốc nếu không thể chuẩn hóa
    """
    if not isinstance(time_text, str):
        return ""
    
    # Loại bỏ các từ không liên quan
    time_text = time_text.replace('Thứ', '').replace('ngày', '').strip()
    
    # Các mẫu thời gian phổ biến trong VnExpress
    patterns = [
        # 12:34, 12/12/2023
        r'(\d{1,2}):(\d{2}),\s*(\d{1,2})/(\d{1,2})/(\d{4})',
        # 12/12/2023
        r'(\d{1,2})/(\d{1,2})/(\d{4})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, time_text)
        if match:
            try:
                if len(match.groups()) == 5:  # Có cả giờ và ngày
                    hour, minute, day, month, year = match.groups()
                    dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                    return dt.isoformat()
                elif len(match.groups()) == 3:  # Chỉ có ngày
                    day, month, year = match.groups()
                    dt = datetime(int(year), int(month), int(day))
                    return dt.isoformat()
            except ValueError:
                pass
    
    # Nếu không khớp với mẫu nào, trả về chuỗi gốc
    return time_text

def extract_keywords(text: str, num_keywords: int = 5) -> list:
    """
    Trích xuất từ khóa từ văn bản
    
    Args:
        text: Văn bản cần trích xuất từ khóa
        num_keywords: Số lượng từ khóa cần trích xuất
        
    Returns:
        list: Danh sách các từ khóa
    """
    if not isinstance(text, str) or not text:
        return []
    
    # Loại bỏ stop words (đây là danh sách cơ bản, có thể mở rộng)
    stop_words = ['và', 'của', 'là', 'có', 'được', 'trong', 'cho', 'không', 'những', 'đã', 'với', 'các', 'này', 'về']
    
    # Tách từ và loại bỏ stop words
    words = [word.lower() for word in re.findall(r'\b\w{3,}\b', text) 
             if word.lower() not in stop_words]
    
    # Đếm tần suất xuất hiện của từ
    word_freq = {}
    for word in words:
        word_freq[word] = word_freq.get(word, 0) + 1
    
    # Sắp xếp theo tần suất và lấy top từ khóa
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    
    # Trả về danh sách từ khóa
    return [word for word, _ in sorted_words[:num_keywords]]

def transform_articles(input_file: str = "/input_data/vnexpress_ai_articles.csv", 
                       output_file: str = "/input/transformed_articles.csv") -> Optional[pd.DataFrame]:
    """
    Biến đổi dữ liệu bài viết
    
    Args:
        input_file: Đường dẫn đến file CSV chứa dữ liệu thô
        output_file: Đường dẫn để lưu dữ liệu đã biến đổi
        
    Returns:
        pd.DataFrame: DataFrame đã biến đổi hoặc None nếu có lỗi
    """
    try:
        print(f"Bắt đầu biến đổi dữ liệu từ {input_file}")
        
        # Kiểm tra file đầu vào
        if not os.path.exists(input_file):
            print(f"File đầu vào {input_file} không tồn tại")
            return None
        
        # Đọc dữ liệu từ file CSV
        df = pd.read_csv(input_file)
        print(f"Đã đọc {len(df)} dòng dữ liệu")
        
        # Loại bỏ các dòng trùng lặp
        df = df.drop_duplicates(subset=['url'])
        
        # Loại bỏ các dòng có giá trị null trong cột quan trọng
        df = df.dropna(subset=['title', 'url'])
        
        # Làm sạch dữ liệu văn bản
        df['title'] = df['title'].apply(clean_text)
        if 'summary' in df.columns:
            df['summary'] = df['summary'].apply(clean_text)
        
        # Chuẩn hóa thời gian
        if 'time' in df.columns:
            df['time'] = df['time'].apply(standardize_time)
        
        # Thêm cột thời gian crawl
        df['crawled_at'] = datetime.now().isoformat()
        
        # Thêm cột keywords từ summary
        if 'summary' in df.columns:
            df['keywords'] = df['summary'].apply(lambda x: ', '.join(extract_keywords(x)))
        
        # Thêm cột article_id
        df['article_id'] = df['url'].apply(lambda x: re.search(r'/(\d+)\.html', x).group(1) if re.search(r'/(\d+)\.html', x) else '')
        
        # Đảm bảo thư mục đầu ra tồn tại
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Lưu dữ liệu đã biến đổi
        df.to_csv(output_file, index=False)
        print(f"Đã biến đổi và lưu {len(df)} bản ghi vào {output_file}")
        
        return df
    
    except Exception as e:
        print(f"Lỗi khi biến đổi dữ liệu: {e}")
        return None