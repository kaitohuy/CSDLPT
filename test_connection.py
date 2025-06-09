import psycopg2

try:
    # Mở kết nối đến PostgreSQL
    conn = psycopg2.connect(
        user='postgres',
        password='081204',
        host='localhost',
        port='5432',
        database='postgres'
    )
    
    # Lấy cursor
    cursor = conn.cursor()
    
    # Kiểm tra phiên bản PostgreSQL
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print("PostgreSQL version:", db_version)
    
    # Đóng kết nối
    cursor.close()
    conn.close()
    print("Kết nối PostgreSQL thành công!")
    
except Exception as e:
    print("Lỗi khi kết nối đến PostgreSQL:", e)