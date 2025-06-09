import time
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import os
import Interface as MyAssignment

# Tạo thư mục kết quả
os.makedirs("results", exist_ok=True)

def get_connection():
    return MyAssignment.getopenconnection(dbname='dds_assgn1')

def clean_tables(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ratings")
    for i in range(5):  # Giả định tối đa 5 phân mảnh
        cur.execute(f"DROP TABLE IF EXISTS range_part{i}")
        cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i}")
    conn.commit()
    cur.close()

# 1. Đo thời gian tải dữ liệu
def test_load_time():
    print("\n1. Đo thời gian tải dữ liệu...")
    conn = get_connection()
    clean_tables(conn)
    
    start_time = time.time()
    MyAssignment.LoadRatings('ratings', 'ratings.dat', conn)
    load_time = time.time() - start_time
    
    print(f"Thời gian tải dữ liệu: {load_time:.6f} giây")
    conn.close()
    return load_time

# 2. Đo thời gian phân mảnh
def test_partition_time():
    print("\n2. Đo thời gian phân mảnh...")
    conn = get_connection()
    clean_tables(conn)
    MyAssignment.LoadRatings('ratings', 'ratings.dat', conn)
    
    # Phân mảnh theo khoảng
    start_time = time.time()
    MyAssignment.Range_Partition('ratings', 5, conn)
    range_time = time.time() - start_time
    print(f"Thời gian phân mảnh theo khoảng: {range_time:.6f} giây")
    
    # Dọn dẹp và chuẩn bị cho phân mảnh vòng tròn
    clean_tables(conn)
    MyAssignment.LoadRatings('ratings', 'ratings.dat', conn)
    
    # Phân mảnh vòng tròn
    start_time = time.time()
    MyAssignment.RoundRobin_Partition('ratings', 5, conn)
    rrobin_time = time.time() - start_time
    print(f"Thời gian phân mảnh vòng tròn: {rrobin_time:.6f} giây")
    
    conn.close()
    return range_time, rrobin_time

# 3. Đo thời gian chèn dữ liệu
def test_insert_time():
    print("\n3. Đo thời gian chèn dữ liệu...")
    conn = get_connection()
    
    # Chuẩn bị môi trường cho phân mảnh theo khoảng
    clean_tables(conn)
    MyAssignment.LoadRatings('ratings', 'ratings.dat', conn)
    MyAssignment.Range_Partition('ratings', 5, conn)
    
    # Đo thời gian chèn theo khoảng
    start_time = time.time()
    MyAssignment.Range_Insert('ratings', 100, 2, 3, conn)
    range_insert_time = time.time() - start_time
    print(f"Thời gian chèn theo khoảng: {range_insert_time:.6f} giây")
    
    # Chuẩn bị môi trường cho phân mảnh vòng tròn
    clean_tables(conn)
    MyAssignment.LoadRatings('ratings', 'ratings.dat', conn)
    MyAssignment.RoundRobin_Partition('ratings', 5, conn)
    
    # Đo thời gian chèn vòng tròn
    start_time = time.time()
    MyAssignment.RoundRobin_Insert('ratings', 200, 3, 4, conn)
    rrobin_insert_time = time.time() - start_time
    print(f"Thời gian chèn vòng tròn: {rrobin_insert_time:.6f} giây")
    
    conn.close()
    return range_insert_time, rrobin_insert_time

# 4. Tạo biểu đồ tổng hợp
def create_summary_chart(load_time, range_time, rrobin_time, range_insert_time, rrobin_insert_time):
    print("\n4. Tạo biểu đồ tổng hợp...")
    
    # Dữ liệu cho biểu đồ
    operations = ['Tải dữ liệu', 'Phân mảnh\ntheo khoảng', 'Phân mảnh\nvòng tròn', 
                  'Chèn\ntheo khoảng', 'Chèn\nvòng tròn']
    times = [load_time, range_time, rrobin_time, range_insert_time, rrobin_insert_time]
    colors = ['#3498db', '#2ecc71', '#9b59b6', '#e67e22', '#f1c40f']
    
    # Tạo biểu đồ
    plt.figure(figsize=(12, 6))
    bars = plt.bar(operations, times, color=colors)
    
    # Thêm giá trị lên mỗi cột
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.0001,
                 f'{height:.6f}s', ha='center', va='bottom')
    
    plt.title('Thời gian thực hiện các hoạt động', fontsize=14)
    plt.ylabel('Thời gian (giây)', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    # Lưu biểu đồ
    plt.savefig('results/summary10m.png', dpi=300, bbox_inches='tight')
    print("Đã lưu biểu đồ tổng hợp: results/summary10m.png")

# Thực hiện các phép đo và tạo biểu đồ
if __name__ == "__main__":
    print("Bắt đầu phân tích hiệu suất...")
    
    # Đo các thông số
    load_time = test_load_time()
    range_time, rrobin_time = test_partition_time()
    range_insert_time, rrobin_insert_time = test_insert_time()
    
    # Tạo biểu đồ
    create_summary_chart(load_time, range_time, rrobin_time, range_insert_time, rrobin_insert_time)
    
    # Tổng hợp kết quả
    print("\n--- KẾT QUẢ PHÂN TÍCH HIỆU SUẤT ---")
    print(f"Thời gian tải dữ liệu: {load_time:.6f} giây")
    print(f"Thời gian phân mảnh theo khoảng: {range_time:.6f} giây")
    print(f"Thời gian phân mảnh vòng tròn: {rrobin_time:.6f} giây")
    print(f"Thời gian chèn theo khoảng: {range_insert_time:.6f} giây")
    print(f"Thời gian chèn vòng tròn: {rrobin_insert_time:.6f} giây")
    print("\nPhân tích hiệu suất hoàn tất!")