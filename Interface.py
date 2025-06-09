#!/usr/bin/python
#
# Interface cho bài tập
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='081204', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def LoadRatings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Hàm để tải dữ liệu từ file @ratingsfilepath vào bảng có tên @ratingstablename.
    """
    con = openconnection
    cur = con.cursor()
    
    # Xóa bảng nếu đã tồn tại
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    
    # Tạo bảng với các cột phụ để phù hợp với định dạng file
    cur.execute("CREATE TABLE " + ratingstablename + 
               " (userid INT, extra1 CHAR, movieid INT, extra2 CHAR, rating FLOAT, extra3 CHAR, timestamp BIGINT)")
    
    # Tải dữ liệu từ file sử dụng copy_from để hiệu quả với tập dữ liệu lớn
    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')
    
    # Loại bỏ các cột phụ
    cur.execute("ALTER TABLE " + ratingstablename + 
               " DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp")
    
    con.commit()
    cur.close()

def Range_Partition (ratingstablename, numberofpartitions, openconnection):
    """
    Hàm để tạo các phân mảnh của bảng chính dựa trên khoảng giá trị của rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Tính toán kích thước khoảng cho mỗi phân mảnh
    range_size = 5.0 / numberofpartitions
    
    # Xóa các bảng phân mảnh đã tồn tại nếu có
    for i in range(numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS " + RANGE_TABLE_PREFIX + str(i))
    
    # Tạo các bảng phân mảnh và chèn dữ liệu
    for i in range(numberofpartitions):
        min_rating = i * range_size
        max_rating = min_rating + range_size
        partition_name = RANGE_TABLE_PREFIX + str(i)
        
        # Tạo bảng phân mảnh
        cur.execute("CREATE TABLE " + partition_name + " (userid INT, movieid INT, rating FLOAT)")
        
        # Chèn dữ liệu vào phân mảnh
        if i == 0:
            cur.execute("INSERT INTO " + partition_name + 
                       " SELECT userid, movieid, rating FROM " + ratingstablename + 
                       " WHERE rating >= " + str(min_rating) + 
                       " AND rating <= " + str(max_rating))
        else:
            cur.execute("INSERT INTO " + partition_name + 
                       " SELECT userid, movieid, rating FROM " + ratingstablename + 
                       " WHERE rating > " + str(min_rating) + 
                       " AND rating <= " + str(max_rating))
    
    con.commit()
    cur.close()

def RoundRobin_Partition (ratingstablename, numberofpartitions, openconnection):
    """
    Hàm để tạo các phân mảnh của bảng chính sử dụng phương pháp round robin.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Xóa các bảng phân mảnh đã tồn tại nếu có
    for i in range(numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS " + RROBIN_TABLE_PREFIX + str(i))
    
    # Tạo các bảng phân mảnh và chèn dữ liệu
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("CREATE TABLE " + table_name + " (userid INT, movieid INT, rating FLOAT)")
        cur.execute("INSERT INTO " + table_name + 
                   " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + 
                   "(SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rnum FROM " + 
                   ratingstablename + ") as temp WHERE MOD(temp.rnum-1, " + str(numberofpartitions) + ") = " + str(i))
    
    con.commit()
    cur.close()

def RoundRobin_Insert (ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm để chèn một hàng mới vào bảng chính và phân mảnh cụ thể dựa trên phương pháp round robin.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Chèn hàng mới vào bảng chính
    cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    
    # Đếm số lượng phân mảnh
    num_partitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    
    # Đếm tổng số hàng trong bảng chính
    cur.execute("SELECT COUNT(*) FROM " + ratingstablename)
    total_rows = cur.fetchone()[0]
    
    # Tính toán chỉ số phân mảnh cho hàng mới
    partition_idx = (total_rows - 1) % num_partitions
    
    # Chèn hàng mới vào phân mảnh thích hợp
    cur.execute("INSERT INTO " + RROBIN_TABLE_PREFIX + str(partition_idx) + 
               " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    
    con.commit()
    cur.close()

def Range_Insert (ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm để chèn một hàng mới vào bảng chính và phân mảnh cụ thể dựa trên khoảng giá trị rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Chèn hàng mới vào bảng chính
    cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    
    # Đếm số lượng phân mảnh
    num_partitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    
    # Tính toán kích thước khoảng cho mỗi phân mảnh
    range_size = 5.0 / num_partitions
    
    # Tính toán chỉ số phân mảnh cho hàng mới
    partition_idx = int(rating / range_size)
    
    # Trường hợp đặc biệt: nếu rating chính xác tại một ranh giới, nó sẽ đi vào phân mảnh thấp hơn (trừ rating 0)
    if rating % range_size == 0 and rating > 0:
        partition_idx -= 1
    
    # Đảm bảo chỉ số nằm trong giới hạn
    partition_idx = min(partition_idx, num_partitions - 1)
    
    # Chèn hàng mới vào phân mảnh thích hợp
    cur.execute("INSERT INTO " + RANGE_TABLE_PREFIX + str(partition_idx) + 
               " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    
    con.commit()
    cur.close()

def create_db(dbname):
    """
    Hàm này đầu tiên kiểm tra xem đã tồn tại cơ sở dữ liệu với tên đã cho hay chưa, nếu chưa thì tạo mới.
    :return:None
    """
    # Kết nối đến cơ sở dữ liệu mặc định
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Kiểm tra xem đã tồn tại cơ sở dữ liệu với tên giống nhau chưa
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Tạo cơ sở dữ liệu
    else:
        print('Cơ sở dữ liệu có tên {0} đã tồn tại'.format(dbname))

    # Dọn dẹp
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Hàm đếm số lượng bảng có tiền tố @prefix trong tên.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE '" + prefix + "%'")
    count = cur.fetchone()[0]
    cur.close()
    return count