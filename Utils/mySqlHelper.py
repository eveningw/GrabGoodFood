import pymysql

def get_connection():
    """建立並返回 MySQL 連線"""
    try:
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='password',
            db='TESTDB',
            charset='utf8mb4'
        )
        print('Successfully connected!')
        return conn
    except pymysql.MySQLError as e:
        print(f'資料庫連線失敗: {e}')
        return None