import pymysql

def get_connection():
    print("Connecting to database...")
    return pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='carousell'
    )
