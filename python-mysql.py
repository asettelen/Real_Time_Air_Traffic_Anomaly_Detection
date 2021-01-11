import mysql.connector as mconn
from datetime import datetime
from time import sleep

def create_database(database_name="test"):
    '''
    create a database using: CREATE DATABASE database_name;
    select a database using: USE database_name '''
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret")
    cur = conn.cursor()
    cur.execute("CREATE DATABASE %s;"%(database_name))
    conn.commit()
    conn.close()

def show_available_databases():
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret")
    cur = conn.cursor()
    cur.execute("SHOW DATABASES;")
    databases = cur.fetchall()
    for database in databases:
        print(database)
    conn.close()

def create_table(table_name="tabl1", database_name="test"):
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret", database=database_name)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS %s (x INT, y FLOAT, y_low FLOAT, y_upp FLOAT);"%(table_name))
    conn.commit()
    conn.close()

def drop_table(table_name="tabl1", database_name="test"):
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret", database=database_name)
    cur = conn.cursor()
    cur.execute("DROP TABLE %s;"%(table_name))
    conn.commit()
    conn.close()

def draft_populate(table_name="tabl1", database_name="test"):
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret", database=database_name)
    cur = conn.cursor()
    with open("pourKaderEtCharles.csv", "r") as f:
        next(f)
        for line_ in f:
            line = line_.replace("\n", "")
            _,y,ds,yhat_lower,yhat_upper,TID,DST = line.split(",")
            t = datetime.strptime(ds,"%Y-%m-%d %H:%M:%S")
            # print("==%s=="%(y))
            cur.execute("INSERT INTO %s VALUES (%s, %s, %s, %s);"%(table_name,(t-datetime(1970,1,1)).total_seconds(), float(y), float(yhat_lower), float(yhat_upper)))
            conn.commit()
            sleep(2)
        f.close()
    conn.close()

def query_from_table(table_name="tabl1", database_name="test"):
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret", database=database_name)
    cur = conn.cursor()
    cur.execute("SELECT * FROM %s;"%(table_name))
    rows = cur.fetchall()
    conn.close()
    for row in rows:
        print(row)

# create_database()
# show_available_databases()
# create_table()
draft_populate()
# query_from_table()
# drop_table()