import sqlite3
from datetime import datetime

def create_table_files():
    conn = sqlite3.connect("graf.db")
    cur = conn.cursor()
    #cur.execute("CREATE TABLE lines (ds VARCHAR(25), y FLOAT, y_low FLOAT, y_upp FLOAT, tid VARCHAR(10), dst VARCHAR(20));")
    cur.execute("CREATE TABLE liness (ds INT, y FLOAT, y_low FLOAT, y_upp FLOAT, tid VARCHAR(10), dst VARCHAR(20));")
    conn.commit()
    conn.close()

def populate_table_files():
    conn = sqlite3.connect("graf.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()
    with open("pourKaderEtCharles.csv", "r") as f:
        next(f)
        for line_ in f:
            line = line_.replace("\n", "")
            _,y,ds,yhat_lower,yhat_upper,TID,DST = line.split(",")
            t = datetime.strptime(ds,"%Y-%m-%d %H:%M:%S")
            # print("==%s=="%(y))
            cur.execute("INSERT INTO liness VALUES (?, ?, ?, ?, ?, ?);", ((t-datetime(1970,1,1)).total_seconds(), float(y), float(yhat_lower), float(yhat_upper), TID, DST))
        f.close()
    conn.commit()
    conn.close()

def unpopulate_table_files():
    conn = sqlite3.connect("graf.db")
    cur = conn.cursor()
    cur.execute("DELETE FROM lines;")
    conn.commit()
    conn.close()

def query_from_table_files(number=30):
    conn = sqlite3.connect("graf.db")
    cur = conn.cursor()
    cur.execute("SELECT * FROM liness;")
    rows = cur.fetchmany(number)
    conn.close()
    for row in rows:
        print(row)

def drop_table_lines():
    conn = sqlite3.connect("graf.db")
    cur = conn.cursor()
    cur.execute("DROP TABLE liness;")
    conn.commit()
    conn.close()


create_table_files()
#unpopulate_table_files()
populate_table_files()
query_from_table_files()
# drop_table_lines()
