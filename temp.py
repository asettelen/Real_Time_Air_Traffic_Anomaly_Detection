def create_database(database_name="activus"):
    '''
    create a database using: CREATE DATABASE database_name;
    select a database using: USE database_name '''
    conn = mconn.connect(host="192.168.37.86", port=3306, user="root", password="secret")
    cur = conn.cursor()
    cur.execute("CREATE DATABASE %s;"%(database_name))
    conn.commit()
    conn.close()

def show_available_databases():
    conn = mconn.connect(host="192.168.37.86", port=3306, user="azerty", password="azerty")
    cur = conn.cursor()
    cur.execute("SHOW DATABASES;")
    databases = cur.fetchall()
    for database in databases:
        print(database)
    conn.close()

def create_table(table_name="tableX", parameters="x INT, y FLOAT", database_name="activus"):
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret", database=database_name)
    cur = conn.cursor()
    n = cur.execute("CREATE TABLE IF NOT EXISTS %s (%s);"%(table_name, parameters))
    print(n)
    conn.commit()
    conn.close()

def show_available_tables(database_name='activus'):
    conn = mconn.connect(host="192.168.37.86", port=3306, user="azerty", password="azerty", database=database_name)
    cur = conn.cursor()
    tables = cur.execute("SHOW TABLES FROM %s;" % (database_name))
    #tables = cur.execute("SHOW TABLES")
    print(tables)
    if not tables: return  
    for table in tables:
        print(table)
    conn.close()

def drop_table(table_name="tabl1", database_name="test"):
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret", database=database_name)
    cur = conn.cursor()
    cur.execute("DROP TABLE %s;"%(table_name))
    conn.commit()
    conn.close()

def draft_populate(table_name="tableX", database_name="test"):
    conn = mconn.connect(host="localhost", port=3306, user="root", password="secret", database=database_name)
    cur = conn.cursor()
    datas = requests.get(url="http://192.168.37.142:50005/stream/2019-05-04-00:00:00/2019-05-04-06:00:00", stream=True)
    tsOld=0
    n=0
    # ts = 2020-01-02 23!12:32
    for line in datas.iter_lines():
        src, cat, tid, ts, dst, sac, sic, tod, tn, theta, rho, fl, cgs, chdg = line.split(",")
        ts = Integer.par(ts)
        #t = datetime.strptime(ts,"%Y-%m-%d %H:%M:%S")
        if ts-tsOld>5:
            tsOld=ts
            n=0
        n+=1
        cur.execute("INSERT INTO %s VALUES (%s, %s, %s, %s);"%(table_name,(t-datetime(1970,1,1)).total_seconds(), float(y), float(yhat_lower), float(yhat_upper)))
        conn.commit()
        sleep(2)
    conn.close()
    
def connect(database_name):
    conn = mconn.connect(host="192.168.37.86", port=3306, user="azerty", password="azerty", database=database_name)
    return conn

parameters="tid STRING, dst STRING, \
#             ds STRING, y FLOAT, yhat FLOAT, yhat_lower FLOAT, yhat_upper FLOAT"
    
def query_from_table(table_name="tabl1", database_name="test"):
    conn = mconn.connect(host="192.168.37.86", port=3306, user="azerty", password="azerty", database=database_name)
    cur = conn.cursor()
    cur.execute("SELECT * FROM %s;"%(table_name))
    rows = cur.fetchall()
    conn.close()
    for row in rows:
        print(row)
        
def insert_table(table_name, conn, tid, dst, ds, y, yhat, yhat_lower, yhat_upper):
    cur = conn.cursor()
    #print("INSERT INTO %s VALUES (%s, %s, %s, %s, %s, %s, %s);"%(table_name, '\'' + str(tid) + '\'', '\'' + str(dst) + '\'', '\'' + str(ds) + '\'', float(y), float(yhat), float(yhat_lower), float(yhat_upper)))
    cur.execute("INSERT INTO %s VALUES (%s, %s, %s, %s, %s, %s, %s);"%(table_name, '\'' + str(tid) + '\'', '\'' + str(dst) + '\'', '\'' + str(ds) + '\'', y, yhat, yhat_lower, yhat_upper))
    #cur.execute("INSERT INTO %s VALUES (%s, %s, %s, %s, %s, %s, %s);"%(table_name, tid, dst, ds, float(y), float(yhat), float(yhat_lower), float(yhat_upper)))
    conn.commit()
    
def disconnect(database_name):
    mconn.connect(host="192.168.37.86", port=3306, user="azerty", password="azerty", database=database_name).close()

#create_database()
#show_available_databases()
# create_table(table_name="planes_trend", parameters="ts TIMESTAMP, number_of_planes INT")
#show_available_tables()
#show_available_tables()
#draft_populate()

#query_from_table(table_name = 'CHDG', database_name='activus')
# drop_table()

#show_available_databases()

#insert_table('CHDG', connect(database_name='activus'), tid='yolo', dst='yolo', ds='yolo', y=0, yhat=0, yhat_lower=0, yhat_upper=0)

insert_table('CHDG', connect(database_name='activus'), tid='yolo', dst='yolo', ds='yolo', y='NULL', yhat='NULL', yhat_lower='NULL', yhat_upper='NULL')
disconnect('activus')
#connect(database_name)
#conn.close()
