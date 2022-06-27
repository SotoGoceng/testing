import pymysql
import pandas as pd
import sys
def csv_to_mysql(load_sql, host, user, password, sql):
    '''
    This function load a csv file to MySQL table according to
    the load_sql statement.
    '''
    try:
        con = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                autocommit=True,
                                local_infile=1)
        print('Connected to DB: {}'.format(host))
        # Create cursor and execute Load SQL
        cursor = con.cursor()
        cursor.execute(load_sql)
        cursor.execute(sql)
        print('Succuessfully loaded the table from csv.')
        # Fetch all the records
        result = cursor.fetchall()
        for i in result:
             print(i)
        con.close()
       
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1) 



# Execution Example
load_sql = "LOAD DATA LOCAL INFILE 'Data_Contoh1.csv' INTO TABLE imambonjol.DKI_Jakarta FIELDS TERMINATED BY '#'  IGNORE 1 LINES;"
host = 'localhost'
user = 'root'
password = 'X3nial123!'
sql = "SELECT * FROM imambonjol.DKI_Jakarta"
csv_to_mysql(load_sql, host, user, password, sql)