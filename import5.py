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
        cursor.execute('USE imambojol;')
        cursor.execute('DROP TABLE IF EXISTS DKI_Jakarta;')
        print('Creating table....')
# in the below line please pass the create table statement which you want #to create
        cursor.execute("CREATE TABLE DKI_Jakarta(id varchar(255),nkk varchar(255),nik varchar(255),nama varchar(255),tempat_lahir varchar(255),tgl_lahir varchar(255),kawin varchar(255),jenis_kelamin varchar(255),alamat varchar(255),rt varchar(255),rw varchar(255),difabel varchar(255),keterangan varchar(255),sumberdata varchar(255),tps varchar(255),kel_id varchar(255),kd_pro varchar(255),pro varchar(255),kd_kab varchar(255),kab varchar(255),kd_kec varchar(255),kec varchar(255),kel varchar(255))")
        print("Table is created....")
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