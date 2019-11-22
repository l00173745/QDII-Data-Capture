import pymysql
import pandas as pd
import os
from common.profiler import Profiler


class DB_mysql():
    def __init__(self,hostname="LITO9-W10", port=3306, user="MYSQLSupp", passwd="Duanka_0904", db="world"):
        self.conn = pymysql.connect(host=hostname, port=port, user=user,password=passwd,database=db,charset="utf8")
        self.dbname = db
        self.cursor = self.conn.cursor()


    def query(self,sql_str,**kwargs):
        """
        :param sql_str:
        :param kwargs:
        :return:
        """
        para_lst = tuple([kwargs[i] for i in kwargs])
        # TODO:to support multiple para
        try:
            if len(para_lst) > 0:
                df = pd.read_sql(sql_str % para_lst, self.conn)
            else:
                df = pd.read_sql(sql_str, self.conn)
            df.columns = [x.upper() for x in df.columns]
        except:
            print("Failed to run with script: %s\n" % sql_str)
        self.conn.close()
        return df

    def insert(self,tbl_nme,data):
        """
        :param tbl_nme: target table
        :param data: dictionry which key for columns and value for values
        :return:
        """
        cur = self.conn.cursor()
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=tbl_nme, keys=keys, values=values)
        try:
            cur.execute(sql, tuple(data.values()))
            print('Successful')
            self.conn.commit()
        except:
            print('Failed')
            self.conn.rollback()
        finally:
            self.conn.close()
        return True

    def read_fm_csv(self, filename):
        df = pd.read_csv(filename, keep_default_na=False, encoding='utf-8')
        table_name = '`' + os.path.split(filename)[-1].split('.')[0].replace(' ', '_') + '`'
        self.df2mysql(db_name=self.dbname, table_name=table_name, df=df)
        return True

    def make_table_sql(self, df):
        # 将csv中的字段类型转换成mysql中的字段类型
        columns = df.columns.tolist()
        types = df.ftypes
        make_table = []
        make_field = []
        for item in columns:
            item1 = '`' + item.replace(' ', '_').replace(':', '') + '`'
            if 'int' in types[item]:
                char = item1 + ' INT'
            elif 'float' in types[item]:
                char = item1 + ' FLOAT'
            elif 'object' in types[item]:
                char = item1 + ' VARCHAR(255)'
            elif 'datetime' in types[item]:
                char = item1 + ' DATETIME'
            else:
                char = item1 + ' VARCHAR(255)'
            # char = item1 + ' VARCHAR(255)'
            make_table.append(char)
            make_field.append(item1)
        return ','.join(make_table), ','.join(make_field)

    def df2mysql(self, db_name, table_name, df):
        field1, field2 = self.make_table_sql(df)
        print("create table {} (id int AUTO_INCREMENT not null primary key, {})".format(table_name, field1))
        self.cursor.execute('drop table if exists {}'.format(table_name))
        self.cursor.execute(
            "create table {} (id int AUTO_INCREMENT not null primary key,{})".format(table_name, field1))
        values = df.values.tolist()
        s = ','.join(['%s' for _ in range(len(df.columns))])
        try:
            print(len(values[0]), len(s.split(',')))
            print('insert into {}({}) values ({})'.format(table_name, field2, s), values[0])
            self.cursor.executemany('insert into {}({}) values ({})'.format(table_name, field2, s), values)
        except Exception as e:
            print(e.message)
        finally:
            self.conn.commit()




if __name__=='__main__':
    pr = Profiler()
    # unit test case 1
    # mysql=DB_mysql()
    # sql_str='select * from city'
    # df = mysql.query(sql_str)
    # mysql = DB_mysql()
    # tbl_nme = 'world.city'
    # data ={
    #     'ID':5001,
    #     'NAME':'TonyCity',
    #     'COUNTRYCODE':'AFG',
    #     'DISTRICT':'Herat',
    #     'POPULATION':190000
    # }
    # mysql.insert(tbl_nme,data)
    # unit test case 2
    M = DB_mysql()
    # csv文件目录
    dir = 'D:/temp\MSRA/results_fullset_20191122'
    file_list = os.listdir(dir)
    for i in range(len(file_list)):
        file_path = os.path.join(dir, file_list[i])
        if os.path.isfile(file_path):
            M.read_fm_csv(file_path)
    pr.stop()