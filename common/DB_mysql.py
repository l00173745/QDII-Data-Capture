import pymysql
import pandas as pd


class DB_mysql():
    def __init__(self):
        self.conn = pymysql.connect(host="10.222.48.24", user="MYSQLSupp",password="Duanka_0904",database="world",charset="utf8")
        pass

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

if __name__=='__main__':
    mysql=DB_mysql()
    sql_str='select * from city'
    df = mysql.query(sql_str)
    mysql = DB_mysql()
    tbl_nme = 'world.city'
    data ={
        'ID':5001,
        'NAME':'TonyCity',
        'COUNTRYCODE':'AFG',
        'DISTRICT':'Herat',
        'POPULATION':190000
    }
    mysql.insert(tbl_nme,data)
    pass