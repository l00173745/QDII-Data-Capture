import pymysql
import pandas as pd


class DB_mysql():
    def __init__(self):
        self.conn = pymysql.connect(host="10.222.47.71", user="MYSQLSupp",password="Duanka_0904",database="mysqldb",charset="utf8")
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



if __name__=='__main__':
    mysql=DB_mysql()
    sql_str='select * from INDEX_CAPTURE_INFO'
    df = mysql.query(sql_str)
    pass