import sqlite3
import pandas as pd
import traceback

def get_table_names(path):
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur_obj = cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        all_list = [i[0] for i in cur_obj.fetchall()]
        conn.close()
        
        return all_list
    except Exception as e:
        traceback.print_exc()
        conn.close()
    

def get_data(path,sql_cmd='SELECT * FROM ',table=''):
    try:
        conn = sqlite3.connect(rf"{path}")
        cur = conn.cursor()
        cur_obj = cur.execute(sql_cmd+table)
        all_data = cur_obj.fetchall()
        all_data_columns = [i[0] for i in cur_obj.description]
        conn.close()
        
        return pd.DataFrame(all_data,columns=all_data_columns)
    except Exception as e:
        traceback.print_exc()
        conn.close()
    

def store_data(path,df_data,table_name,index=False):
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()

        df_data.to_sql(table_name,conn,if_exists='append',index=index)
        conn.commit()

        conn.close()
    except Exception as e:
        traceback.print_exc()
        conn.close()