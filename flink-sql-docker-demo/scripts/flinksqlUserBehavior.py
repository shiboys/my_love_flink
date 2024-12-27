import pandas as pd
import json
import sys
from datetime import datetime
import time
from pandas.core.frame import DataFrame


def main():
    # 第一个参数
    file_path = sys.argv[1]
    headN = True
    exportSql = False
    if (len(sys.argv) > 2):
        headN = strToBool(sys.argv[2])
    if (len(sys.argv) > 3):
        exportSql = strToBool(sys.argv[3])
    # python 的 DataFrame
    #print("headN is " , str(headN))
    df = pd.read_csv(file_path, header=None)
    max = 10
    i = 0
    # lines = len(df)
   # print("begin to print,total lines :" , str(lines))
   # print("exportSql is ", exportSql)
    if (exportSql == True):
        exportMysql(df, headN, max)
        return

    for row in df.itertuples():
        # datetime.strptime str parse time, datetime.strftime datetime's format
        # 由于第 5 列就是 timestamp ，因此用不上 string datetime 到 timestamp 的转换了
        # int(datetime.strptime(row[6],"%Y/%m/%d %H:%M").timestamp()*1000)
        if (row[1] == None or row[1] <= 0):
            continue

        userBehavior = {
            "user_id": row[1],
            "item_id": row[2],
            "category_id": row[3],
            "behavior": str(row[4]),
            "event_ts": int(row[5])
        }
        print(json.dumps(userBehavior))
        #休眠 1ms 实现近似 1000 records / second 的速度
        time.sleep(0.001)
        i = i+1
        if (headN == True and i >= max):
            break


def exportMysql(df: DataFrame, headN: bool, max: int):
    i = 0
    sql = "insert into category(sub_category_id,parent_category_id) select {cat_id}, {cat_pid} on duplicate key update create_time = now();"
    for row in df.itertuples():
        print(sql.format(cat_id=row[3], cat_pid=row[3] % 10))
        i = i+1
        if (headN == True and i >= max):
            break


def strToBool(str):
    return True if str.lower() == 'true' else False


if __name__ == "__main__":
    main()
