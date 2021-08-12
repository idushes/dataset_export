# This is a sample Python script.

# Press ⌘R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from conf import DBNAME, PASSWORD, USER, HOST, TABLES
import psycopg2
from tqdm import tqdm
import pandas as pd
# conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)


def get_dataset(connection, table_name, batch_size=10000, limit = 50000):
    cursor = connection.cursor()
    cursor.execute(f"SELECT count(*) FROM {table_name} LIMIT {limit}")
    count = cursor.fetchone()[0]
    offset = 1
    pbar = tqdm(total=count, desc=table_name)
    data = None
    columns = None
    while offset < count:
        data_dict = {}
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}")
        result = cursor.fetchall()
        for row in result:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col.name] = row[i]
            data_dict.append(row_dict)
        df = pd.from_dict(data_dict, orient='index', columns=columns)
        if data is None:
            columns = list(cursor.description)
            data = df
        else:
            data.append(df)
        pbar.update(batch_size)
        offset += batch_size
    pbar.close()
    return data

    # result = cursor.fetchall()
    # results = []
    #
    # pbar = tqdm(total=cursor.rowcount, desc=table_name)
    # while True:
    #     cursor.execute(f"SELECT * FROM {table_name} limit 10000 offset 1")
    #     columns = list(cursor.description)
    #     while True:
    #         row = cursor.fetchone()
    #         if row is None: break
    #         row_dict = {}
    #         for i, col in enumerate(columns):
    #             row_dict[col.name] = row[i]
    #         results.append(row_dict)
    #         pbar.update()
    #     pbar.close()

    # for row in tqdm(result):
    #     row_dict = {}
    #     for i, col in enumerate(columns):
    #         row_dict[col.name] = row[i]
    #     results.append(row_dict)

def test():
    # connect
    connection = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)
    for table_name in TABLES:
        d = get_dataset(connection, table_name)

    # cursor = connection.cursor()
    #
    # # query
    # cursor.execute("SELECT * FROM kids_userevent LIMIT 100000")
    # print(cursor.rowcount)
    # # transform result
    # columns = list(cursor.description)
    # result = cursor.fetchall()
    #
    # # make dict
    # results = []
    # for row in tqdm(result):
    #     row_dict = {}
    #     for i, col in enumerate(columns):
    #         row_dict[col.name] = row[i]
    #     results.append(row_dict)

    # display
    # print(result)

# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
#
#     cursor = conn.cursor()
#     cursor.execute('SELECT * FROM kids_userevent LIMIT 10')
#     colnames = [desc[0] for desc in cursor.description]
#     print(colnames)
#     rec = cursor.fetchone()
#     for row in cursor:
#         res = list(map(type, row))
#         print(str(res))
#
#     from time import sleep
#     from tqdm import tqdm
#     for i in tqdm(range(20)):
#         sleep(0.1)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    test()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
