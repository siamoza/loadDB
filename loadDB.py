# Скрипт формирует сырой датасета из DB-файлов (формат Paradox)
# Для чтения DB-файлов нужна библиотека 'pypxlib-2.0'. Новее работать не будет, базы старые.
# # Выходные данные пишутся в txt-файл.
# (c) Sergey Simkovich

import sys
import sqlalchemy_paradox
from sqlalchemy import create_engine
from datetime import datetime
import os
from pypxlib import Table

DB_PATH = '/opt/datasets/mpp_src'
# SERVER = 'localhost'
# PORT = 3050
# USERNAME = 'SYSDBA'
# PASSWORD = 'masterkey'
# CHARSET = 'WIN1251'
files_encountered = 0  # files counter
total_records = 0  # rows counter over all DB's
point1 = datetime.now()
mpp = []  # common list of lists, final collection
QUERY = "SELECT * FROM KolParBin"

if __name__ == '__main__':
    dir_list = [x[0] for x in os.walk(DB_PATH)]  # список каталогов
    dir_list.pop(0)  # лишний элемент, имя родительского каталога
    for i in dir_list:
        dir_name = os.path.join(DB_PATH, i)  # get full filename
        for j in os.listdir(i):
            if j.endswith("DB"):
                fullpath = dir_name + '/' + j
                with Table(fullpath, encoding='1251') as wheels:
                    for row in wheels:
                        print(row)
                sys.exit(0)
        #
        #
        # db = create_engine("paradox+pyodbc://@your_dsn", echo=False)
        #
        # con = firebirdsql.connect(
        #     host=SERVER,
        #     port=PORT,
        #     database=filename,
        #     user=USERNAME,
        #     password=PASSWORD,
        #     charset=CHARSET
        #     )
        # files_encountered += 1
        # print('#', files_encountered, ', loading ', i, '...', sep='')
        # cols = con.cursor().execute("SELECT count(*) FROM RDB$RELATION_FIELDS "
        #                             "WHERE RDB$RELATION_NAME='VU90';").fetchall()
        # query = QUERY73 if cols[0][0] == 73 else QUERY97  # select right query
        # res = con.cursor().execute(query+" FROM VU90").fetchall()  # main query
        # con.close()
        # records = 0  # records counter inside one file
        # filename_set = []  # collection for records the file