# Скрипт для фильтрации сырого датасета mpp_dataset.txt, предварительно созданный скриптом loadDB.py.
#
# (c) Sergey Simkovich
from datetime import datetime
import numpy as np
import pandas as pd

DATASET_PATH = '/opt/datasets/mpp_dataset.txt'
FILTERED = '/opt/datasets/mpp_filtered.txt'

mpp = pd.DataFrame()
if __name__ == '__main__':

    # read dataframe from file
    point1 = datetime.now()
    print('Читаем исходник...')
    mpp = pd.read_csv(DATASET_PATH, dtype=str, skipinitialspace=True, delimiter=',')

    mpp.columns = ["num_loco", "num_wheel", "num_operator", "tech", "main_tech", "result_l",
                   "result_r", "date", "time", "freq", "repair_code", "factory", "defect_left", "defect_right"]

    # replace dumb strings
    print('Замена "None"...')
    mpp = mpp.replace("None", np.nan, regex=True)

    # erasing duplicates
    print('Удаление дубликатов...')
    strings_before = len(mpp)
    mpp = mpp.drop_duplicates(subset=None, keep='first', inplace=False)
    strings_after = len(mpp)
    print('... удалено', strings_before - strings_after, "строк.")

    print(mpp['num_operator'].unique())

    # Очистка 'num_operator'



    # mpp['num_operator'].replace(to_replace=r'.*Давы.*', value='Давыдов Д.В.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Реш.*', value='Решетов Д.В.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Нескор.*', value='Нескоромный Р.А.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*чник.*', value='Пасечник Д.В.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Рах.*', value='Рахманина Л.А.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Некрасов.*', value='Некрасов С.С.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*евей.*', value='Шелевей Д.А.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*лонов.*', value='Филонов В.В.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Тын.*', value='Тынянских С.А.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Федо.*', value='Федосеенко А.Н.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Литвин.*', value='Литвин П.А.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Ларин.*', value='Ларин Д.А.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Антонов.*', value='Антонов А.В.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Гераскин.*', value='Гераскин Е.Н.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*щенко.*', value='Лыщенко А.Г.', regex=True, inplace=True)
    # vu90['master1'].replace(to_replace=r'.*Климов.*', value='Климов А.И.', regex=True, inplace=True)
    # vu90['master1'] = vu90['master1'].str.replace(r'\'', '', regex=True)
    #
    # # 'master2' cleaning
    # vu90['master2'].replace(to_replace=r'.*Пып.*', value='Пыпа В.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Антонов.*', value='Антонов А.В.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Г.*', value='Горкунов С.Н.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Зуев.*', value='Зуев К.С.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Иш.*', value='Ишков А.Б.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Ков.*', value='Коваленко С.В.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Лы.*', value='Лыщенко А.Г.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*рицкий.*', value='Мижерицкий А.В.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*тахов.*', value='Мифтахов Р.Г.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Олейник.*', value='Олейник Д.О.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Сакаев.*', value='Сакаев Х.Х.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Сле.*', value='Слезов Д.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*одумов.*', value='Сорокодумов Н.В.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Тыня.*', value='Тынянских С.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Шамы.*', value='Шамыгин А.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Шеле.*', value='Шелевей Д.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Рахма.*', value='Рахманина Л.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Мами.*', value='Мамичев И.Н.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'Лврин Д.А.', value='Ларин Д.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'.*Литвин.*', value='Литвин П.А.', regex=True, inplace=True)
    # vu90['master2'].replace(to_replace=r'^\.$', value=np.nan, regex=True, inplace=True)
    # vu90['master2'] = vu90['master2'].str.replace(r'^\'', '', regex=True)
    #
    # # Killing unnecessary spaces in axles
    # vu90['axle'] = vu90['axle'].str.replace(r' ', '', regex=True)

    # It's all, folks
    # print('До обработки: ', strings_before)
    # print('После обработки: ', strings_after)
    # mpp.to_csv(FILTERED, index=False)
    # print('Выполнено за ', datetime.now() - point1)
