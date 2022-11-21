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

    # Очистка 'num_operator'
    # Цифры вместо фамилий
    mpp['num_operator'].replace(to_replace=r"[0-9]+", value="", regex=True, inplace=True)
    # Левые символы
    mpp['num_operator'].replace(to_replace=r"\\+", value="", regex=True, inplace=True)
    # nan не нужен
    mpp['num_operator'].replace(to_replace=np.nan, value="", regex=True, inplace=True)

    mpp['num_operator'].replace(to_replace=r'Агарков.*', value='Агарков С.Ю.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Анненков.*', value='Анненков Н.В.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Бажинов.*', value='Бажинов С.С.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Бурцев.*', value='Бурцев Ю.Г.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Годовников.*', value='Годовников А.С.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Давыдов.*', value='Давыдов Д.В.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Дриманов.*', value='Дриманов П.В.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'[Кк]равец.*', value='', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'.*сюков.*', value='Красюков А.Г.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Куд.*', value='Кудашов Е.В.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Мишин.*', value='Мишин С.А.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Нескор.*', value='Нескоромный Р.А.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Рахманина.*', value='Рахманина Л.А.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'[Сс]веташова.*', value='', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Тарасов.*', value='', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Тынянских.*', value='Тынянских С.А.', regex=True, inplace=True)
    mpp['num_operator'].replace(to_replace=r'Чемоданов.*', value='Чемоданов В.Г.', regex=True, inplace=True)

    # It's all, folks
    print('До обработки: ', strings_before)
    print('После обработки: ', strings_after)
    mpp.to_csv(FILTERED, index=False)
    print('Выполнено за ', datetime.now() - point1)
