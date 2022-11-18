# Скрипт формирует сырой датасета из DB-файлов (формат Paradox)
# Для чтения DB-файлов нужна библиотека 'pypxlib-2.0'. Версия новее работать не будет, базы-исходники старые.
# Выходные данные пишутся в txt-файл.
# Во время работы скрипта появляются предупреждения с текстом "Number of records counted in blocks does not match
# number of records in header". Можно игнорировать, ситуация связана с несколькими повреждёнными базами в их хвостах.
# Обход этого бага сделан в коде,
# (c) Sergey Simkovich

import os
from pypxlib import Table, PXError

DB_PATH = '/opt/datasets/mpp_src'
mpp = []
name_count = 0

if __name__ == '__main__':
    dir_list = [x[0] for x in os.walk(DB_PATH)]  # список каталогов
    dir_list.pop(0)  # лишний элемент, имя родительского каталога
    for i in dir_list:
        dir_name = os.path.join(DB_PATH, i)  # get full filename
        print(dir_name, '...')
        name_count += 1
        for j in os.listdir(i):
            if j.endswith("DB"):
                fullpath = dir_name + '/' + j
                with Table(fullpath, encoding='1251') as wheels:
                    try:
                        for row in wheels:
                            # обход глюка - сбойная пустая строка в Paradox, встречается несколько раз
                            if str(row) == "Row()":
                                break

                            # заполнение воздухом отдельных переменных
                            num_loco = tech = main_tech = freq = repair_code = factory = defect_left = defect_right = ''

                            # заполнение переменных, которые есть во всех трёх видах баз
                            num_wheel = str(row.NumCtrlKolPar).replace(',', '')  # встречаются лишние запятые, убить
                            num_operator = str(row.TabNumDefMan)
                            result_l = '1' if str(row.Result).split(";")[0] == "Л-Годная" else '0'
                            result_r = '1' if str(row.Result).split(";")[1] == "П-Годная" else '0'
                            date = str(row.Date.strftime("%Y-%m-%d"))
                            time = str(row.Time.strftime("%H:%M"))

                            if len(wheels.fields) == 15:
                                # в версии таблицы с 15-ю полями есть все поля, нам нужны 13 (ещё 8):
                                num_loco = str(row.NumLoco)
                                tech = str(row.Tehnolog)
                                main_tech = str(row.MainTehnolog)
                                freq = str(row.Tach)
                                repair_code = str(row.VID_REMONTA)
                                factory = str(row.Factory)
                                defect_left = str(row.DefectLeft).replace(',', '')  # встречаются лишние запятые, убить
                                defect_right = str(row.DefectRight)

                            elif len(wheels.fields) == 10:
                                # в версии с 10 полями немного другой набор, заполняем недостающие 3:
                                num_loco = str(row.NumLoco)
                                tech = str(row.Tehnolog)
                                main_tech = str(row.MainTehnolog)
                            # для версии с 7 полями все данные на этом шаге уже взяты, либо равны ''

                            # финальная сборка списка
                            string = ",".join([num_loco, num_wheel, num_operator, tech, main_tech, result_l,
                                               result_r, date, time, freq, repair_code, factory, defect_left,
                                               defect_right])
                            # добавляем в датафрейм
                            mpp.append(string)
                    except PXError:
                        print("Ошибка библиотеки pypxlib")

    print("Обработано", name_count, "файлов. Запись в файл...")
    with open(DB_PATH + '/mpp_dataset.txt', 'w') as dataset_file:
        rows = 0  # uploaded strings counter
        for i in mpp:
            dataset_file.write(f"{i}\n")
            rows += 1
        print(rows, 'строк')
