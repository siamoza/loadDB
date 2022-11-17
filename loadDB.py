# Скрипт формирует сырой датасета из DB-файлов (формат Paradox)
# Для чтения DB-файлов нужна библиотека 'pypxlib-2.0'. Версия новее работать не будет, базы старые.
# # Выходные данные пишутся в txt-файл.
# (c) Sergey Simkovich

from datetime import datetime
import os

from pypxlib import Table, PXError

DB_PATH = '/opt/datasets/mpp_src'
files_encountered = 0  # files counter
total_records = 0  # rows counter over all DB's
point1 = datetime.now()
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
                # if dir_name != "/opt/datasets/mpp_src/10240":
                #     continue
                with Table(fullpath, encoding='1251') as wheels:
                    try:
                        for row in wheels:
                            # обход глюка - сбойная пустая строка в Paradox
                            if str(row) == "Row()":
                                break

                            # заполнение воздухом 13 переменных, перезапишутся не все из них
                            num_loco = num_wheel = num_operator = tech = main_tech = result = date = time = \
                                freq = repair_code = factory = defect_left = defect_right = ''

                            if len(wheels.fields) == 15:
                                # в версии таблицы с 15-ю полями есть все поля, нам нужны 13:
                                num_loco = str(row.NumLoco)
                                num_wheel = str(row.NumCtrlKolPar)
                                num_operator = str(row.TabNumDefMan)
                                tech = str(row.Tehnolog)
                                main_tech = str(row.MainTehnolog)
                                result_l = '1' if str(row.Result).split(";")[0] == "Л-Годная" else '0'
                                result_r = '1' if str(row.Result).split(";")[1] == "П-Годная" else '0'
                                date = str(row.Date.strftime("%Y-%m-%d"))
                                time = str(row.Time.strftime("%H:%M"))
                                freq = str(row.Tach)
                                repair_code = str(row.VID_REMONTA)
                                factory = str(row.Factory)
                                defect_left = str(row.DefectLeft)
                                defect_right = str(row.DefectRight)

                            elif len(wheels.fields) == 10:
                                # В версии таблицы с 10-ю полями перезапишем 8 полей "nan" в реальные,
                                # нельзя обращаться к полям, которых нет.
                                num_loco = str(row.NumLoco)
                                num_wheel = str(row.NumCtrlKolPar)
                                num_operator = str(row.TabNumDefMan)
                                tech = str(row.Tehnolog)
                                main_tech = str(row.MainTehnolog)
                                result_l = '1' if str(row.Result).split(";")[0] == "Л-Годная" else '0'
                                result_r = '1' if str(row.Result).split(";")[1] == "П-Годная" else '0'
                                date = str(row.Date.strftime("%Y-%m-%d"))
                                time = str(row.Time.strftime("%H:%M"))

                            else:
                                # В версии таблицы с 7-ю полями перезапишем 5 нужных полей из "nan" в реальные:
                                num_wheel = str(row.NumCtrlKolPar)
                                num_operator = str(row.TabNumDefMan)
                                result_l = '1' if str(row.Result).split(";")[0] == "Л-Годная" else '0'
                                result_r = '1' if str(row.Result).split(";")[1] == "П-Годная" else '0'
                                date = str(row.Date.strftime("%Y-%m-%d"))
                                time = str(row.Time.strftime("%H:%M"))
                            # финальная сборка списка
                            string = ",".join([num_loco, num_wheel, num_operator, tech, main_tech, result_l,
                                               result_r, date, time, freq, repair_code, factory, defect_left,
                                               defect_right])
                            mpp.append(string)
                    except PXError:
                        print("Ошибка библиотеки")

    print('Запись в файл...')
    with open(DB_PATH + '/mpp_dataset.txt', 'w') as dataset_file:
        rows = 0  # uploaded strings counter
        for i in mpp:
            dataset_file.write(f"{i}\n")
            rows += 1
        print(rows, 'rows')
