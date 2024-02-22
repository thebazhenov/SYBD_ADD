# Перенос запросов в функции

import psycopg2
from config import host, user, password, db_name, port

try:
    # connect
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name,
        port=port
    )

    connection.autocommit = True

    date_list_start = ['2022-01-01', '2023-01-01', '2024-01-01']
    date_list_end = ['2022-12-31', '2023-12-31', '2024-12-31']
    digit_raspr = [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00]
    kbk_user_list = []

    # def add_kbk_rasp(list_start, list_end, kbk):
    #   for date_start, date_end in zip(date_list_start, date_list_end):
    #      cursor.execute(
    #         f"""INSERT INTO public.skbk_raspr (kbk_raspr_id, date_start, date_end, percent_1,
    #        percent_2, percent_3, percent_4, percent_5, percent_6, percent_7, percent_8,
    #       percent_9, update_time, sync_time, is_sync, kbk_id) VALUES(gen_random_uuid(),
    #      {date_start}, {date_end}, {digit_raspr[0]}, {digit_raspr[1]}, {digit_raspr[2]}, {digit_raspr[3]},
    #     {digit_raspr[4]}, {digit_raspr[5]},
    #    {digit_raspr[6]}, {digit_raspr[7]}, {digit_raspr[8]}, current_timestamp, NULL,
    #   false, {uuid_kbk});"""
    # )

    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT value from skbk;"""
        )

        kbk_list = [int(row[0]) for row in cursor.fetchall()]

    while True:
        kbk_user = int(input("""Введите КБК на добавление: """))

        if kbk_user == 0:
            break
        else:
            kbk_user_list.append(kbk_user)

    # 20
    for i_kbk in kbk_user_list:
            # Нужно добавить откат изменений в случае фейла??
            # Рефактор на название доп переменной
        if len(str(i_kbk)) == 20:

            print("Введите распределение КБК, где:\n"
                  "        0 - Федеральный\n"
                  "        1 - Краевой\n"
                  "        2 - Муниципальный район\n"
                  "        3 - Городской округ\n"
                  "        4 - Городское поселение\n"
                  "        5 - Сельское поселение\n"
                  "        6 - Уровень бюджета 7\n"
                  "        7 - Уровень бюджета 8\n"
                  "        8 - Уровень бюджета 9")

            kbk_raspr = int(input())

            if kbk_raspr in range(0, 8):

                kbk_deb = input('Введите дебетовый субсчет оплаты: ')
                kbk_cred = input('Введите кредитовый субсчет оплаты: ')

                digit_raspr.insert(kbk_raspr, 100.00)

                if i_kbk in kbk_list:
                    print(f'КБК {i_kbk} найден в таблице SKBK')

                    # Получаем uuid кбк по его значению. В дальнейшем будем юзать

                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"""SELECT kbk_id FROM skbk
                                WHERE value = '{i_kbk}';"""
                        )

                        uuid_kbk_tuple = cursor.fetchone()

                    uuid_kbk = uuid_kbk_tuple[0]

                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"""SELECT kbk_id FROM skbk_raspr 
                                WHERE kbk_id = '{uuid_kbk}';"""
                        )

                        find_kbk = cursor.fetchone()
                        # Выполняется , если запрос вернул Null, т.е. такого кбк нет в распределение
                        if find_kbk is None:
                            with connection.cursor() as cursor:
                                for date_start, date_end in zip(date_list_start, date_list_end):
                                    cursor.execute(
                                        f"""INSERT INTO public.skbk_raspr (kbk_raspr_id, date_start, date_end, percent_1, 
                                        percent_2, percent_3, percent_4, percent_5, percent_6, percent_7, percent_8, 
                                        percent_9, update_time, sync_time, is_sync, kbk_id) VALUES(gen_random_uuid(), 
                                        '{date_start}', '{date_end}', {digit_raspr[0]}, {digit_raspr[1]}, {digit_raspr[2]}, {digit_raspr[3]},
                                        {digit_raspr[4]}, {digit_raspr[5]},
                                        {digit_raspr[6]}, {digit_raspr[7]}, {digit_raspr[8]}, current_timestamp, NULL, 
                                        false, '{uuid_kbk}');"""
                                    )

                                print(f'Добавлено распределение кбк с {date_list_start[0]} по {date_list_end[-1]}')

                            with connection.cursor() as cursor:
                                cursor.execute(
                                    f"""select budget_operation_type_id  from sbudget_operation_type 
                                    where kbk = '{i_kbk}'"""
                                )

                                operation_type = cursor.fetchone()
                                if operation_type:
                                    print(f'В журнале найдена операция оплаты с id {operation_type[0]}')

                            if operation_type is None:
                                with connection.cursor() as cursor:
                                    cursor.execute(
                                        f"""INSERT INTO public.sbudget_operation_type (budget_operation_type_id, kbk, update_time, is_sync, sync_time, debet_account_id, credit_account_id, journal_number, descr, "type")
                                        VALUES(gen_random_uuid(), '{i_kbk}', current_timestamp, false, NULL, '{kbk_deb}', '{kbk_cred}', '0', 'СУФД.Невыясненные', 0);"""
                                    )
                                    print(f'В журнал типовых операций была добавлена оплата для КБК {i_kbk}')



                        else:
                            print('В skbk_raspr найдены записи')
                            # Получаем результа о последней дате
                            with connection.cursor() as cursor:
                                cursor.execute(
                                    f"""SELECT date_start FROM skbk_raspr 
                                        WHERE kbk_id = '{uuid_kbk}'
                                        order by date_start desc;"""
                                )

                                date_end_tuple = cursor.fetchall()
                                # date_end = date_end_tuple[0]
                                print('\nДаты за которые уже существуют записи в SKBK_RASPR:')
                                for j in range(len(date_end_tuple)):
                                    print(date_end_tuple[j][0])
                                print()

                            with connection.cursor() as cursor:
                                cursor.execute(
                                    f"""select budget_operation_type_id  from sbudget_operation_type 
                                    where kbk = '{i_kbk}'"""
                                )

                                operation_type = cursor.fetchone()
                                if operation_type:
                                    print(f'В журнале найдена операция оплаты с id {operation_type[0]}')

                                if operation_type is None:
                                    with connection.cursor() as cursor:
                                        cursor.execute(
                                            f"""INSERT INTO public.sbudget_operation_type (budget_operation_type_id, kbk, update_time, is_sync, sync_time, debet_account_id, credit_account_id, journal_number, descr, "type")
                                        VALUES(gen_random_uuid(), '{i_kbk}', current_timestamp, false, NULL, '{kbk_deb}', '{kbk_cred}', '0', 'СУФД.Невыясненные', 0);"""
                                    )
                                    print(f'В журнал типовых операций была добавлена оплата для КБК {i_kbk}')

                else:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"""INSERT INTO public.skbk
                            (descr, value, kbk_id, update_time, is_sync, sync_time, sync_pays_to_mo, oktmo_to_sync, date_budget_close)
                            VALUES('CУФД.Невыясненные', '{i_kbk}', gen_random_uuid(), current_timestamp, false, NULL, false, '', NULL);
                            """
                        )
                        print(f'{i_kbk} успешно добавлен в SKBK')
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"""SELECT kbk_id FROM skbk
                                WHERE value = '{i_kbk}';"""
                        )

                        uuid_kbk_tuple = cursor.fetchone()

                    uuid_kbk = uuid_kbk_tuple[0]

                    with connection.cursor() as cursor:
                        for date_start, date_end in zip(date_list_start, date_list_end):
                            cursor.execute(
                                f"""INSERT INTO public.skbk_raspr (kbk_raspr_id, date_start, date_end, percent_1, 
                                        percent_2, percent_3, percent_4, percent_5, percent_6, percent_7, percent_8, 
                                        percent_9, update_time, sync_time, is_sync, kbk_id) VALUES(gen_random_uuid(), 
                                        '{date_start}', '{date_end}', {digit_raspr[0]}, {digit_raspr[1]}, {digit_raspr[2]}, {digit_raspr[3]},
                                        {digit_raspr[4]}, {digit_raspr[5]},
                                        {digit_raspr[6]}, {digit_raspr[7]}, {digit_raspr[8]}, current_timestamp, NULL, 
                                        false, '{uuid_kbk}');"""
                            )
                        print(f'Добавлено распределение кбк с {date_list_start[0]} по {date_list_end[-1]}')
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"""INSERT INTO public.sbudget_operation_type (budget_operation_type_id, kbk, update_time, is_sync, sync_time, debet_account_id, credit_account_id, journal_number, descr, "type")
                            VALUES(gen_random_uuid(), '{i_kbk}', current_timestamp, false, NULL, '{kbk_deb}', '{kbk_cred}', '0', 'СУФД.Невыясненные', 0);"""
                            )
                        print(f'В журнал типовых операций была добавлена оплата для КБК {i_kbk}')
            else:
                print('Введено неккоректное значение')
        else:
            print('Вы ввели какую-то хуйню, которая не равна 20 символам кбк')

except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print('[INFO] PostgreSQL connection closed')
