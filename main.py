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

    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT value from skbk;"""
        )

        kbk_list = [int(row[0]) for row in cursor.fetchall()]

    kbk_user = int(input('Введите КБК на добавление: '))
    # 20
    if len(str(kbk_user)) == 20:

        kbk_raspr = int(input('\nВведите распределение кбк:'
                          'Где муниципальный район(0)'
                          'Город(1)'))

        if kbk_raspr in [0, 1]:

            kbk_deb = input('Введите дебетовый субсчет оплаты: ')
            kbk_cred = input('Введите кредитовый субсчет оплаты: ')

            digit_raspr.insert(kbk_raspr, 100.00)

            if kbk_user in kbk_list:
                print(f'КБК {kbk_user} найден в таблице SKBK')

                # Получаем uuid кбк по его значению. В дальнейшем будем юзать

                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""SELECT id FROM skbk
                            WHERE value = '{kbk_user}';"""
                    )

                    uuid_kbk_tuple = cursor.fetchone()

                uuid_kbk = uuid_kbk_tuple[0]

                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""SELECT id_kbk FROM skbk_raspr 
                            WHERE id_kbk = '{uuid_kbk}';"""
                    )

                    find_kbk = cursor.fetchone()
                    # Выполняется , если запрос вернул Null, т.е. такого кбк нет в распределение
                    if find_kbk is None:
                        with connection.cursor() as cursor:
                            for date_start, date_end in zip(date_list_start, date_list_end):
                                cursor.execute(
                                    f"""INSERT INTO skbk_raspr (id_kbk, raspr, date_start, date_end, rasrp2) VALUES
                                        ('{uuid_kbk}', {digit_raspr[0]},'{date_start}','{date_end}',{digit_raspr[1]});"""
                                )
                            print(f'Добавлено распределение кбк с {date_list_start[0]} по {date_list_end[-1]}')

                        with connection.cursor() as cursor:
                            cursor.execute(f"""INSERT INTO sbudget_operation_type(id, id_kbk, deb, cred, type) VALUES
                            (gen_random_uuid(), '{uuid_kbk}', '{kbk_deb}', '{kbk_cred}', 1);""")
                            print(f'В журнал типовых операций была добавлена оплата для КБК {kbk_user}')

                    else:
                        print('В skbk_raspr найдены записи')
                        # Получаем результа о последней дате
                        with connection.cursor() as cursor:
                            cursor.execute(
                                f"""SELECT date_start FROM skbk_raspr 
                                    WHERE id_kbk = '{uuid_kbk}'
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
                                f"""SELECT id FROM sbudget_operation_type 
                                    WHERE id_kbk = '{uuid_kbk}' and type = 1;"""
                            )

                            operation_type = cursor.fetchone()
                            print(f'В журнале найдена операция оплаты с id {operation_type[0]}')

                            if operation_type is None:
                                with connection.cursor() as cursor:
                                    cursor.execute(f"""INSERT INTO sbudget_operation_type(id, id_kbk, deb, cred, type) VALUES
                                (gen_random_uuid(), '{uuid_kbk}', '{kbk_deb}', '{kbk_cred}', 1);""")
                                print(f'В журнал типовых операций была добавлена оплата для КБК {kbk_user}')

            else:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""INSERT INTO skbk (id, value) VALUES
                        (gen_random_uuid(),'{kbk_user}');
                        """
                    )
                    print(f'{kbk_user} успешно добавлен в SKBK')
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""SELECT id FROM skbk
                            WHERE value = '{kbk_user}';"""
                    )

                    uuid_kbk_tuple = cursor.fetchone()

                uuid_kbk = uuid_kbk_tuple[0]

                with connection.cursor() as cursor:
                    for date_start, date_end in zip(date_list_start, date_list_end):
                        cursor.execute(
                            f"""INSERT INTO skbk_raspr (id_kbk, raspr, date_start, date_end, rasrp2) VALUES
                                ('{uuid_kbk}', {digit_raspr[0]},'{date_start}','{date_end}',{digit_raspr[1]});"""
                        )
                    print(f'Добавлено распределение кбк с {date_list_start[0]} по {date_list_end[-1]}')
                with connection.cursor() as cursor:
                    cursor.execute(f"""INSERT INTO sbudget_operation_type(id, id_kbk, deb, cred, type) VALUES
                    (gen_random_uuid(), '{uuid_kbk}', '{kbk_deb}', '{kbk_cred}', 1);""")
                    print(f'В журнал типовых операций была добавлена оплата для КБК {kbk_user}')
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
