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

    kbk = input('Введите КБК на добавление: ')

    w

# Генерация КБК
    # with connection.cursor() as cursor:
    #     kbk = 90111105034040007120
    #     for _ in range(10):
    #         kbk += 1
    #         cursor.execute(
    #             f"""INSERT INTO skbk (id, value) VALUES
    #             (gen_random_uuid(),'{kbk}');"""
    #         )
    #         print(f'[INFO] В базу {db_name} добавлен {kbk}')


except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print('[INFO] PostgreSQL connection closed')
