import psycopg2


def drop_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE phones;
            DROP TABLE client;
        """)
    return


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(50) NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            client_id INTEGER REFERENCES client(id), 
            number VARCHAR(50) NOT NULL,
            CONSTRAINT pk_phones PRIMARY KEY (client_id, number)
        );
        """)
        conn.commit()
    return


def add_client(conn, first_name, last_name, email, phones=None, client_id=None):
    with conn.cursor() as cur:
        
        if client_id == None:
            cur.execute("""
            INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s)
            RETURNING id;
            """, (first_name, last_name, email))
        else:
            cur.execute("""
            INSERT INTO client(id, first_name, last_name, email) VALUES(%s, %s, %s, %s) 
            ON CONFLICT (id) DO UPDATE SET first_name = excluded.first_name, last_name = excluded.last_name, email = excluded.email
            RETURNING id;
            """, (client_id, first_name, last_name, email))
        cur_client_id = cur.fetchone()

        if phones != None:
            for phone in phones:
                cur.execute("""
                INSERT INTO phones(client_id, number) VALUES(%s, %s)
                ON CONFLICT (client_id, number) DO NOTHING;
                """, (cur_client_id, phone))

        conn.commit()

    return


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phones(client_id, number) VALUES(%s, %s)
        ON CONFLICT (client_id, number) DO NOTHING;
        """, (client_id, phone))
        conn.commit()

    return


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    pass


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones WHERE client_id = %s AND number = %s;
        """, (client_id, phone))
        conn.commit()

    return


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones WHERE client_id = %s;
        """, (client_id,))
        cur.execute("""
        DELETE FROM client WHERE client_id = %s;
        """, (client_id,))
        conn.commit()
    
    return


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    

    return


if __name__ == '__main__':

    with psycopg2.connect(database="task_05_psycopg_db", user="postgres", host="localhost") as conn:
        
        # Функция, создающая структуру БД (таблицы)
        drop_db(conn) 
        create_db(conn)
        
        # Функция, позволяющая добавить нового клиента
        add_client(conn, client_id=1, first_name='Petr', last_name='Ivanov', email='ivanovp@mail.ru', phones=['+79261234567'])
        add_client(conn, client_id=2, first_name='Alexey', last_name='Sergeev', email='sergeeva@mail.ru', phones=['+79261112233', '+79261112234', '+79261112235'])
        add_client(conn, client_id=3, first_name='Alexey', last_name='Pavlov', email='pavlova@mail.ru')
        add_client(conn, client_id=4, first_name='Sergey', last_name='Smirnov', email='smirnovs@mail.ru', phones=['+79997777777'])

        # Функция, позволяющая добавить телефон для существующего клиента
        add_phone(conn, client_id=3, phone='+79265555555')

        # Функция, позволяющая изменить данные о клиенте
        change_client(conn, client_id=1, last_name='Sidorov', email='sidorovp@mail.ru', phones=['+79260000001'])

        # Функция, позволяющая удалить телефон для существующего клиента
        delete_phone(conn, client_id=2, phone='+79261112235')

        # Функция, позволяющая удалить существующего клиента
        delete_client(conn, client_id=4)

        # Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
        find_client(conn, first_name='Alexey')
        find_client(conn, last_name='Sergeev', phone='+79261112234')

        print('End')        

    conn.close()
