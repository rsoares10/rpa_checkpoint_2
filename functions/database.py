import sqlite3
import traceback
import sys



def create_table(nome_table):

    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db')
        sqlite_create_table_query_sixmonthsafter = '''CREATE TABLE {} (
                                                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                    TITLE  TEXT,
                                                    GENRE  TEXT,
                                                    RELEASE_DATE DATETIME DEFAULT CURRENT_TIMESTAMP,
                                                    N_ORDERS  INTEGER);'''.format(nome_table)

        cursor = sqliteConnection.cursor()
        print("Conectado com sucesso!")
        cursor.execute(sqlite_create_table_query_sixmonthsafter)
        sqliteConnection.commit()
        print("Tabela SixMonthsAfter criada!")

        cursor.close()

    except sqlite3.Error as error:
        print("Erro durante a criação da tabela", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("Conexão encerrada com sucesso!")






def drop_table(nome_table):
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db')
        cursor = sqliteConnection.cursor()
        print("Conectado com sucesso!")

        sqlite_drop_query = 'DROP TABLE {}'.format(nome_table)

        count = cursor.execute(sqlite_drop_query)
        sqliteConnection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Falha pra dropar tabela")
        print("Execção na classe: ", error.__class__)
        print("Erro: ", error.args)
        exc_type, exc_value, exc_tb = sys.exc_info()
        print(traceback.format_exception(exc_type, exc_value, exc_tb))
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("Conexão encerrada com sucesso!")






def inserir_dados(nome_table, titulo_filme, genero_filme,  data_lancamento):
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db')
        cursor = sqliteConnection.cursor()
        print("Conectado com sucesso!")

        sqlite_insert_query = """INSERT INTO {} (TITLE, GENRE, RELEASE_DATE, N_ORDERS)  VALUES  ('{}', '{}', '{}', 0)""".format(nome_table, titulo_filme, genero_filme, data_lancamento)

        count = cursor.execute(sqlite_insert_query)
        sqliteConnection.commit()
        print("Quantidade linhas adicionadas: ", cursor.rowcount)
        cursor.close()

    except sqlite3.Error as error:
        print("Falha pra inserir os dados")
        print("Execção na classe: ", error.__class__)
        print("Erro: ", error.args)
        exc_type, exc_value, exc_tb = sys.exc_info()
        print(traceback.format_exception(exc_type, exc_value, exc_tb))
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("Conexão encerrada com sucesso!")





def read_table(nome_tabela):
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db', timeout=20)
        cursor = sqliteConnection.cursor()

        sqlite_select_query = 'SELECT * from {}'.format(nome_tabela)

        cursor.execute(sqlite_select_query)
        totalRows = cursor.fetchall()
        sqliteConnection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Falha pra ler os dados")
        print("Execção na classe: ", error.__class__)
        print("Erro: ", error.args)
        exc_type, exc_value, exc_tb = sys.exc_info()
        print(traceback.format_exception(exc_type, exc_value, exc_tb))
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("Conexão encerrada com sucesso!")
        return totalRows

    




#atualiza n_orders caso o filme exista na tabela em questão (utilização na API arquivo main.ipynb)
def update_orders(title):
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db', timeout=20)
        cursor = sqliteConnection.cursor()

        sqlite_select_query_1 = "UPDATE SixMonthsAfter SET N_ORDERS = N_ORDERS + 1 WHERE TITLE = '{}'".format(title)
        sqlite_select_query_2 = "UPDATE SixMonthsBefore SET N_ORDERS = N_ORDERS + 1 WHERE TITLE = '{}'".format(title)
        cursor.execute(sqlite_select_query_1)
        cursor.execute(sqlite_select_query_2)
        sqliteConnection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Falha pra atualizar")
        print("Execção na classe: ", error.__class__)
        print("Erro: ", error.args)
        exc_type, exc_value, exc_tb = sys.exc_info()
        print(traceback.format_exception(exc_type, exc_value, exc_tb))
    finally:
        if (sqliteConnection):
            sqliteConnection.close()
            print("Conexão encerrada com sucesso!")
