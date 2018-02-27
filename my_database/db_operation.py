# Standard libraries
import logging
from database import MyPostgreSqlDB


def create_database(db_name, conn):
    if not conn.db_already_exists(db_name):
        conn.execute_sql_command("CREATE DATABASE {};".format(db_name))
    else:
        logging.info('Database {} already exists.'.format(db_name))


def drop_database_if_exists(db_name, conn):
    conn.execute_sql_command("DROP DATABASE IF EXISTS {};".format(db_name))


def initialize_database(conn, db_name):

    drop_database_if_exists(db_name, conn)

    logging.info("Initializing database '{}' and tables needed...".format(db_name))

    create_database(db_name, conn)
    _create_my_tables(conn)


def _create_my_tables(conn):
    """
    create database with name db_name
    Do nothing if it already exists
    """
    creator = _PgsqlTableCreator(conn)
    creator.create_all_tables()


class _PgsqlTableCreator:

    def __init__(self, conn):
        self.conn = conn

    def create_all_tables(self):
        self._create_main_table('mytable01')

    def _create_main_table(self, table_name):

        query = (
            "CREATE TABLE {}"
            "("
            "   id serial PRIMARY KEY NOT NULL,"
            "   title CHAR(50) NOT NULL,"
            "   url CHAR(50) NOT NULL,"
            "   time INT NOT NULL,"
            "   score REAL NOT NULL DEFAULT '0.00',"
            "   note TEXT"
            ");"
        )

        self._create_table(table_name, query)

    def _create_table(self, table_name, query, *args):

        if self.conn.table_already_exists(table_name):
            logging.info("Table '{}' already exists. Skip it.".format(table_name))
            return False
        else:
            query = query.format(table_name)
            self.conn.execute_sql_command(query, *args)
            return True


if __name__ == '__main__':

    test_db_name = "mydb"

    with MyPostgreSqlDB() as conn:
        initialize_database(conn, test_db_name)

    with MyPostgreSqlDB() as conn:
        drop_database_if_exists(test_db_name, conn)
