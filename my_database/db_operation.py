from database import MyPostgreSqlDB, DEFAULT_DB_NAME
import psycopg2


class _PgsqlDatabaseCreator:

    def __init__(self, db_name, conn=None):
        self.conn = conn
        self.db_name = db_name

    def create_database(self):
        self._operate_on_datebase("CREATE DATABASE {};", self._already_exists)

    def drop_database(self):
        # self._operate_on_datebase("DROP DATABASE {};", self._not_yet_exist)
        try:
            self.conn.execute_sql_command("DROP DATABASE IF EXISTS {};".format(self.db_name))
        except psycopg2.ProgrammingError as e:
            pass  # 不該這樣寫，Except 應該統一處理掉，在 database.py

    def _operate_on_datebase(self, sql_command, unwanted_condition):
        # 這個寫法很難懂
        # 現在有用 "DROP DATABASE IF EXISTS" 來處理，應該不需要這樣寫了
        # 改掉

        use_new_conn = False

        if not self.conn:
            # self.conn = MyMariaDB(database=None).open()
            self.conn = MyPostgreSqlDB(database='template1').open()

            use_new_conn = True

        if not unwanted_condition():
            self.conn.execute_sql_command(sql_command.format(self.db_name))
        else:
            print('[*] reached unwanted_condition')

        if use_new_conn:
            self.conn.close()

    def _already_exists(self, show_msg=True):
        """
        The database (which should not have exist) exists
        """
        if self.conn.db_already_exists(self.db_name):
            if show_msg:
                print("[-] Database '{}' already exists".format(self.db_name))
            return True
        else:
            return False

    def _not_yet_exist(self):

        if not self._already_exists(show_msg=False):
            print("[*] Database '{}' does not exist".format(self.db_name))
            return True
        else:
            return False


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

        self._run_create(table_name, query)

    def _run_create(self, table_name, query, *args):

        if self.conn.table_already_exists(table_name):
            print("[-] Table '{}' already exists. Skip it.".format(table_name))
            return False
        else:
            query = query.format(table_name)
            self.conn.execute_sql_command(query, *args)
            return True


def initialize(db):
    print("[*] Initializing database '{}' and tables needed...".format(DEFAULT_DB_NAME))
    create_database(DEFAULT_DB_NAME)
    _create_my_tables(db)


def reset(sure=False):
    """
    Drop current database!!!
    Use with caution!!!
    """

    if not sure:
        print("[*] The database '{}' used by this program will be deleted!".format(DEFAULT_DB_NAME))
        answer = input("    Are you sure? [y/n]")
    else:
        answer = ''

    if sure or answer.upper() in ('Y', 'YES'):

        with MyPostgreSqlDB(database='template1') as conn:
            drop_database(DEFAULT_DB_NAME, conn)

        print("[+] Droping database '{}' (if exists).".format(DEFAULT_DB_NAME))
    else:
        print("[-] The database '{}' will not be dropped.".format(DEFAULT_DB_NAME))


def create_database(db_name, conn=None):
    creator = _PgsqlDatabaseCreator(db_name, conn)
    creator.create_database()


def drop_database(db_name, conn=None):
    creator = _PgsqlDatabaseCreator(db_name, conn)
    creator.drop_database()


def _create_my_tables(db):
    """
    create database with name db_name
    Do nothing if it already exists
    """
    with db as conn:
        creator = _PgsqlTableCreator(conn)
        creator.create_all_tables()


if __name__ == '__main__':

    reset(sure=True)

    # db = MyMariaDB()
    db = MyPostgreSqlDB()

    initialize(db)

    reset(sure=True)
