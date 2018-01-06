
from database import MyMariaDB, database_name


class _MyDatabaseCreator:

    def __init__(self, db_name, conn=None):
        self.conn = conn
        self.db_name = db_name

    def create_database(self):
        self._operate_on_datebase("CREATE DATABASE {};", self._already_exists)

    def drop_database(self):
        self._operate_on_datebase("DROP DATABASE {};", self._not_yet_exist)

    def _operate_on_datebase(self, sql_command, unwanted_condition):

        use_new_conn = False

        if not self.conn:
            self.conn = MyMariaDB(database=None).open()

            use_new_conn = True

        if not unwanted_condition():
            self.conn.execute_sql_command(sql_command.format(self.db_name))

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


class _MyTableCreator:

    def __init__(self, conn):
        self.conn = conn

    def create_all_tables(self):
        self._create_main_table('mytable01')

    def _create_main_table(self, table_name):

        # 有個多對多的 keyword 對應 ()
        query = ("CREATE TABLE {}"
                 "("
                 "  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                 "  title VARCHAR(50) NOT NULL,"
                 "  url VARCHAR(50) NOT NULL,"
                 "  time INT UNSIGNED NOT NULL,"
                 "  score FLOAT NOT NULL DEFAULT '0.00',"
                 "  note TEXT"
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


def initialize():
    print("[*] Initializing database '{}' and tables needed...".format(database_name))
    create_database(database_name)
    _create_my_tables(MyMariaDB())


def reset(sure=False):
    """
    Drop current database!!!
    Use with caution!!!
    """

    if not sure:
        print("[*] The database '{}' used by this program will be deleted!".format(database_name))
        answer = input("    Are you sure? [y/n]")
    else:
        answer = ''

    if sure or answer.upper() in ('Y', 'YES'):
        drop_database(database_name)
        print("[*] The database '{}' is dropped.".format(database_name))
    else:
        print("[*] The database '{}' will not be dropped.".format(database_name))


def create_database(db_name, conn=None):
    creator = _MyDatabaseCreator(db_name, conn)
    creator.create_database()


def drop_database(db_name, conn=None):
    creator = _MyDatabaseCreator(db_name, conn)
    creator.drop_database()


def _create_my_tables(db=None):
    """
    create database with name db_name
    Do nothing if it already exists
    """
    if not db:
        db = MyMariaDB()

    with db as conn:
        creator = _MyTableCreator(conn)
        creator.create_all_tables()


if __name__ == '__main__':

    reset()

    initialize()

    reset(sure=True)
