import mysql.connector


database_name = "mydb"
_tables = []


class MyMariaDB:
    def __init__(self, db_host='localhost', db_user='root',
                 db_password='12345', db_port=3306, database=database_name,
                 verbose=True):
        self.config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'port': db_port,
            'database': database,
            'charset': 'utf8',
            'use_unicode': True,
            'raise_on_warnings': True,
            'get_warnings': True,
        }
        self.verbose = verbose
        self._conn = None
        self.cursor = None

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def open(self):
        if self._conn:
            # nested with blocks are forbidden
            raise RuntimeError('Already connected to database.')
        else:
            self._conn = mysql.connector.connect(**self.config)
            self.cursor = self._conn.cursor()
            if not self.cursor:
                raise RuntimeError('Fail to establish a mysql cursor.')

            if self.verbose:
                print("[+] A database connection has been established.")

            return self

    def close(self):
        self.cursor.close()
        self._conn.close()
        self.cursor = None
        self._conn = None
        if self.verbose:
            print("[-] The database connection has been closed.")

    def execute_sql_command(self, query, *args):
        self.cursor.execute(query, args)
        try:
            ret = self.cursor.fetchall()
        except mysql.connector.errors.InterfaceError as e:
            ret = None

        return ret

    def db_already_exists(self, db_name):
        # %s 引入之後，似乎會自帶引號!!??   如果 %s 用引號圍繞的話就搜不到結果
        query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s LIMIT 1;"
        rows = self.execute_sql_command(query, db_name)
        return bool(rows)

    def table_already_exists(self, table_name):

        current_db = self.execute_sql_command('SELECT DATABASE();')[0][0]

        query = ("SELECT table_name FROM INFORMATION_SCHEMA.TABLES "
                 "WHERE table_schema = '{}' AND table_name = %s LIMIT 1;"
                 .format(current_db))

        rows = self.execute_sql_command(query, table_name)

        return bool(rows)
