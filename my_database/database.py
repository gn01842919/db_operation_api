import psycopg2

DEFAULT_DB_NAME = "mydb"
_tables = []


class MyDB:
    def __init__(self):
        raise NotImplementedError("Base class 'MyDB' should not be instanciated.")

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def open(self):
        raise NotImplementedError("Derived classes should implement open().")

    def close(self):
        self.cursor.close()
        self._conn.close()
        self.cursor = None
        self._conn = None
        if self.verbose:
            print("[-] The database connection has been closed.")

    def execute_sql_command(self, query, *args):
        raise NotImplementedError("Derived classes should implement execute_sql_command().")

    def db_already_exists(self, db_name):
        # %s 引入之後，似乎會自帶引號!!??   如果 %s 用引號圍繞的話就搜不到結果

        query = (
            "SELECT EXISTS("
            "    SELECT datname FROM pg_catalog.pg_database"
            "    WHERE lower(datname) = lower(%s)"
            ");"
        )

        rows = self.execute_sql_command(query, db_name)
        return rows[0][0]

    def table_already_exists(self, table_name):

        # current_db = self.execute_sql_command('SELECT DATABASE();')[0][0]

        # query = ("SELECT table_name FROM INFORMATION_SCHEMA.TABLES "
        #          "WHERE table_schema = '{}' AND table_name = %s LIMIT 1;"
        #          .format(current_db))

        query = (
            "SELECT EXISTS ("
            "  SELECT 1 FROM pg_catalog.pg_class c"
            "  JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
            "  WHERE  n.nspname = 'public'"
            "  AND    c.relname = %s"
            "  AND    c.relkind = 'r'"
            ");"
        )

        rows = self.execute_sql_command(query, table_name)

        return rows[0][0]


class MyPostgreSqlDB(MyDB):

    def __init__(self, db_host='localhost', db_user='dja1', db_password='_MY_DB_PASSWORD_',
                 db_port=5432, database=DEFAULT_DB_NAME, verbose=True):

        # Note that variable 'database' can be None for MySQL (not using any database)
        # But CANNOT be None for PostgreSQL!
        self.config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'port': db_port,
            'database': database,
        }
        self.verbose = verbose
        self._conn = None
        self.cursor = None

    def open(self):
        if self._conn:
            # nested with blocks are forbidden
            raise RuntimeError('Already connected to database.')
        else:
            self._conn = psycopg2.connect(**self.config)
            self._conn.autocommit = True
            self.cursor = self._conn.cursor()
            if not self.cursor:
                raise RuntimeError('Fail to establish a databae cursor.')

            if self.verbose:
                print("[+] A database connection has been established.")

            return self

    def execute_sql_command(self, query, *args):

        self.cursor.execute(query, args)
        try:
            ret = self.cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            if 'no results to fetch' in str(e):
                ret = None
            else:
                raise

        # self._conn.commit()
        return ret
