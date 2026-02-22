import psycopg2
import subprocess

class RDSDatabase:
    def __init__(self, host, port, db, user, password, auth_token_provider=None, logger=None):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.auth_token_provider = auth_token_provider
        self.conn = None
        self.cursor = None
        self.logger = logger

    def log(self, msg):
        if not self.logger is None:
            self.logger.info(msg)

    def warn(self, msg):
        if not self.logger is None:
            self.logger.warning(msg)

    def generate_rds_auth_token(self):
        (cmd, args) = self.auth_token_provider.split(' ', 1)
        result = subprocess.run([self.auth_token_provider], stdout=subprocess.PIPE, shell=True)
        return result.stdout.decode('utf-8')

    def connect(self):
        try:
            if not self.auth_token_provider is None:
                auth_token = self.generate_rds_auth_token()
            else:
                auth_token = self.password

            dsn = "host='%s' port=%d dbname='%s' user='%s' password='%s'" % (self.host, self.port, self.db, self.user, auth_token)
            self.conn = psycopg2.connect(dsn)
            self.cursor = self.conn.cursor()
            self.log('Connected to %s' % self.db)
            return True
        except psycopg2.Error as e:
            raise RuntimeError("Unable to connect to %s : %s\nUsing DSN:\n%s" % (self.db, e, dsn)) from e 

    def check_connection(self):
        if self.conn is None:
            return self.connect()

        retries = 3
        sql_query = 'select 1'
        while retries >= 0:
            try:
                retries -= 1
                self.cursor.execute(sql_query)
                return True
            except psycopg2.Error as e:
                self.warn('connection_test failed :%s' % e)
                if retries >= 0:
                    self.connect()
                else:
                    self.warn('Unable to reconnect to %s' % self.conf.db)
                    return False

    def execute_query(self, sql_query, auto_commit=True):
        try:
            self.cursor.execute(sql_query)
            if auto_commit is True:
                self.conn.commit()
            return True
        except psycopg2.Error as e:
            self.warn('Failed to execute query %s :%s' % (sql_query, e))
            return False

    def get_table_partitions(self, table):
        sql_query = """SELECT
            nmsp_parent.nspname AS parent_schema,
            parent.relname      AS parent,
            nmsp_child.nspname  AS child_schema,
            child.relname       AS child
        FROM pg_inherits
            JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
        WHERE parent.relname='%s';""" % table
        try:
            self.cursor.execute(sql_query)
            partitions = self.cursor.fetchall()
            return partitions
        except psycopg2.Error as e:
            self.warn('Failed to execute query %s :%s' % (sql_query, e))
            return []
    
    def normalize_partition_name(self, table, value):
        partition_name = "%s_%s" % (table, value)
        partition_name = partition_name.replace(' ', '_').lower().strip()
        partition_name = partition_name.replace('-', '_')
        partition_name = partition_name.replace('.', '_')
        partition_name = partition_name.replace('#', '_')
        partition_name = partition_name.replace('$', '_')
        partition_name = partition_name.replace('@', '_')
        partition_name = partition_name.replace('*', '_')
        return partition_name

    def partition_exists(self, table, value, partitions):
        partition_name = self.normalize_partition_name(table, value)
        if not partitions is None and len(partitions) > 0:
            for partition in partitions:
                if partition[3] == partition_name:
                    return True
        return False

    def create_partition(self, table, value):
        partition_name = self.normalize_partition_name(table, value)

        sql_query = "CREATE TABLE %s PARTITION OF %s FOR VALUES IN ('%s');" % (partition_name, table, value)
        if self.execute_query(sql_query) is True:
            return partition_name

        return None

    def start_transaction(self):
        return self.execute_query('BEGIN TRANSACTION;')

    def end_transaction(self):
        return self.execute_query('COMMIT;')

    def rollback(self):
        return self.execute_query('ROLLBACK;')
