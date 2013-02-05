import collections
import logging
import sqlite3
import os
try:
    import json
except ImportError:
    import simplejson as json
from urlparse import urlsplit

logger = logging.getLogger(__name__)


def parse_db_url(url):
    connection_info = urlsplit(url)
    return dict(engine = connection_info.scheme,
                host=connection_info.hostname,
                port=connection_info.port,
                user=connection_info.username,
                password=connection_info.password,
                database=connection_info.path[1:])


class SQLException(Exception):
    pass


FilenameSha1 = collections.namedtuple('FilenameSha1', 'filename sha1')

class MigrationCommandInfo(object):
    def __init__(self, command, migration_sql, migration_info_sql, filename):
        self.command = command
        self.migration_sql = migration_sql
        self.migration_info_sql = migration_info_sql
        self.applied, self.ghost = False, False
        self.filename = filename

    def __str__(self):
        if self.command:
            return 'command: %s\nmigration info: %s' % (self.command, self.migration_info_sql)
        return 'sql: %s\nmigration info: %s' % (self.migration_sql, self.migration_info_sql)

INSERT_STMT = "INSERT INTO dbmigration (filename, sha1, date) VALUES ('%s', '%s', %s());"

class DatabaseMigrationEngine(object):
    migration_table_sql = (
        "CREATE TABLE dbmigration "
        "(filename varchar(255), sha1 varchar(40), date datetime);")
    ENGINES = {}


    def create_migration_table(self):
        self.execute(self.migration_table_sql)


    def sql(self, directory, filename, sha1_hash):
        command = None
        sql_statement = ''

        if os.path.splitext(filename)[-1] == '.sql':
            with open(os.path.join(directory, filename), 'r') as migration:
                sql_statement = migration.read()
        else:
            command = os.path.join(directory, filename)

        return MigrationCommandInfo(command=command,
                                    migration_sql=sql_statement,
                                    migration_info_sql=INSERT_STMT % (filename, sha1_hash, self.date_func),
                                    filename=filename)

    @property
    def performed_migrations(self):
        return [FilenameSha1(r[0], r[1]) for r in self.results(
            "SELECT filename, sha1 FROM dbmigration ORDER BY filename")]

    @classmethod
    def register(cls):
        DatabaseMigrationEngine.ENGINES[cls.SCHEME] = cls

    @classmethod
    def connect(cls, db_url):
        db_data = parse_db_url(db_url)
        return cls.ENGINES[db_data['engine']](db_data)


class sqlite(DatabaseMigrationEngine):
    """a migration engine for sqlite"""
    date_func = 'datetime'
    SCHEME = 'sqlite'

    def __init__(self, db_data):
        self.connection = sqlite3.connect(db_data['database'])

    def execute(self, statement):
        try:
            return self.connection.executescript(statement)
        except sqlite3.OperationalError as e:
            raise SQLException(str(e))

    def results(self, statement):
        try:
            return self.connection.execute(statement).fetchall()
        except sqlite3.OperationalError as e:
            raise SQLException(str(e))


class GenericEngine(DatabaseMigrationEngine):
    """a generic database engine"""
    date_func = 'now'

    def __init__(self, db_data):
        self.connection = self.engine.connect(db_data)
        self.ProgrammingError = self.engine.ProgrammingError
        self.OperationalError = self.engine.OperationalError

    def execute(self, statement):
        try:
            c = self.connection.cursor()
            c.execute(statement)
            return c
        except (self.ProgrammingError, self.OperationalError) as e:
            self.connection.rollback()
            raise SQLException(str(e))

    def results(self, statement):
        return list(self.execute(statement).fetchall())

class mysql(GenericEngine):
    """a migration engine for mysql"""

    SCHEME = 'mysql'

    def __init__(self, connection_string):
        import MySQLdb
        self.engine = MySQLdb
        super(mysql, self).__init__(connection_string)


class postgresql(GenericEngine):
    """a migration engine for postgres"""

    migration_table_sql = (
        "CREATE TABLE dbmigration "
        "(filename varchar(255), sha1 varchar(40), date timestamp);")

    SCHEME = 'postgresql'

    def __init__(self, connection_string):
        import psycopg2
        self.engine = psycopg2
        super(postgresql, self).__init__(connection_string)


    def execute(self, statement):
        try:
            c = self.connection.cursor()
            c.execute(statement)
            self.connection.commit()
            return c
        except (self.ProgrammingError, self.OperationalError) as e:
            self.connection.rollback()
            raise SQLException(str(e))

for engine in [postgresql,mysql,sqlite]:
    engine.register()
