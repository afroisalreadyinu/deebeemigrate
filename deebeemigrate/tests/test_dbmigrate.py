from deebeemigrate.core import (
    DBMigrate, OutOfOrderException, ModifiedMigrationException
)
from deebeemigrate.dbengines import parse_db_url
import subprocess
import os

import unittest

# TODO:
# - Add test for filling migrations with migrations if migrations
# table is created (because you are creating the db from schema and
# don't need migrations anyway)

class FakeFile(object):

    def __call__(self, filename, options):
        self.filename = filename
        self.options = options
        return self

    def write(self, contents):
        self.contents = contents


class TestConnectionUrlParser(unittest.TestCase):

    def test_postgres_url_parts(self):
        url = 'postgresql://usernm:passwd@hosthost:12345/dbname'
        parts = parse_db_url(url)
        self.assertEqual(parts,
                         dict(engine='postgresql',
                              host='hosthost',
                              port=12345,
                              user='usernm',
                              password='passwd',
                              database='dbname'))


    def test_sqlite_memory(self):
        url = 'sqlite:///:memory:'
        parts = parse_db_url(url)
        self.assertEqual(parts,
                         dict(engine='sqlite',
                              host=None,
                              port=None,
                              user=None,
                              password=None,
                              database=':memory:'))

class TestDBMigrate(unittest.TestCase):

    def setUp(self):
        connection_string = 'sqlite:///:memory:'
        db_data = parse_db_url(connection_string)
        self.settings = {
            'out_of_order': False,
            'dry_run': False,
            'connection_string': connection_string,
            'run_for_new_db': True
        }
        if db_data['engine'] == 'mysql':
            import MySQLdb
            # create the test database
            db = connection_settings.pop('db')
            c = MySQLdb.connect(**connection_settings)
            c.cursor().execute('DROP DATABASE IF EXISTS %s' % db)
            c.cursor().execute('CREATE DATABASE %s' % db)
        if db_data['engine'] == 'postgres':
            import psycopg2
            # create the test database
            database = connection_settings['database']
            schema = connection_settings.pop('schema', None)

            if schema is None:
                c = psycopg2.connect(database='template1')
                c.set_isolation_level(0)
                cur = c.cursor()
                cur.execute('DROP DATABASE IF EXISTS %s' % database)
                cur.execute('CREATE DATABASE %s' % database)

            else:
                c = psycopg2.connect(**connection_settings)
                c.cursor().execute('DROP SCHEMA IF EXISTS %s CASCADE' % schema)
                c.cursor().execute('CREATE SCHEMA %s' % schema)
                c.commit()

    def test_create(self):
        self.settings['directory'] = '/tmp'
        dbmigrate = DBMigrate(**self.settings)
        fake_file = FakeFile()
        dbmigrate.create('test slug', 'sql', fake_file)
        self.assert_(fake_file.filename.startswith('/tmp'))
        self.assert_(fake_file.filename.endswith('test-slug.sql'))
        self.assertEqual(fake_file.contents, '-- add your migration here')


    def test_current_migrations(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'initial')
        self.settings['directory'] = fixtures_path
        dbmigrate = DBMigrate(**self.settings)
        self.assertEqual(
            dbmigrate.current_migrations(), [(
                '20120115075349-create-user-table.sql',
                '0187aa5e13e268fc621c894a7ac4345579cf50b7'
            )])

    def test_dry_run_migration(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'initial')
        self.settings['directory'] = fixtures_path
        self.settings['dry_run'] = True
        dbmigrate = DBMigrate(**self.settings)
        self.assertEqual(dbmigrate.migrate(), (
"""sql: -- intentionally making this imperfect so it can be migrated
CREATE TABLE users (
  id int PRIMARY KEY,
  name varchar(255),
  password_sha1 varchar(40)
);
migration info: INSERT INTO dbmigration (filename, sha1, date) VALUES ('20120115075349-create-user-table.sql', '0187aa5e13e268fc621c894a7ac4345579cf50b7', %s());""" % dbmigrate.engine.date_func))

    def test_multiple_migration_dry_run(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'second-run')
        self.settings['directory'] = fixtures_path
        self.settings['dry_run'] = True
        dbmigrate = DBMigrate(**self.settings)

        self.assertEqual(dbmigrate.migrate(), (
"""sql: -- intentionally making this imperfect so it can be migrated
CREATE TABLE users (
  id int PRIMARY KEY,
  name varchar(255),
  password_sha1 varchar(40)
);
migration info: INSERT INTO dbmigration (filename, sha1, date) VALUES ('20120115075349-create-user-table.sql', '0187aa5e13e268fc621c894a7ac4345579cf50b7', %(date_func)s());
sql: ALTER TABLE users ADD COLUMN email varchar(70);
migration info: INSERT INTO dbmigration (filename, sha1, date) VALUES ('20120603133552-awesome.sql', '6759512e1e29b60a82b4a5587c5ea18e06b7d381', %(date_func)s());""" % {'date_func': dbmigrate.engine.date_func}))

    def test_initial_migration(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'initial')
        self.settings['directory'] = fixtures_path
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        # since the database is in memory we need to reach in to get it
        self.assertEqual(
            dbmigrate.engine.performed_migrations, [(
                '20120115075349-create-user-table.sql',
                '0187aa5e13e268fc621c894a7ac4345579cf50b7'
            )])


    def test_initial_migration_without_run_all(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'initial')
        self.settings['directory'] = fixtures_path
        self.settings['run_for_new_db'] = False
        dbmigrate = DBMigrate(**self.settings)
        migrated = dbmigrate.migrate()

        self.assertEqual(migrated, 'Simulated 1 migrations:\n20120115075349-create-user-table.sql')
        #self.assertEqual(dbmigrate.engine.performed_migrations,


    def test_out_of_order_migration(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'out-of-order-1')
        self.settings['directory'] = fixtures_path
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        dbmigrate.directory = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'out-of-order-2')
        try:
            dbmigrate.migrate()
            self.fail('Expected an OutOfOrder exception')
        except OutOfOrderException as e:
            self.assertEqual(
                str(e),
                ('[20120114221757-before-initial.sql] '
                 'older than the latest performed migration'))

    def test_allowed_out_of_order_migration(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'out-of-order-1')
        self.settings['directory'] = fixtures_path
        self.settings['out_of_order'] = True
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        dbmigrate.directory = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'out-of-order-2')
        dbmigrate.migrate()
        self.assertEqual(
            dbmigrate.engine.performed_migrations,
            [('20120114221757-before-initial.sql',
              'c7fc17564f24f7b960e9ef3f6f9130203cc87dc9'),
             ('20120115221757-initial.sql',
              '841ea60d649264965a3e8c8a955fd7aad54dad3e')])

    def test_modified_migrations_detected(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'modified-1')
        self.settings['directory'] = fixtures_path
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        dbmigrate.directory = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'modified-2')
        try:
            dbmigrate.migrate()
            self.fail('Expected a ModifiedMigrationException')
        except ModifiedMigrationException as e:
            self.assertEqual(
                str(e),
                ('[20120115221757-initial.sql] migrations were '
                 'modified since they were run on this database.'))

    def test_deleted_migrations_detected(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'deleted-1')
        self.settings['directory'] = fixtures_path
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        dbmigrate.directory = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'deleted-2')
        try:
            dbmigrate.migrate()
            self.fail('Expected a ModifiedMigrationException')
        except ModifiedMigrationException as e:
            self.assertEqual(
                str(e),
                ('[20120115221757-initial.sql] migrations were '
                 'deleted since they were run on this database.'))

    def test_multiple_migrations(self):
        self.settings['directory'] = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'initial')
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        dbmigrate.directory = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'second-run')
        dbmigrate.migrate()
        self.assertEqual(
            dbmigrate.engine.performed_migrations,
            [('20120115075349-create-user-table.sql',
              '0187aa5e13e268fc621c894a7ac4345579cf50b7'),
             ('20120603133552-awesome.sql',
              '6759512e1e29b60a82b4a5587c5ea18e06b7d381')])

    def test_null_migration_after_successful_migration(self):
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'initial')
        self.settings['directory'] = fixtures_path
        self.settings['out_of_order'] = False
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        dbmigrate.migrate()

    def test_null_dry_run_migration(self):
        self.settings['directory'] = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'second-run')
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        self.settings['dry_run'] = True
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()

    def test_passing_script_migration(self):
        self.settings['directory'] = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'arbitrary-scripts')
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        self.assertEqual(
            dbmigrate.engine.performed_migrations,
            [('20121019152404-initial.sql',
              '4485430c4b18fdbe273a845e654c66ada42d3066'),
             ('20121019152409-script.sh',
              '837a6ab019646fae8488048e20ff2651437b2fbd'),
             ('20121019152412-final.sql',
              '4485430c4b18fdbe273a845e654c66ada42d3066')])

    def test_failing_script_migration(self):
        self.settings['directory'] = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'arbitrary-scripts-failing')
        dbmigrate = DBMigrate(**self.settings)
        try:
            dbmigrate.migrate()
        except subprocess.CalledProcessError as e:
            self.assert_('20121019152409-script.sh' in str(e))
        else:
            self.fail('Expected the script to fail')

    def test_ignore_filenames_sha1_migration(self):
        self.settings['directory'] = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'sha1-update-1')
        dbmigrate = DBMigrate(**self.settings)
        dbmigrate.migrate()
        dbmigrate.directory = os.path.join(
            os.path.dirname(__file__), 'fixtures', 'sha1-update-2')
        dbmigrate.renamed()
        dbmigrate.migrate()
        self.assertEqual(
            dbmigrate.engine.performed_migrations,
            [('20120115075300-add-another-test-table-renamed-reordered.sql',
              '4aebd2514665effff5105ad568a4fbe62f567087'),
             ('20120115075349-create-user-table.sql',
              '0187aa5e13e268fc621c894a7ac4345579cf50b7')])
