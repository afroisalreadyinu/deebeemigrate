import os
import sys
import subprocess
import logging
from hashlib import sha1
from optparse import OptionParser
from datetime import datetime
from glob import glob

from deebeemigrate.dbengines import (DatabaseMigrationEngine,
                                     FilenameSha1,
                                     SQLException)
from deebeemigrate.command import command


logger = logging.getLogger(__name__)


class OutOfOrderException(Exception):
    pass


class ModifiedMigrationException(Exception):
    pass


class DBMigrate(object):
    """A set of commands to safely migrate databases automatically"""
    def __init__(self,
                 out_of_order,
                 dry_run,
                 connection_string,
                 directory,
                 run_for_new_db):
        self.out_of_order = out_of_order
        self.dry_run = dry_run
        self.engine = DatabaseMigrationEngine.connect(connection_string)
        self.directory = directory
        self.run_for_new_db = run_for_new_db


    def blobsha1(self, filename):
        """returns the git sha1sum of a file so the exact migration
        that was run can easily be looked up in the git history"""
        text = open(filename).read()
        s = sha1(("blob %u\0" % len(text)).encode('UTF-8'))
        s.update(text.encode('UTF-8'))
        return s.hexdigest()

    def current_migrations(self):
        """returns the current migration files as a list of
           (filename, sha1sum) tuples"""
        return [
            FilenameSha1(os.path.basename(filename), self.blobsha1(filename))
            for filename in glob(os.path.join(self.directory, '*'))]

    def warn(self, message):
        sys.stderr.write(message + "\n")

    @command
    def renamed(self, *args):
        """rename files in the migration table if the order changed"""
        performed_migrations = dict(
            (v, k) for k, v in self.engine.performed_migrations)
        current_migrations = dict(
            (v, k) for k, v in self.current_migrations())
        renames = []
        for sha1, old_filename in performed_migrations.items():
            new_filename = current_migrations.get(sha1)
            if sha1 in current_migrations and old_filename != new_filename:
                renames.append(FilenameSha1(new_filename, sha1))
        sql = '\n'.join(
            "UPDATE dbmigration SET filename = '%(filename)s' "
            "WHERE sha1 = '%(sha1)s';" % rename._asdict()
            for rename in renames)
        if self.dry_run:
            return sql
        else:
            self.engine.execute(sql)

    @command
    def migrate(self, *args):
        """migrate a database to the current schema"""
        new_db = False
        if not self.dry_run:
            try:
                self.engine.create_migration_table()
            except SQLException:
                pass
            else:
                new_db = True
        try:
            performed_migrations = self.engine.performed_migrations
        except SQLException:
            if self.dry_run:
                performed_migrations = []
            else:
                raise

        current_migrations = self.current_migrations()
        files_current = [x.filename for x in current_migrations]
        files_performed = [x.filename for x in performed_migrations]
        files_sha1s_to_run = (
            set(current_migrations) - set(performed_migrations))
        files_to_run = [x.filename for x in files_sha1s_to_run]
        if len(files_performed):
            latest_migration = max(files_performed)
            old_unrun_migrations = list(filter(
                lambda f: f < latest_migration, files_to_run))
            if len(old_unrun_migrations):
                if self.out_of_order:
                    self.warn('Running [%s] out of order.' %
                              ','.join(old_unrun_migrations))
                else:
                    raise OutOfOrderException(
                        '[%s] older than the latest performed migration' %
                        ','.join(old_unrun_migrations))
        modified_migrations = set(files_to_run).intersection(files_performed)
        if modified_migrations:
            raise ModifiedMigrationException(
                '[%s] migrations were modified since they were '
                'run on this database.' % ','.join(modified_migrations))
        deleted_migrations = (
            set(files_performed + files_to_run) - set(files_current))
        if deleted_migrations:
            raise ModifiedMigrationException(
                '[%s] migrations were deleted since they were '
                'run on this database.' % ','.join(deleted_migrations))

        migrations = [self.engine.sql(self.directory, filename, sha1_hash)
                      for filename, sha1_hash in sorted(files_sha1s_to_run)]

        if self.dry_run:
            return '\n'.join(str(x) for x in migrations)

        for migration_info in migrations:
            if not (new_db and not self.run_for_new_db):

                if migration_info.command:
                    subprocess.check_call(migration_info.command)

                if migration_info.migration_sql:
                    self.engine.execute(migration_info.migration_sql)
                migration_info.applied = True
            else:
                migration_info.ghost = True
            self.engine.execute(migration_info.migration_info_sql)

        return self.generate_response(migrations, new_db)


    def generate_response(self, migrations, new_db):
        response = ['Created migrations table'] if new_db else []

        if not migrations:
            response.append('No unapplied migrations')
            return '\n'.join(response)
        applied = [migration for migration in migrations if migration.applied]
        ghosts = [migration for migration in migrations if migration.ghost]
        if applied:
            response.append('Ran %d migrations:' % len(applied))
            response.append('\n'.join(x.filename for x in applied))
        if ghosts:
            response.append('Simulated %d migrations:' % len(ghosts))
            response.append('\n'.join(x.filename for x in ghosts))
        return '\n'.join(response)


    @command
    def create(self, slug, ext="sql", open=open):
        """create a new migration file"""

        create_content_map ={
            'sql': '-- add your migration here',
            'py': '#!/usr/bin/env python\n# add migration here',
        }
        content = create_content_map.get(ext, "")

        dstring = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        slug = "-".join(slug.split(" "))
        filename = os.path.join(self.directory, '%s-%s.%s' %
                                (dstring, slug, ext))
        if self.dry_run:
            return 'Would create %s with:\n%s' % (filename, content)
        else:
            open(filename, 'w').write(content)


def main():
    usage = '\n'
    for command_name, help in sorted(command.help.iteritems()):
        usage += "%s - %s\n" % (command_name.rjust(15), help)

    parser = OptionParser(usage=usage)

    parser.add_option(
        "-o", "--out-of-order", dest="out_of_order", action="store_true",
        help="allow migrations to be run out of order",
        default=False)
    parser.add_option(
        "-n", "--dry-run", dest="dry_run", action="store_true",
        help="print SQL that would be run but take no real action",
        default=False)
    parser.add_option(
        "-c", "--connection-string", dest="connection_string", action="store",
        help="string used by the database engine to connect to the database",
        default="sqlite:///:memory:",
        type="string")
    parser.add_option(
        "-d", "--directory", dest="directory", action="store",
        help="directory where the migrations are stored",
        type="string",
        default=".")
    parser.add_option(
        "-r", "--run-for-new-db", dest="run_for_new_db", action="store_false",
        help="whether the existing migrations should be run if the migration table has been created")

    (options, args) = parser.parse_args()

    if not len(args):
        parser.print_help()
        return

    options = vars(options)
    options['connection_string'] = os.environ.get(
        'DBMIGRATE_CONNECTION', options['connection_string'])
    dbmigrate = DBMigrate(**options)
    result = command.commands[args[0]](dbmigrate, *args[1:])
    if result:
        print(result)


if __name__ == '__main__':
    main()
