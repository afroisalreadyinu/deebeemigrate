dbmigrate
=========

There are many high quality automatic database migration tools such as [Ruby on Rails' ActiveRecord migrations](http://guides.rubyonrails.org/migrations.html) and [South](http://south.aeracode.org/) for Django. Unfortunately, most that I could find were tightly coupled with a particular framework. Since I work with many different frameworks and I don't like manually migrating database schemas I wrote dbmigrate.


Usage
-----

    Usage:
             create - create a new migration file
            migrate - migrate a database to the current schema
            renamed - rename files in the migration table if the order changed


    Options:
      -h, --help            show this help message and exit
      -o, --out-of-order    allow migrations to be run out of order
      -n, --dry-run         print SQL that would be run but take no real action
      -e ENGINE, --engine=ENGINE
                            database engine
      -c CONNECTION_STRING, --connection-string=CONNECTION_STRING
                            string used by the database engine to connect to the
                            database
      -d DIRECTORY, --directory=DIRECTORY
                            directory where the migrations are stored


Examples
--------

If these commands are run without --dry-run (-n), they will perform the specified actions.

     % dbmigrate --dry-run create test
    Would create ./20121024224936-test.sql with:
    -- add your migration here

     % dbmigrate create "some slug" py -n -d /tmp
    Would create /tmp/20121024224833-some-slug.py with:
    #!/usr/bin/env python
    # add migration here

     % dbmigrate --dry-run -d tests/fixtures/initial migrate
    sql: -- start filename: 20120115075349-create-user-table.sql sha1: 0187aa5e13e268fc621c894a7ac4345579cf50b7
    -- intentionally making this imperfect so it can be migrated
    CREATE TABLE users (
      id int PRIMARY KEY,
      name varchar(255),
      password_sha1 varchar(40)
    );
    INSERT INTO dbmigration (filename, sha1, date) VALUES ('20120115075349-create-user-table.sql', '0187aa5e13e268fc621c894a7ac4345579cf50b7', datetime());


Environment Variables
---------------------

Instead of using `-e` and `-c` for the migrate command,
the environment variables, `DBMIGRATE_ENGINE` and `DBMIGRATE_CONNECTION`
may be used. The defaults are equivalent to:

    export DBMIGRATE_ENGINE=sqlite
    export DBMIGRATE_CONNECTION=:memory:

For the `create` command, you can change the defaults with
`DBMIGRATE_CREATE_CONTENT`. The default would be something like:

    read -d '' -r DBMIGRATE_CREATE_CONTENT <<"EOF"
    {
        'sql': '-- add your migration here',
        'py': '#!/usr/bin/env python\n# add migration here'
    }
    EOF
    export DBMIGRATE_CREATE_CONTENT


Behavior
--------

dbmigrate is very strict by default. Past migrations are treated as immutable and if modifications to past migrations are detected it is treated as an error condition by default.

These are the currently detected situations:

* A migration was modified after it was run on the target database
* A migration was deleted after it was run on the target database
* A new migration was inserted in-between migrations that have already run on the target database

Developers can run dbmigrate with -o or --out-of-order to ignore the out-of-order exception (if you merge in another developer's work that contains a migration) since this situation is usually not that dangerous.

dbmigrate is strict by default so you can safely incrementally update a schema on a staging server and know that the same series of migrations will be performed when migrating production. If something happens that causes the an error condition on staging you should be able to modify the order of the files so the migrations apply cleanly. There will still be situations where you will need to roll back to a backup of your staging database.

Contributing
------------

Please follow [PEP-8](http://www.python.org/dev/peps/pep-0008/) and add unit tests.

Please run the tests against SQLite (default), MySQL, and Postgres (please note that the database specified in MySQL and the schema specified in the Postgres connection settings will be dropped so be sure to use test databases and schemas):

     % nosetests
    Running [20120114221757-before-initial.sql] out of order.
    .....I'm about to fail :-(
    .........I ran!
    .
    ----------------------------------------------------------------------
    Ran 15 tests in 0.032s
    
    OK
    
     % DBMIGRATE_ENGINE=mysql DBMIGRATE_CONNECTION='{"db":"testdbmigrate", "user":"root"}' tox
    ...
      py26: commands succeeded
      py27: commands succeeded
    ERROR:   py32: commands failed
    [The Python 3.2 test will fail against mysql because MySQLdb-python hasn't been ported to 3.x yet]
    
     % DBMIGRATE_ENGINE=postgres DBMIGRATE_CONNECTION='{"database":"dbravender","schema":"testdbmigrate"}' tox
    ...
      py26: commands succeeded
      py27: commands succeeded
      py32: commands succeeded
      congratulations :)


TODO
----

* Settings file to simplify command invocation
* Automated rollbacks
