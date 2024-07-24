PostgreSQL Workload Analyzer detailled installation guide
=========================================================

Read [README.md](https://github.com/powa-team/powa/blob/master/README.md) and
[the official documentation](http://powa.readthedocs.io/) for further details
about PoWA.

PoWA requires PostgreSQL 9.5 or more. This documentation assumes you're using
the version 15 of PostgreSQL.

The following documentation describes the detailed installation steps to install
PoWA.


Download powa-archivist from the website
----------------------------------------

The latest stable version should be used. It can be downloaded from
[github](https://github.com/powa-team/powa-archivist/releases/latest).

This documentation assumes that the version is 4.2.2, and you downloaded
the .zip file.

Unpack the downloaded file
--------------------------

```
cd /usr/src
unzip powa-REL_4_2_2.zip
```

Compile and install the software
--------------------------------

Before proceeding, be sure to have a compiler installed and the appropriate PostgreSQL development packages. Something like
```
apt-get install postgresql-server-dev-15
```
or
```
yum install postgresql94-devel
```

Then:
```
cd /usr/src/powa-REL_4_2_2
make
```

If everything goes fine, you will have this kind of output :
```
gcc -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -g -fpic -I. [...]   -c -o powa.o powa.c
gcc -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -g -fpic [...] -shared -o powa.so powa.o
```

Install the software :

- This step has to be made with the user that has installed PostgreSQL. If you
  have used a package, it will be certainly be root. If so:
```
sudo make install
```
Else, sudo into the user that owns your PostgreSQL executables, and
```
make install
```

It should output something like the following :
```
/bin/mkdir -p '/usr/pgsql-15/share/extension'
/bin/mkdir -p '/usr/pgsql-15/share/extension'
/bin/mkdir -p '/usr/pgsql-15/lib'
/bin/mkdir -p '/usr/pgsql-15/share/doc/extension'
/usr/bin/install -c -m 644 ./powa.control '/usr/pgsql-15/share/extension/'
/usr/bin/install -c -m 644 ./powa--2.0.1.sql ./powa--2.0-2.0.1.sql ./powa--3.0.0.sql '/usr/pgsql-15/share/extension/'
/usr/bin/install -c -m 755  powa.so '/usr/pgsql-15/postgresql-15.7/lib/'
/usr/bin/install -c -m 644 ./README.md '/usr/pgsql-15/share/doc/extension/'
```


Create a PoWA database and create required extensions
-----------------------------------------------------

Note: if you are upgrading from a previous PoWA release, please consult the
upgrading section at the end of this file.


First, connect to PostgreSQL as administrator :
```
bash-4.1$ psql
psql (9.3.5)
Type "help" for help.
postgres=# create database powa;
CREATE DATABASE
postgres=# \c powa
You are now connected to database "powa" as user "postgres".
powa=# create extension pg_stat_statements ;
CREATE EXTENSION
powa=# create extension btree_gist ;
CREATE EXTENSION
powa=# create extension powa;
CREATE EXTENSION
powa=# \dt
                          List of relations
 Schema |              Name               | Type  |  Owner
--------+---------------------------------+-------+----------
 public | powa_functions                  | table | postgres
 public | powa_last_aggregation           | table | postgres
 public | powa_last_purge                 | table | postgres
 public | powa_statements                 | table | postgres
 public | powa_statements_history         | table | postgres
 public | powa_statements_history_current | table | postgres
 [...]
```


Modify the configuration files
------------------------------

In `postgresql.conf`:

Change the `shared_preload_libraries` appropriately :
```
shared_preload_libraries = 'powa,pg_stat_statements'# (change requires restart)
```

If possible (check with pg_test_timing), activate track_io_timing on your instance, in postgresql.conf:

```
track_io_timing = on
```

Other GUC variables are available. Read [README.md](https://github.com/powa-team/powa/blob/master/README.md) for further details.

In `pg_hba.conf`:

Add an entry if needed for the PostgreSQL user(s) that need to connect on the GUI.
For instance, assuming a `local connection` on database `powa`, allowing any user:

`host    powa    all     127.0.0.1/32    md5`

Restart PostgreSQL
------------------

As root, run the following command :
```
service postgresql-9.3 restart
```

PostgreSQL should output the following messages in the log files :
```
2014-07-25 03:48:20 IST LOG:  registering background worker "powa"
2014-07-25 03:48:20 IST LOG:  loaded library "powa"
2014-07-25 03:48:20 IST LOG:  loaded library "pg_stat_statements"
```

Upgrading from a previous version of PoWA
-----------------------------------------

If you already have an older PoWA installation, you can simply upgrade PoWA with the following steps :

First, connect to PostgreSQL as administrator and update the extension :
```
bash-4.1$ psql powa
psql (9.3.5)
Type "help" for help.
powa=# ALTER EXTENSION powa UPDATE ;
ALTER EXTENSION
```

However, due to a lot of changes in the data storage, it's not possible to
update to PoWA 3.0.0. In this case, you need to drop and create the extension.

Next, you will need to restart PostgreSQL in order to take account of the
updated background worker. As root, run the following command :
```
service postgresql-15 restart
```
