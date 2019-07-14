import sqlite3, ast

##############################################
### Login to database
##############################################

def login(dbfile):
    conn = sqlite3.connect(dbfile)  # create or open db file
    curs = conn.cursor()
    return conn, curs

##############################################
### Create new database
##############################################

def makedb(dbfile, table, columnFeatures):
    #columnFeatures = input("eg: (Column1 char(30), Column2 char(10), Column3 int(4))")
    conn, curs = login(dbfile)
    try:
        curs.execute('drop table ' + table)
        print('Dropped table ' + table)
    except:
        print('database table did not exist')
    command = 'create table %s %s' % (table, columnFeatures)
    curs.execute(command)
    conn.commit()

##############################################
### Load Data
##############################################

def loaddb(table, dbfile, datafile, conn=None, verbose=True):
    conn, curs = login(dbfile)
    file = open(datafile)
    rows = [line.rstrip().split('\t') for line in file]  # [[x,x,x], [x,x,x]]
    rows = [str(tuple(rec)) for rec in rows[1:]]            # ["(x,x,x)", "(x,x,x)"]
    for recstr in rows:
        curs.execute('insert into ' + table + ' values ' + recstr)
    if conn:
        conn.commit()
    if verbose:
        print(len(rows), 'rows loaded')

##############################################
### Remove a table from Database
##############################################

def cleardb(dbfile, table):
    conn, curs = login(dbfile)
    try:
        curs.execute('drop table ' + table)
        conn.commit()
        print('Dropped table ', table)
    except:
        print(table, 'table did not exist, creating this table')