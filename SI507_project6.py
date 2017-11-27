# Import statements
import psycopg2
import csv
#from config import *
import json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import psycopg2.extras

conn, cur = None, None

# Write code / functions to set up database connection and cursor here.


def get_connection_and_cursor():
    global conn, cur
    try:
        conn = psycopg2.connect("dbname='innoobi_507project6' user='{}' password='{}'".format(db_user, db_password)) # No password on the databases yet -- wouldn't want to save that in plain text, anyway
            # Remember: need to, at command prompt or in postgres GUI: createdb test507_music (or whatever db name is in line ^)
        print("Success connecting to database")

    except:
        try:
             conn = psycopg2.connect("dbname='postgres' user='{}' password='{}'".format(db_user, db_password)) # No password on the databases yet -- wouldn't want to save that in plain text, anyway
             db_Name = "innoobi_507project6"
             conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
             cur = conn.cursor()
             sql = """CREATE DATABASE """
             cur.execute(sql + db_Name)
             cur.close()
             conn.close()
             conn = psycopg2.connect("dbname='innoobi_507project6' user='{}' password='{}'".format(db_user, db_password)) # No password on the databases yet -- wouldn't want to save that in plain text, anyway
                 # Remember: need to, at command prompt or in postgres GUI: createdb test507_music (or whatever db name is in line ^)
             print("Success connecting to database")

        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) # Stop running program if there's no db connection.

    ## SETUP FOR CREATING DATABASE AND INTERACTING IN PYTHON
    # cur = conn.cursor()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) # So you can insert by column name, instead of position, which makes the Python code even easier to write!
    return conn, cur

# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database(conn, cur):
    ## Code to DROP TABLES IF EXIST IN DATABASE (so no repeats)
    ## We'll address this idea in more depth later.
    cur.execute("DROP TABLE IF EXISTS States, Sites CASCADE")
    conn.commit()

    cur.execute("CREATE TABLE States(ID SERIAL PRIMARY KEY, Name VARCHAR(40) UNIQUE)")
    cur.execute("CREATE TABLE Sites (ID SERIAL PRIMARY KEY, Name VARCHAR(128) UNIQUE, Type VARCHAR(128), State_ID INTEGER REFERENCES States(ID) NOT NULL, Location VARCHAR(255), Description TEXT)")
    conn.commit()
    print('Setup database complete')

# Write code / functions to deal with CSV files and insert data into the database here.

def create_JSON():
    STATEDIC = {}
    filename = ['arkansas.csv', 'california.csv', 'michigan.csv']
    state = ['Arkansas', 'California', 'Michigan']
    jsonname = ['arkansas.json', 'california.json', 'michigan.json']

    for i in range(3):
        with open(filename[i], 'r', encoding='utf-8') as f:
            next(f)
            reader = csv.DictReader(f)
            reader.fieldnames = 'Name', 'Location', 'Type', 'Address', 'Description'
            rows = list(reader)
            STATEDIC[state[i]] = rows

        with open (jsonname[i], 'w', encoding='utf-8') as f:
            dumping = json.dumps(rows, indent=4, sort_keys=False, separators=(',', ': '))
            f.write(dumping)
    return STATEDIC

def insert_state(state, conn, cur):
    """Inserts an state and returns name, None if unsuccessful"""
    sql = """INSERT INTO States (Name) VALUES (%s)"""
    cur.execute(sql, (state,))
    conn.commit
    sql = """SELECT ID FROM States where name = %s"""
    cur.execute(sql, (state,))
    data = ''.join(map(str,cur.fetchone()))
    return data

def insert_site(name, location, types, description, states, conn, cur):
    """Returns True if succcessful, False if not"""
    sql = """INSERT INTO Sites(Name, Type, Location, Description, State_ID) VALUES(%s, %s, %s, %s, %s)"""
    cur.execute(sql,(name, types, location, description, states ))
    conn.commit()
    return True

def add_dic_to_db(superlist):
    for diction in superlist:
        states = insert_state(diction, conn, cur)
        for site in superlist[diction]:
            res = insert_site(site["Name"], site["Location"], site["Type"], site["Description"], states, conn, cur) # Here: passing in actual conn and cur that exist
            if res:
                print("Success adding site and state: {}, {}".format(site["Name"], diction))
            else:
                print("Failed adding site and state")


# Write code to be invoked here (e.g. invoking any functions you wrote above)

if __name__ == '__main__':

    db_user = str(input("Please enter user name: "))
    db_password = str(input("Please enter password: "))

    conn, cur = get_connection_and_cursor()
    setup_database(conn, cur)

    superlist = create_JSON()
    add_dic_to_db(superlist)

# Write code to make queries and save data in variables here.

    cur.execute("SELECT location FROM Sites")
    all_locations = cur.fetchall()
    #print(all_locations)

    cur.execute("""SELECT name FROM Sites WHERE Sites.description LIKE '%beautiful%'""")
    beautiful_sites = cur.fetchall()
    print(beautiful_sites)

    cur.execute("""SELECT count(*) FROM Sites WHERE Sites.type LIKE 'National Lakeshore'""")
    natl_lakeshores = ''.join(map(str,cur.fetchone()))
    print(natl_lakeshores)

    cur.execute("""SELECT name FROM Sites WHERE (SELECT id FROM States WHERE name LIKE 'Michigan') = Sites.state_id""")
    michigan_names = cur.fetchall()
    print(michigan_names)

    cur.execute("""SELECT count(*) FROM Sites WHERE (SELECT id FROM States WHERE name LIKE 'Arkansas') = Sites.state_id""")
    total_number_arkansas = ''.join(map(str,cur.fetchone()))
    print(total_number_arkansas)
