import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Create Sparkify database
def create_database():
    # connect to default database (UDACITY-Data-Engineer)
    try:
        conn = psycopg2.connect(database='studentdb', user='student', password='student')
    except psycopg2.Error as e:
        print("Error: Could not make connection to the postgres default database (UDACITY-Data-Engineer)")
        print(e)
    try:
        cur = conn.cursor()
    except:
        print("Error: Could not make curser to the default database (UDACITY-Data-Engineer)")
        print(e)
    conn.set_session(autocommit=True)
    # create sparkify database with UTF8 encoding
    try:
        print()
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH OWNER student ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e: 
        print("Error: Issue creating Database")
        print (e)
    # connect to sparkify database
    try:
        conn = psycopg2.connect(database='sparkifydb', user='student', password='student')
    except psycopg2.Error as e:
        print("Error: Could not make connection to the sparkify database")
        print(e)
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    return conn, cur

# drop All tables of sparkify database
def drop_tables(conn, cur):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue dropping table")
            print (e)

# create All tables of sparkify database
def create_tables(conn, cur):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue creating table")
            print (e)

def main():
    conn, cur = create_database()
    # drop All tables of sparkify database if they exist
    drop_tables(conn, cur)
    # Create All tables of sparkify database if not exist
    create_tables(conn, cur)
    # disconnect session of sparkify database 
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    main()