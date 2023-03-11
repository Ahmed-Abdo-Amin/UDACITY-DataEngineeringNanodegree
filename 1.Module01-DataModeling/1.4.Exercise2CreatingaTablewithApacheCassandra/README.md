# L1 Exercise 2: Creating a Table with Apache Cassandra
<img src="./cassandralogo.png">

### Walk through the basics of Apache Cassandra. Complete the following tasks:<li> Create a table in Apache Cassandra, <li> Insert rows of data,<li> Run a simple SQL query to validate the information. <br>
`#####` denotes where the code needs to be completed.
    
Note: __Do not__ click the blue Preview button in the lower taskbar


```python
!pip install cassandra-driver
```

    Collecting cassandra-driver
      Using cached https://files.pythonhosted.org/packages/af/aa/3d3a6dae349d4f9b69d37e6f3f8b8ef286a06005aa312f0a3dc7af0eb556/cassandra-driver-3.25.0.tar.gz
    Collecting six>=1.9 (from cassandra-driver)
      Using cached https://files.pythonhosted.org/packages/d9/5a/e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/six-1.16.0-py2.py3-none-any.whl
    Collecting geomet<0.3,>=0.1 (from cassandra-driver)
      Downloading https://files.pythonhosted.org/packages/cf/21/58251b3de99e0b5ba649ff511f7f9e8399c3059dd52a643774106e929afa/geomet-0.2.1.post1.tar.gz
    Collecting futures (from cassandra-driver)
      Downloading https://files.pythonhosted.org/packages/d4/ea/9d513529a89bcbcd07c8acbac9eecfad29e7562e0b9d69d14f475987ad70/futures-3.4.0-py2-none-any.whl
    Collecting click (from geomet<0.3,>=0.1->cassandra-driver)
      Using cached https://files.pythonhosted.org/packages/d2/3d/fa76db83bf75c4f8d338c2fd15c8d33fdd7ad23a9b5e57eb6c5de26b430e/click-7.1.2-py2.py3-none-any.whl
    Installing collected packages: six, click, geomet, futures, cassandra-driver
      Running setup.py install for geomet: started
        Running setup.py install for geomet: finished with status 'done'
      Running setup.py install for cassandra-driver: started
        Running setup.py install for cassandra-driver: finished with status 'done'
    Successfully installed cassandra-driver-3.25.0 click-7.1.2 futures-3.4.0 geomet-0.2.1.post1 six-1.16.0
    

    DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
    WARNING: You are using pip version 19.2.3, however version 20.3.4 is available.
    You should consider upgrading via the 'python -m pip install --upgrade pip' command.
    

#### Import Apache Cassandra python package


```python
import cassandra
from cassandra.cluster import Cluster
```

### Create a connection to the database
If you use cassandra in local machine, run it in cmd by "cassandra -f"


```python
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)
 
```

### TO-DO: Create a keyspace to do the work in 


```python
## TO-DO: Create the keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS music_library_1 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
```

### TO-DO: Connect to the Keyspace


```python
## To-Do: Add in the keyspace you created
try:
    session.set_keyspace('music_library_1')
except Exception as e:
    print(e)
```

### Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single. 

`song_title
artist_name
year
album_name
single`

### TO-DO: You need to create a table to be able to run the following query: 
`select * from songs WHERE year=1970 AND artist_name="The Beatles"`


```python
## TO-DO: Complete the query below
query = "CREATE TABLE IF NOT EXISTS music_library_table_1 "
query = query + "(song_title text, artist_name text, year int, album_name text, single Boolean, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

```

### TO-DO: Insert the following two rows in your table
`First Row:  "Across The Universe", "The Beatles", "1970", "False", "Let It Be"`

`Second Row: "The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"`


```python
## Add in query and then run the insert statement
query = "INSERT INTO music_library_table_1 (song_title, artist_name, year, album_name, single)" 
query = query + " VALUES (%s, %s, %s, %s, %s)"

try:
    session.execute(query, ("Across The Universe", "The Beatles", 1970, "Let It Be", False))
except Exception as e:
    print(e)
    
try:
    session.execute(query, ("Think For Yourself", "The Beatles", 1965, "Rubber Soul", False))
except Exception as e:
    print(e)
```

### TO-DO: Validate your data was inserted into the table.


```python
## TO-DO: Complete and then run the select statement to validate the data was inserted into the table
query = 'SELECT * FROM music_library_table_1'
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)
```

    1965 Rubber Soul The Beatles
    1970 Let It Be The Beatles
    

### TO-DO: Validate the Data Model with the original query.

`select * from songs WHERE YEAR=1970 AND artist_name="The Beatles"`


```python
##TO-DO: Complete the select statement to run the query 
query = "SELECT * from music_library_table_1 where YEAR=1970 and artist_name = 'The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)
```

    1970 Let It Be The Beatles
    

### And Finally close the session and cluster connection


```python
session.shutdown()
cluster.shutdown()
```


```python

```
