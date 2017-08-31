# litecache

A thread safe SQL cache and asynchronous delivery engine for sqlite3. Currently only returns a single row from the query but I'm sure you can hack around that if need be. Eventually consistent or "not very good" as we used to call it, I hope you realise this is not for 'real' transactional SQL or anything like that. Should be pretty quick though.

Use it by constructing the cache then querying through ```query(sql, params, error)``` where sql and params have the same semantics as the sqlite3 (built into Python 3) library functions, and error is the text that gets placed into a ValueError that gets raised if the query returns no values. Again, I'm sure you can hack this to bits quite successfully.

To use: construct the cache passing a directory to place the database files, the name of the database (file), and a source directory. If, on construction, the object creates a blank sqlite3 database (say, 'foo') it will look for a sql script in the source directory with which to initialise the database (ie 'foo.sql'). Query as above but, **important**, pass any SELECT, UPDATE, DELETE or any other operations that will mutate the database through the 'mutate' call and not query. Mutate wipes the *entire* cache to ensure we're not serving out of date results.

```
db = litecache.SqlCache('/var/', 'foo', '/home/me/sql')  # will initialse off /home/me/sql/foo.sql

db.mutate('INSERT INTO sometable (col, umns) VALUES (?, ?)", (20, 30))

# mutate grabs a mutex so query will block unti it has finished writing into sometable
print(db.query('SELECT umns FROM sometable WHERE col=?', (20,), "somehow wasn't stored")[0]) 

# and this one will be heaps faster
print(db.query('SELECT umns FROM sometable WHERE col=?', (20,), "somehow wasn't stored")[0]) 
```
