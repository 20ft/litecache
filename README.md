# litecache

A thread safe SQL cache and asynchronous delivery engine for sqlite3. Eventually consistent or "not very good" as we used to call it, I hope you realise this is not for 'real' transactional SQL or anything like that - the main use case is for running sqlite on horribly slow disks not that I'm referring to Amazon EFS, of course.

Your best bet for installation is ```pip3 install litecache```.

To use: construct the cache passing the directory where you place the databases, the name of the database (file), and SQL to initialise if it's not there. Call ```query(sql, params)``` where sql and params with the same semantics as the sqlite3 library functions and will return a list of rows. There's also a ```query_one(sql, params, error)``` that returns just the first row of the results if there are any, or raises a ValueError with the passed error string if there are not. This might seem weird but it makes sense if you're using sqlite as a vaguely bodged KV store.

Calling the (badly named, sorry) ```async(sql, params)``` assumes the provided sql is a write and holds a mutex preventing further queries until the write is actually on disk. Mutating the file causes the entire cache to be wiped which no doubt is less than ideal but it does give you zero chance of serving stale data. This cache wiping is achieved through a file watcher so if something external writes to the db, that will wipe the cache as well.

Because there are threads running the cache needs to be explicitly closed before it can be garbage collected. Sorry about that, but a small price to pay :)

```
from litecache.cache import SqlCache

# Builds on-disk only if it's not there to start with.
db = SqlCache('/tmp/', 'foo',
              'CREATE TABLE things (key INTEGER UNIQUE, val INTEGER)')

# Async write with a mutex preventing any reads from taking place.
db.async('INSERT INTO things (key, val) VALUES (?, ?)', (20, 30))

# A "many rows" read:
print(db.query('SELECT val FROM things WHERE key=?', (20,)))

# And this one will be heaps faster:
print(db.query('SELECT val FROM things WHERE key=?', (20,)))

# a single row read:
print(db.query_one('SELECT val FROM things WHERE key=?', (20,),
      'Value was not in database'))

# a single row read that raises
try:
    print(db.query_one('SELECT val FROM things WHERE key=?', (42,),
          'Value was not in database'))
except ValueError as e:
    print(str(e))

# you do actually have to call this
db.close()
```
