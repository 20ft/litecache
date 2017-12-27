import logging
import time
import litecache.cache

logging.basicConfig(level=logging.DEBUG)

litecache = litecache.cache.SqlCache(".", "test", "CREATE TABLE test (param TEXT NOT NULL);")
print(litecache.query("SELECT * FROM test WHERE param=?", ('fred', )))
litecache.mutate("INSERT INTO test VALUES (?)", ('fred', ))
print(litecache.query("SELECT * FROM test WHERE param=?", ('fred', )))
litecache.mutate("DELETE FROM test WHERE param=?", ('fred', ))
print(litecache.query("SELECT * FROM test WHERE param=?", ('fred', )))
