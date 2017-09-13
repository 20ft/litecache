import logging
import time
import litecache.cache

logging.basicConfig(level=logging.DEBUG)

litecache = litecache.cache.SqlCache(".", "test", "CREATE TABLE test (param TEXT NOT NULL);")
time.sleep(0.5)
print(litecache.query("SELECT * FROM test WHERE param=?", ('fred', )))
time.sleep(0.5)
litecache.async("INSERT INTO test VALUES (?)", ('fred', ))
time.sleep(0.5)
print(litecache.query("SELECT * FROM test WHERE param=?", ('fred', )))
time.sleep(0.5)
litecache.async("DELETE FROM test WHERE param=?", ('fred', ))
time.sleep(0.5)
print(litecache.query("SELECT * FROM test WHERE param=?", ('fred', )))
time.sleep(0.5)
litecache.close()
time.sleep(0.5)
