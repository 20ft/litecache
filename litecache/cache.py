# Copyright (c) 2017 David Preece, All rights reserved.
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""An 'eventually consistent' sqlite cache / async delivery wrapper. """

import sqlite3
import logging
import pathlib
import _thread
from threading import Thread, Lock
from inotify_simple import INotify, flags


class SqlCache:
    def __init__(self, directory, name, src_dir):
        # open or make the database
        self.filename = directory + "/" + name + ".sqlite3"
        self.db = None
        try:
            self.db = sqlite3.connect(self.filename, isolation_level='EXCLUSIVE', check_same_thread=False)
        except:
            raise RuntimeError("Couldn't open %s.sqlite3, fatal." % name)

        # make sure the database has been initialised
        cursor = self.db.execute("SELECT * FROM sqlite_master WHERE type='table'")
        if cursor.fetchone() is None:
            with open("%s%s.sql" % (src_dir, name)) as script_file:
                script = script_file.read()
                self.db.executescript(script)
                self.db.commit()
                logging.info("Created new database: " + name)

        # the actual cache
        self.cache = {}

        # locking access to the cache
        self.lock = Lock()

        # watching for changes
        self.running = True
        self.inotify = INotify()
        self.inotify.add_watch(self.filename, flags.MODIFY | flags.CLOSE_WRITE | flags.CLOSE_NOWRITE)
        self.watch = Thread(group=None, target=self.watch, name="SqlCache: " + name)
        self.watch.start()
        logging.debug("Caching SQL for: " + name)

    def underlying(self):
        return self.db

    def close(self):
        self.lock.acquire()  # ensure any in-flight mutations are done
        self.lock.release()
        self.running = False  # will cause the notify thread to exit the loop when it next times out
        self.db.close()
        self.watch.join()  # wait for watch to actually stop

    def query(self, sql, params):
        """Synchronously query and cache"""
        self.lock.acquire()
        try:
            return self.cache[(sql, params, True)]  # True implies this was an 'all rows' query
        except KeyError:  # not cached, execute the sql
            cursor = self.db.execute(sql, params)
            results = cursor.fetchall()
            self.cache[(sql, params, True)] = results  # cache for next time
            return results
        finally:
            self.lock.release()

    def query_one(self, sql, params, error):
        """Expects to find at least one row in the result set and borks if it doesn't"""
        # I use this for a KV store
        self.lock.acquire()
        try:
            return self.cache[(sql, params, False)]  # a single row query
        except KeyError:
            cursor = self.db.execute(sql, params)
            row = cursor.fetchone()
            if row is None:  # zero rows
                raise ValueError(error)
            else:
                self.cache[(sql, params, False)] = row
            return row
        finally:
            self.lock.release()

    def async(self, sql, params):
        # do the update on a background thread
        self.lock.acquire()
        _thread.start_new_thread(SqlCache._mutate, (self, sql, params))

    def watch(self):
        # watches for changes on the underlying DB and blows away the cache if it sees one
        while self.running:
            for event in self.inotify.read(timeout=1):
                logging.debug("File notification on database, clearing cache: " + self.filename)
                self.cache = {}
        logging.debug("Watch thread closed for: " + self.filename)

    def _mutate(self, sql, params):
        # do the mutation inside a transaction
        # note that the cache will be cleared by 'watch' when it detects the write on the database
        try:
            self.db.execute(sql, params)
            self.db.commit()
        finally:
            self.lock.release()
