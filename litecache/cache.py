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

"""An eventually consistent sqlite cache / async delivery wrapper. """

import _thread
import sqlite3
import logging


class SqlCache:
    def __init__(self, directory, name, src_dir):
        # open or make the database
        self.db = None
        try:
            self.db = sqlite3.connect(directory + "/" + name + ".sqlite3",
                                      isolation_level='EXCLUSIVE',
                                      check_same_thread=False)
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
        self.lock = _thread.allocate_lock()

    def underlying(self):
        return self.db

    def close(self):
        self.lock.acquire()  # ensure any in-flight transactions are done
        self.db.close()

    def query(self, sql, params, error):
        # is it?
        self.lock.acquire()
        try:
            return self.cache[(sql, params)]
        except KeyError:  # not cached, execute the sql
            cursor = self.db.execute(sql, params)
            row = cursor.fetchone()
            if row is None:  # zero rows
                raise ValueError(error)
            else:  # cache for next time
                self.cache[(sql, params)] = row
            return row
        finally:
            self.lock.release()

    def mutate(self, sql, params):
        # do the update on a background thread
        self.lock.acquire()
        _thread.start_new_thread(SqlCache._mutate, (self, sql, params))

    def _mutate(self, sql, params):
        # do the mutation inside a transaction
        try:
            self.db.execute(sql, params)
            self.db.commit()
            # flush the entire cache
            self.cache = {}
        finally:
            self.lock.release()
