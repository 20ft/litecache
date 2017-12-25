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

import sqlite3
import logging
import _thread
import os
from threading import Thread
from multiprocessing import Queue, Process


class SqlCache:
    def __init__(self, directory, name, create_sql):
        # open or make the database
        self.name = name
        self.filename = directory + "/" + name + ".sqlite3"
        self.db = None
        try:
            self.db = sqlite3.connect(self.filename)
        except BaseException as e:
            raise RuntimeError("Couldn't open %s.sqlite3, fatal: %s" % (name, str(e)))

        # make sure the database has been initialised
        cursor = self.db.execute("SELECT * FROM sqlite_master WHERE type='table'")
        if cursor.fetchone() is None:
            self.db.executescript(create_sql)
            self.db.commit()
            logging.info("Created new database: " + name)

        # set up various options
        self.db.execute("PRAGMA AUTO_VACUUM = FULL")
        self.db.execute("PRAGMA AUTOMATIC_INDEX = False")
        self.db.execute("PRAGMA ENCODING = 'UTF-8'")
        self.db.execute("PRAGMA SYNCHRONOUS = OFF")
        self.db.execute("PRAGMA THREADS = 0")
        self.db.execute("PRAGMA JOURNAL_MODE = MEMORY")
        self.db.execute("PRAGMA TEMP_STORE = MEMORY")

    def underlying(self):
        return self.db

    def close(self):
        self.db.close()

    def query(self, sql, params):
        """Query and return results"""
        cursor = self.db.execute(sql, params)
        results = cursor.fetchall()
        return results

    def query_one(self, sql, params, error):
        """Query and return results for exactly one row"""
        # I use this for a KV store
        cursor = self.db.execute(sql, params)
        row = cursor.fetchone()
        if row is None:  # zero rows
            raise ValueError(error)
        return row
