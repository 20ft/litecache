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

"""An 'eventually consistent' sqlite async delivery wrapper. """

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
            self.db = sqlite3.connect('file:%s?nolock=1' % self.filename, uri=True, check_same_thread=False)
        except BaseException as e:
            raise RuntimeError("Couldn't open %s.sqlite3: %s" % (name, e))

        # make sure the database has been initialised
        cursor = self.db.execute("SELECT * FROM sqlite_master WHERE type='table'")
        if cursor.fetchone() is None:
            self.db.executescript(create_sql)
            self.db.commit()
            logging.info("Created new database: " + name)

        # async updates
        self.update_queue = Queue()
        self.update_process = Process(target=SqlCache._updates, args=(self.filename, self.update_queue))
        self.update_process.start()

    def close(self):
        logging.debug("Closing: " + self.filename)
        self.update_queue.put(None)  # update thread too
        self.update_process.join()  # update thread too
        self.db.close()
        logging.debug("Closed: " + self.filename)

    def query(self, sql, params=()):
        """Synchronously query"""
        cursor = self.db.execute(sql, params)
        results = cursor.fetchall()
        return results

    def query_one(self, sql, params=(), error=b''):
        """Synchronously query for exactly one row"""
        # Used as a KV store
        cursor = self.db.execute(sql, params)
        row = cursor.fetchone()
        if row is None:  # zero rows
            raise ValueError(error)
        return row

    def mutate(self, sql, params):
        """Queue the given SQL and it's parameters to be written to the database"""
        self.update_queue.put((sql, params))

    @staticmethod
    def _updates(filename, queue):
        # really not very important
        os.setpriority(os.PRIO_PROCESS, 0, 15)
        
        # listens on the queue for SQL to write to the database
        rw_sql = sqlite3.connect(filename, isolation_level=None)  # rw
        while True:
            record = queue.get()

            # exit?
            if record is None:
                rw_sql.close()
                logging.debug("Update thread closed for: " + filename)
                return

            # go
            logging.debug("Updating SQL: " + record[0])
            rw_sql.execute(record[0], record[1])
