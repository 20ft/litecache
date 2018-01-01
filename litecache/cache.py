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
            self.db = sqlite3.connect('file:%s?mode=ro&nolock=1' % self.filename, uri=True, check_same_thread=False)
        except:
            raise RuntimeError("Couldn't open %s.sqlite3, fatal." % name)

        # make sure the database has been initialised
        cursor = self.db.execute("SELECT * FROM sqlite_master WHERE type='table'")
        if cursor.fetchone() is None:
            self.db.executescript(create_sql)
            self.db.commit()
            logging.info("Created new database: " + name)

        # the actual cache
        self.cache = {}

        # watching for changes
        self.running = True
        try:
            from inotify_simple import INotify, flags
            self.mac = False
            self.inotify = INotify()
            self.inotify.add_watch(self.filename, flags.MODIFY | flags.CLOSE_WRITE | flags.CLOSE_NOWRITE)

        except OSError:  # no libc.so.6 on OSX ... try this
            from select import kqueue, kevent, KQ_FILTER_VNODE, KQ_EV_ADD, KQ_EV_CLEAR, KQ_NOTE_WRITE
            self.mac = True
            self.fd = os.open(self.filename, os.O_RDONLY)
            self.kq = kqueue()
            ke = kevent(self.fd, filter=KQ_FILTER_VNODE, flags=KQ_EV_ADD | KQ_EV_CLEAR, fflags=KQ_NOTE_WRITE)
            self.kq.control([ke], 0)

        self.watch = Thread(group=None, target=self._watch, name="SqlCache-watch:" + name, daemon=True)
        self.watch.start()
        logging.debug("Caching SQL for: " + name)

        # async updates
        self.update_queue = Queue()
        self.update_process = Process(target=SqlCache._updates, args=(self.filename, self.update_queue))
        self.update_process.start()

    def close(self):
        logging.debug("Closing cache for: " + self.filename)
        self.running = False  # will cause the notify thread to exit the loop
        self.update_queue.put(None)  # update thread too
        self.watch.join()  # wait for watch to actually stop
        self.update_process.join()  # update thread too
        if self.mac:
            os.close(self.fd)
        self.db.close()
        logging.debug("Closed: " + self.filename)

    def query(self, sql, params=()):
        """Synchronously query and cache"""
        try:
            return self.cache[(sql, params, True)]  # True implies this was an 'all rows' query
        except KeyError:  # not cached, execute the sql
            cursor = self.db.execute(sql, params)
            results = cursor.fetchall()
            self.cache[(sql, params, True)] = results  # cache for next time
            return results

    def query_one(self, sql, params=(), error=b''):
        """Synchronously query and cache for exactly one row"""
        # I use this for a KV store
        try:
            return self.cache[(sql, params, False)]  # False implies a single row query
        except KeyError:
            cursor = self.db.execute(sql, params)
            row = cursor.fetchone()
            if row is None:  # zero rows
                raise ValueError(error)
            else:
                self.cache[(sql, params, False)] = row
            return row

    def mutate(self, sql, params):
        """Queue the given SQL and it's parameters to be written to the database"""
        self.update_queue.put((sql, params))

    def _watch(self):
        # watches for changes on the underlying DB and blows away the cache if it sees one
        while self.running:
            for _ in self.kq.control(None, 256, 1) if self.mac else self.inotify.read():
                if not self.running:
                    break
                self.cache = {}
        logging.debug("Watch thread closed for: " + self.filename)

    @staticmethod
    def _updates(filename, queue):
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
            logging.debug("Updating SQL: " + str(record))
            rw_sql.execute(record[0], record[1])
