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


from setuptools import setup

setup(name='litecache',
      version='1.0.2',
      author='David Preece',
      author_email='davep@polymath.tech',
      url='https://polymath.tech',
      license='BSD',
      packages=['litecache'],
      install_requires=['inotify_simple'],
      description='Thread safe query cache and async write for sqlite databases',
      long_description="A brutally simple map of (SQL, (params,)) to a single result with writes delivered "
                       "on a separate thread for running non-blocking applications on slow drives.",
      keywords='sqlite sqlite3 asynchronous sql query cache inotify kqueue kevent',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Intended Audience :: Information Technology',
          'License :: OSI Approved :: BSD License',
          'Topic :: Database',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: Utilities',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6'
      ],
      )
