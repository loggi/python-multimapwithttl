# MultiMapWithTTL


[![pypi](https://img.shields.io/pypi/v/python-multimapwithttl.svg)](https://pypi.org/project/python-multimapwithttl/)
[![python](https://img.shields.io/pypi/pyversions/python-multimapwithttl.svg)](https://pypi.org/project/python-multimapwithttl/)
[![Build Status](https://github.com/loggi/python-multimapwithttl/actions/workflows/dev.yml/badge.svg)](https://github.com/loggi/python-multimapwithttl/actions/workflows/dev.yml)
[![codecov](https://codecov.io/gh/loggi/python-multimapwithttl/branch/main/graphs/badge.svg)](https://codecov.io/github/loggi/python-multimapwithttl)



An implementation of multimap with per-item expiration backed up by Redis.


* Documentation: <https://loggi.github.io/python-multimapwithttl>
* GitHub: <https://github.com/loggi/python-multimapwithttl>
* PyPI: <https://pypi.org/project/python-multimapwithttl/>
* Free software: MIT


## Description

This lib is based on: https://quickleft.com/blog/how-to-create-and-expire-list-items-in-redis/
without the need for an extra job to delete old items.

Values are internally stored on Redis using Sorted Sets :

    key1: { (score1, value1), (score2, value2), ... }
    key2: { (score3, value3), (score4, value4), ... }
    ...

Where the `score` is the timestamp when the value was added.
We use the timestamp to filter expired values and when an insertion happens,
we opportunistically garbage collect expired values.

The key itself is set to expire through redis ttl mechanism together with the newest value.
These operations result in a simulated multimap with item expiration.

You can use to keep track of values associated to keys,
when the value has a notion of expiration.

```
>>> s = MultiMapWithTTL(redis_client, 'multimap')
>>> s.add('a', 1, 2, 3)
>>> sorted(s.get('a'))
[1, 2, 3]
>>> s.add_many([('b', (4, 5, 6)), ('c', (7, 8, 9)), ])
>>> sorted(sorted(values) for values in s.get_many('a', 'b', 'c')))
[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
```
