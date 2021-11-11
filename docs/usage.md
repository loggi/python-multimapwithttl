# Usage

To use MultiMapWithTTL in a project, first:

```
from multimapwithttl import MultiMapWithTTL
```

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
