"""An implementation of multimap with per-item expiration backed up by Redis."""

import inspect
from datetime import datetime
from itertools import cycle
from typing import Any, Callable, Generator, Iterable, Iterator, Tuple, TypeVar

T = TypeVar("T")  # Type of values returned by MultiMapWithTTL, user defined by ``cat_fn``


class MultiMapWithTTL(object):
    """
    An implementation of multimap with per-item expiration backed up by Redis.

    It was based on: https://quickleft.com/blog/how-to-create-and-expire-list-items-in-redis/
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

        >>> s = MultiMapWithTTL('multimap')
        >>> s.add('a', 1, 2, 3)
        >>> sorted(s.get('a'))
        [1, 2, 3]
        >>> s.add_many([('b', (4, 5, 6)), ('c', (7, 8, 9)), ])
        >>> sorted(sorted(values) for values in s.get_many('a', 'b', 'c')))
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    """

    def __init__(self, redis_client, key_prefix: str, ttl: int = 3600, cast_fn: Callable[[Any], Any] = None):
        """
        Initialize the instance.

        Args:
            redis_client: A redis-py.StrictRedis client instance.
            key_prefix (str): A prefix to generate Redis keys.
            ttl (int): Set a timeout, in seconds, of when old values should be removed.
                After the timeout has expired without adding new items to a key, the key itself
                will be automatically deleted. Defaults to 60 min.
            cast_fn (Callable[[str], T]): Cast the returned values from Redis
                to a desired type, defaults to `int`
        """
        self.key_prefix = key_prefix
        self.ttl = ttl
        self.cast_fn = cast_fn if cast_fn is not None else lambda x: int(x)
        self.redis = redis_client

        # From redis-py 3.x, the zadd method changed from accepting (*args) to a (mapping)
        self.old_redis = 'mapping' not in inspect.signature(redis_client.zadd).parameters.keys()

    @staticmethod
    def _get_current_score():  # type: () -> int
        """Return the current timestamp as score."""
        return int(datetime.now().timestamp())

    def _get_ttl_score(self):  # type: () -> int
        """Return a future timestamp (now + ttl) to be assigned on new values."""
        return self._get_current_score() + self.ttl

    def _get_score_iter(self):  # type: () -> Iterator[int]
        """Return a generator of the current score (same value will be generated forever)."""
        return cycle([self._get_ttl_score()])

    def _get_key(self, name: str) -> str:
        """Return `name` namespaced with the key prefix, so all keys starts equal."""
        return f"{self.key_prefix}:{name}"

    def add(self, name: str, *values: Iterable[Any]) -> None:
        """
        Insert `*values` at the `name` key.

        Args:
            name (str): The key name where `values` should be stored.
            *values: A list of values to be stored at `name`.

        Returns:
            None
        """
        self.add_many(((name, values),))

    def get(self, name):  # type: (str) -> Generator[T, None, None]
        """Return a generator of all values stored at `name` that are not expired."""
        return next(self.get_many(name))

    def add_many_with_ttl(self, data):  # type: (Iterable[Tuple[str,Iterable[Tuple[Any, int]]]]) -> None  # noqa
        """
        Bulk insert data.

        Args:
            data: An iterator of (key, (values/ttls)) pairs.

            The ttl is the expected timestamp when the value should expire.

            As this:
                MultiMapWithTTL(redis_client, 'expiringset').add_many_with_ttl([
                    ('a', ((value1, score1), (value2, score2), (value3, score3))),
                    ('b', ((value4, score4), (value5, score5), (value6, score6))),
                ])

            Example:
                MultiMapWithTTL(redis_client, 'expiringset').add_many_with_ttl([
                    ('a', ((1, 159165312), (2, 159165312), (3, 159165312))),
                    ('b', ((4, 159165312), (5, 159165312), (6, 159165312))),
                    ('c', ((7, 159165312), (8, 159165312), (9, 159165312))),
                ])

        Returns:
            None
        """
        # The operations in the pipeline were ordered carefully such that failure
        # of the subsequent operations do not leave the data structure in an inconsistent state.
        # Thanks to that, we do not need to use a transaction or
        # wait calls, making the code efficient and robust.
        pipeline = self.redis.pipeline(transaction=False)
        current_score = self._get_current_score()

        for name, values in data:
            key = self._get_key(name)

            # expireat api requires a half open interval
            pipeline.expireat(key, self._get_ttl_score() + 1)
            # We may don't have values to add,
            # but we still want to execute the other steps on pipeline.

            if self.old_redis:
                # we're building a generator as expected by `.zadd(*args)`,
                # in the form of: score1, name1, score2, name2, ...
                params = [item for pair in values for item in pair]
                if params:
                    pipeline.zadd(key, *params)
            else:
                mapping = {name: score for score, name in values}
                if mapping:
                    pipeline.zadd(key, mapping=mapping)

            # note zremrangebyscore is inclusive
            pipeline.zremrangebyscore(key, 0, current_score)
        pipeline.execute()

    def add_many(self, data):  # type: (Iterable[Tuple[str, Iterable[Any]]]) -> None
        """
        Bulk insert data.

        Args:
            data: An iterator of (key, values) pairs.

            Example:
                MultiMapWithTTL(redis_client, 'expiringset').add_many([
                    ('a', (1, 2, 3)),
                    ('b', (4, 5, 6)),
                    ('c', (7, 8, 9)),
                ])

        Returns:
            None
        """
        scores = self._get_score_iter()
        self.add_many_with_ttl((name, zip(scores, values)) for name, values in data)

    def get_many(self, *names) -> Generator[Generator[T, None, None], None, None]:
        """
        Return a generator of generators of all values stored at `*names` that are not expired.

        Args:
            *names: Name of the keys being queried.

        Returns:
            Generator[T]
        """
        pipeline = self.redis.pipeline(transaction=False)
        current_score = self._get_current_score() + 1
        keys = (self._get_key(name) for name in names)
        for key in keys:
            # zrangebyscore inclusive range
            pipeline.zrangebyscore(key, current_score, "+inf")
        return ((self.cast_fn(x) for x in results) for results in pipeline.execute())

    def delete(self, *names) -> None:
        """Delete `*names` from the multimap."""
        keys = (self._get_key(name) for name in names)
        self.redis.delete(*keys)
