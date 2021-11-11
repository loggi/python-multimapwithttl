#!/usr/bin/env python
"""Tests for `multimapwithttl` package."""

import time
from datetime import timedelta

import fakeredis
import pytest


@pytest.fixture
def redis_client_base():
    """Return a fake Redis client to be used as storage backend."""
    server = fakeredis.FakeServer()
    return fakeredis.FakeRedis(server=server)


@pytest.fixture(params=[False, True], ids=["redis_3x", "redis_2x"])
def redis_client(redis_client_base, request):
    """Patch the redis_client to simulate the 2x version of redis-py.

    Every test that depends on this fixture will have two versions, one for each redis version.

    This is due to the changes on redis-py client `zadd` API from version 3.x.
    This library aims to be compatible with both versions.
    See: https://github.com/redis/redis-py#mset-msetnx-and-zadd
    """
    from redis.client import Pipeline

    should_patch = request.param

    real_client_zadd = redis_client_base.zadd
    real_pipeline_zadd = Pipeline.zadd

    def pairwise(iterable):
        """Iterate over `iterable` in pairs.

        Example: `s -> (s0, s1), (s2, s3), (s4, s5), ...`
        """
        a = iter(iterable)
        return zip(a, a)

    def patched_zadd(self, name, *args, **kwargs):
        """Set any number of score, element-name pairs to the key ``name``.

        Pairs can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        mapping = {name: score for score, name in pairwise(args)}

        return real_pipeline_zadd(self, name, mapping=mapping)

    if should_patch:
        redis_client_base.__class__.zadd = patched_zadd
        Pipeline.zadd = patched_zadd

    yield redis_client_base

    if should_patch:
        redis_client_base.__class__.zadd = real_client_zadd
        Pipeline.zadd = real_pipeline_zadd


@pytest.fixture
def MultiMapWithTTLClass():
    """Return the `MultiMapWithTTL` class.

    Used to avoid import errors from blocking pytest collect phase.
    """
    from multimapwithttl import MultiMapWithTTL

    return MultiMapWithTTL


@pytest.fixture
def multimap(MultiMapWithTTLClass, redis_client):
    """Return a property configured `MultiMapWithTTL` instance with 10s ttl."""
    return MultiMapWithTTLClass(redis_client, 'multimap', ttl=10)


class TestMultiMapWithTTL(object):
    """Test `MultiMapWithTTL` class."""

    def test_should_add_an_item(self, multimap):
        multimap.add('a', 1)
        assert list(multimap.get('a')) == [1]

    def test_should_allow_read_without_previous_write(self, multimap):
        assert list(multimap.get('a')) == []

    def test_should_allow_read_without_previous_write_many(self, multimap):
        assert list(list(x) for x in multimap.get_many('a', 'b')) == [
            [],
            [],
        ]

    def test_should_not_duplicate_items(self, multimap):
        multimap.add('a', 1)
        multimap.add('a', 1)
        multimap.add('a', 1, 1, 1)
        assert list(multimap.get('a')) == [1]

    def test_should_add_multiple_values(self, multimap):
        values = [2, 3, 5, 7, 9, 11, 15, 13]
        multimap.add('x', *values)
        assert sorted(multimap.get('x')) == sorted(values)

    def test_should_not_break_without_values(self, multimap):
        values = []
        multimap.add('x', *values)
        assert sorted(multimap.get('x')) == values

    def test_should_insert_multiple_keys(self, multimap):
        expected = [
            ('a', [1, 2, 3]),
            ('b', [4, 5, 6]),
            ('c', [2, 4, 8]),
        ]
        multimap.add_many(expected)

        result = list(multimap.get_many(*(k for k, v in expected)))
        assert len(result) == len(expected)
        for result_values, expected_item in zip(result, expected):
            expected_key, expected_values = expected_item
            assert sorted(result_values) == sorted(expected_values)

    def test_custom_cast_function(self, redis_client, MultiMapWithTTLClass):
        shared = MultiMapWithTTLClass(redis_client, 'multimap', cast_fn=lambda x: 'vai:{}'.format(int(x)))
        shared.add('a', 10, 20, 30)
        assert list(shared.get('a')) == ['vai:10', 'vai:20', 'vai:30']

    def test_delete(self, redis_client, MultiMapWithTTLClass):
        shared = MultiMapWithTTLClass(redis_client, 'multimap')
        shared.add('a', 10)
        shared.delete('a')
        assert list(shared.get('a')) == []

    def test_delete_multiple(self, redis_client, MultiMapWithTTLClass):
        shared = MultiMapWithTTLClass(redis_client, 'multimap')
        shared.add('a', 10)
        shared.add('b', 20)
        shared.delete('a', 'b')
        assert list(shared.get('a')) == []
        assert list(shared.get('b')) == []


@pytest.mark.freeze_time
def test_remove_old_score(multimap, freezer):
    multimap.add('a', 1, 2)
    freezer.tick(delta=timedelta(seconds=9))
    assert sorted(multimap.get('a')) == [1, 2]

    multimap.add('a', 3)
    assert sorted(multimap.get('a')) == [1, 2, 3]

    freezer.tick()
    multimap.add('a', 4)
    assert sorted(multimap.get('a')) == [3, 4]


@pytest.mark.freeze_time
def test_remove_old_score_on_read(multimap, freezer):
    multimap.add('a', 1)
    freezer.tick(delta=timedelta(seconds=9))
    assert sorted(multimap.get('a')) == [1]

    multimap.add('a', 2)
    assert sorted(multimap.get('a')) == [1, 2]

    freezer.tick()
    assert sorted(multimap.get('a')) == [2]


def test_remove_key_from_ttl(multimap):
    multimap.ttl = 1
    multimap.add('a', 1)
    time.sleep(1.1)
    assert sorted(multimap.get('a')) == []
