from __future__ import annotations
import fnmatch


class FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._commands = []

    def __getattr__(self, name):
        def _enqueue(*args, **kwargs):
            self._commands.append((name, args, kwargs))
            return self

        return _enqueue

    def execute(self):
        results = []
        for name, args, kwargs in self._commands:
            method = getattr(self._redis, name)
            results.append(method(*args, **kwargs))
        self._commands.clear()
        return results


class FakeRedis:
    def __init__(self):
        self._sets = {}
        self._lists = {}
        self._hashes = {}
        self._sorted_sets = {}
        self._strings = {}

    def clear(self):
        self._sets.clear()
        self._lists.clear()
        self._hashes.clear()
        self._sorted_sets.clear()
        self._strings.clear()

    def pipeline(self):
        return FakePipeline(self)

    def expire(self, key, _seconds):
        return 1 if key in self._all_keys() else 0

    def _all_keys(self):
        return set(self._sets) | set(self._lists) | set(self._hashes) | set(self._sorted_sets) | set(self._strings)

    def keys(self, pattern="*"):
        return [key for key in self._all_keys() if fnmatch.fnmatch(str(key), pattern)]

    def scan_iter(self, match=None):
        pattern = match or "*"
        for key in self.keys(pattern):
            yield key

    def delete(self, *keys):
        removed = 0
        for key in keys:
            removed += int(self._sets.pop(key, None) is not None)
            removed += int(self._lists.pop(key, None) is not None)
            removed += int(self._hashes.pop(key, None) is not None)
            removed += int(self._sorted_sets.pop(key, None) is not None)
            removed += int(self._strings.pop(key, None) is not None)
        return removed

    # String ops
    def setex(self, key, _seconds, value):
        self._strings[key] = value
        return True

    def get(self, key):
        return self._strings.get(key)

    def set(self, key, value):
        self._strings[key] = value
        return True

    # Set ops
    def sadd(self, key, *values):
        members = self._sets.setdefault(key, set())
        before = len(members)
        for value in values:
            members.add(value)
        return len(members) - before

    def smembers(self, key):
        return set(self._sets.get(key, set()))

    def sismember(self, key, value):
        return value in self._sets.get(key, set())

    def srem(self, key, *values):
        members = self._sets.get(key, set())
        removed = 0
        for value in values:
            if value in members:
                members.remove(value)
                removed += 1
        return removed

    def scard(self, key):
        return len(self._sets.get(key, set()))

    # List ops
    def rpush(self, key, *values):
        arr = self._lists.setdefault(key, [])
        arr.extend(values)
        return len(arr)

    def lpop(self, key):
        arr = self._lists.get(key, [])
        if not arr:
            return None
        return arr.pop(0)

    def llen(self, key):
        return len(self._lists.get(key, []))

    def lrange(self, key, start, end):
        arr = self._lists.get(key, [])
        if end == -1:
            end = len(arr) - 1
        if start < 0:
            start = max(len(arr) + start, 0)
        if end < 0:
            end = len(arr) + end
        if end < start:
            return []
        return arr[start:end + 1]

    def lrem(self, key, count, value):
        arr = self._lists.get(key, [])
        if not arr:
            return 0

        removed = 0
        if count == 0:
            count = len(arr)

        if count > 0:
            i = 0
            while i < len(arr) and removed < count:
                if arr[i] == value:
                    arr.pop(i)
                    removed += 1
                else:
                    i += 1
            return removed

        target = -count
        i = len(arr) - 1
        while i >= 0 and removed < target:
            if arr[i] == value:
                arr.pop(i)
                removed += 1
            i -= 1
        return removed

    # Hash ops
    def hset(self, key, field=None, value=None, mapping=None):
        h = self._hashes.setdefault(key, {})
        added = 0
        if mapping is not None:
            for map_field, map_value in mapping.items():
                if map_field not in h:
                    added += 1
                h[map_field] = str(map_value)
            return added
        if field is None:
            return 0
        if field not in h:
            added = 1
        h[field] = str(value)
        return added

    def hget(self, key, field):
        return self._hashes.get(key, {}).get(field)

    def hgetall(self, key):
        return dict(self._hashes.get(key, {}))

    def hlen(self, key):
        return len(self._hashes.get(key, {}))

    def hvals(self, key):
        return list(self._hashes.get(key, {}).values())

    def hmget(self, key, fields):
        h = self._hashes.get(key, {})
        return [h.get(field) for field in fields]

    def hdel(self, key, *fields):
        h = self._hashes.get(key, {})
        removed = 0
        for field in fields:
            if field in h:
                del h[field]
                removed += 1
        return removed

    def hincrby(self, key, field, amount=1):
        h = self._hashes.setdefault(key, {})
        current = int(h.get(field, 0) or 0)
        next_value = current + int(amount)
        h[field] = str(next_value)
        return next_value

    # Sorted set ops
    def zadd(self, key, mapping):
        zset = self._sorted_sets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in zset:
                added += 1
            zset[member] = float(score)
        return added

    def _zsorted(self, key, reverse=False):
        zset = self._sorted_sets.get(key, {})
        return sorted(
            zset.items(),
            key=lambda item: (item[1], item[0]),
            reverse=reverse,
        )

    def zrange(self, key, start, end, withscores=False):
        items = self._zsorted(key, reverse=False)
        if end == -1:
            end = len(items) - 1
        sliced = items[start:end + 1]
        if withscores:
            return sliced
        return [member for member, _score in sliced]

    def zrevrange(self, key, start, end, withscores=False):
        items = self._zsorted(key, reverse=True)
        if end == -1:
            end = len(items) - 1
        sliced = items[start:end + 1]
        if withscores:
            return sliced
        return [member for member, _score in sliced]

    def zrem(self, key, *members):
        zset = self._sorted_sets.get(key, {})
        removed = 0
        for member in members:
            if member in zset:
                del zset[member]
                removed += 1
        return removed

    def zremrangebyscore(self, key, min_score, max_score):
        zset = self._sorted_sets.get(key, {})
        if not zset:
            return 0

        def _to_bound(value, default):
            if isinstance(value, str):
                text = value.strip().lower()
                if text == "-inf":
                    return float("-inf")
                if text == "+inf":
                    return float("inf")
            if value is None:
                return default
            return float(value)

        low = _to_bound(min_score, float("-inf"))
        high = _to_bound(max_score, float("inf"))

        to_remove = [member for member, score in zset.items() if low <= float(score) <= high]
        for member in to_remove:
            del zset[member]
        return len(to_remove)

    def zcard(self, key):
        return len(self._sorted_sets.get(key, {}))

    def zscore(self, key, member):
        return self._sorted_sets.get(key, {}).get(member)
