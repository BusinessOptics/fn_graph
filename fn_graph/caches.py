import hashlib
import inspect
import pickle
from io import BytesIO
from logging import getLogger
from pathlib import Path

import networkx as nx

log = getLogger(__name__)


class NullCache:
    """
    Performs no caching
    """

    def valid(self, composer, key):
        return False

    def get(self, composer, key):
        pass

    def set(self, composer, key, value):
        pass

    def invalidate(self, composer, key):
        pass


class SimpleCache:
    """
    Stores results in memory, performs no automatic invalidation.

    DO NOT USE THIS IN DEVELOPMENT OR A NOTEBOOK!
    """

    def __init__(self):
        self.cache = {}

    def valid(self, composer, key):
        return key in self.cache

    def get(self, composer, key):
        return self.cache[key]

    def set(self, composer, key, value):
        self.cache[key] = value

    def invalidate(self, composer, key):
        if key in self.cache:
            del self.cache[key]


class DevelopmentCache:
    """
    Store cache on disk, analyses the coe for changes and performs automatic 
    invalidation.

    This is only for use during development! DO NOT USE THIS IN PRODUCTION!

    The analysis of code changes is limited, it assumes that all functions are 
    pure, and tht there have been no important changes in the outside environment, 
    like a file that has been changed,
    """

    def __init__(self, name, cache_dir):
        self.name = name

        if cache_dir is None:
            cache_dir = ".fn_graph_cache"

        self.cache_dir = Path(cache_dir)
        self.cache_root = self.cache_dir / name
        self.cache_root.mkdir(parents=True, exist_ok=True)

    def valid(self, composer, key):
        exists = (self.cache_root / f"{key}.pickle").exists()
        log.debug(
            "Checking development cache '%s' for key '%s': exists = %s",
            self.name,
            key,
            exists,
        )

        if not exists:
            return False

        current_hash = self._hash_fn(composer, key)
        with open(self.cache_root / f"{key}.fn.hash", "rb") as f:
            previous_hash = f.read()

        if current_hash != previous_hash:
            log.debug(
                "Function change detected in development cache '%s' for key '%s'",
                self.name,
                key,
            )
            return False

        return True

    def get(self, composer, key):
        log.debug("Retrieving from development cache '%s' for key '%s'", self.name, key)
        with open(self.cache_root / f"{key}.pickle", "rb") as f:
            return pickle.load(f)

    def set(self, composer, key, value):
        log.debug("Writing to development cache '%s' for key '%s'", self.name, key)

        with open(self.cache_root / f"{key}.fn.hash", "wb") as f:
            f.write(self._hash_fn(composer, key))

        with open(self.cache_root / f"{key}.pickle", "wb") as f:
            pickle.dump(value, f)

    def invalidate(self, composer, key):
        log.debug("Invalidation in development cache '%s' for key '%s'", self.name, key)
        paths = [self.cache_root / f"{key}.pickle", self.cache_root / f"{key}.fn.hash"]

        for path in paths:
            if path.exists():
                path.unlink()

    def _fn_value(self, composer, key):
        value = (
            composer._parameters[key]
            if key in composer._parameters
            else composer._functions[key]
        )
        if callable(value):
            return inspect.getsource(value)
        else:
            return str(value)

    def _hash_fn(self, composer, key):
        value = (
            composer._parameters[key]
            if key in composer._parameters
            else composer._functions[key]
        )

        if callable(value):
            buffer = inspect.getsource(value).encode("utf-8")
            return hashlib.sha256(buffer).digest()
        else:
            buffer = BytesIO()
            pickle.dump(value, buffer)
            return hashlib.sha256(buffer.getvalue()).digest()
