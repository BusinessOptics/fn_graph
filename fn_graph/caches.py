import hashlib
import inspect
import pickle
from io import BytesIO
from logging import getLogger
from pathlib import Path

import networkx as nx

log = getLogger(__name__)


class NullCache:
    def find_invalid(self, composer, execution_graph):
        return execution_graph.nodes()

    def prepare_execution(self, composer, execution_graph):
        pass

    def has(self, composer, key):
        return False

    def get(self, composer, key):
        pass

    def set(self, composer, key, value):
        pass

    def invalidate(self, composer, key):
        pass


class SimpleCache:
    def __init__(self):
        self.cache = {}

    def find_invalid(self, composer, execution_graph):

        to_invalidate = set()
        for node in execution_graph.nodes():
            if not self.has(composer, node):
                to_invalidate.update(nx.descendants(execution_graph, node))
                to_invalidate.add(node)

        return to_invalidate

    def prepare_execution(self, composer, execution_graph):
        pass

    def has(self, composer, key):
        return key in self.cache

    def get(self, composer, key):
        return self.cache[key]

    def set(self, composer, key, value):
        self.cache[key] = value

    def invalidate(self, composer, key):
        del self.cache[key]


class DevelopmentCache:
    def __init__(self, name, cache_dir):
        self.name = name

        if cache_dir is None:
            cache_dir = ".fn_graph_cache"

        self.cache_dir = Path(cache_dir)
        self.cache_root = self.cache_dir / name
        self.cache_root.mkdir(parents=True, exist_ok=True)

    def find_invalid(self, composer, execution_graph):
        invalid = []
        for key in execution_graph.nodes():

            if not self.has(composer, key):
                invalid.append(key)
                continue

            current_hash = self._hash_fn(composer, key)
            with open(self.cache_root / f"{key}.fn.hash", "rb") as f:
                previous_hash = f.read()

            if current_hash != previous_hash:
                log.debug(
                    "Function change detected in development cache '%s' for key '%s'",
                    self.name,
                    key,
                )
                invalid.append(key)

        to_invalidate = set()
        for node in invalid:
            to_invalidate.update(nx.descendants(execution_graph, node))
            to_invalidate.add(node)

        return to_invalidate

    def prepare_execution(self, composer, execution_graph):
        log.debug("Preparing cache execution")
        # Invalidate all the items that have changed, or had ancestors change
        for key in self.find_invalid(composer, execution_graph):
            self.invalidate(composer, key)

    def has(self, composer, key):
        exists = (self.cache_root / f"{key}.pickle").exists()
        log.debug(
            "Checking development cache '%s' for key '%s': exists = %s",
            self.name,
            key,
            exists,
        )
        return exists

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
