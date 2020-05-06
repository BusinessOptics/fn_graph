import hashlib
import inspect
import json
import pickle
from io import BytesIO
from logging import getLogger
from pathlib import Path


log = getLogger(__name__)


def fn_value(fn):
    if getattr(fn, "_is_fn_graph_link", False):
        return ",".join(inspect.signature(fn).parameters.keys())
    else:
        return inspect.getsource(fn)


def hash_fn(composer, key, use_id=False):
    value = (
        composer._parameters[key][1]
        if key in composer._parameters
        else composer._functions[key]
    )

    log.debug(f"Fn Value %s: %s", key, value)

    if use_id:
        return id(value)

    if callable(value):
        buffer = fn_value(value).encode("utf-8")
        return hashlib.sha256(buffer).digest()
    else:
        buffer = BytesIO()
        pickle.dump(value, buffer)
        return hashlib.sha256(buffer.getvalue()).digest()


class NullCache:
    """
    Performs no caching.

    Used as a base class for other caches to indicate that they all have the same signature.
    """

    def valid(self, composer, key):
        return False

    def get(self, composer, key):
        pass

    def set(self, composer, key, value):
        pass

    def invalidate(self, composer, key):
        pass


class SimpleCache(NullCache):
    """
    Stores results in memory, performs no automatic invalidation.

    Set hash_parameters=False to only check the object identities of parameters
    when checking cache validity. This is slightly less robust to buggy usage
    but is much faster for big parameter objects.

    DO NOT USE THIS IN DEVELOPMENT OR A NOTEBOOK!
    """

    def __init__(self, hash_parameters=False):
        self.cache = {}
        self.hashes = {}
        self.hash_parameters = hash_parameters

    def _hash(self, composer, key):
        return hash_fn(composer, key, use_id=not self.hash_parameters)

    def valid(self, composer, key):
        log.debug("Length %s", len(composer._functions))
        if key in self.hashes:
            current_hash = self._hash(composer, key)
            stored_hash = self.hashes[key]
            matches = current_hash == stored_hash
            log.debug(f"hash test {key} {matches}: {current_hash} {stored_hash}")
            return matches
        else:
            log.debug(f"cache test {key} {key in self.cache}")
            return key in self.cache

    def get(self, composer, key):
        return self.cache[key]

    def set(self, composer, key, value):
        self.cache[key] = value
        if key in composer.parameters():
            self.hashes[key] = self._hash(composer, key)

    def invalidate(self, composer, key):

        if key in self.cache:
            del self.cache[key]

        if key in self.hashes:
            del self.hashes[key]


class DevelopmentCache(NullCache):
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
        pickle_file_path = self.cache_root / f"{key}.data"
        info_file_path = self.cache_root / f"{key}.info.json"
        fn_hash_path = self.cache_root / f"{key}.fn.hash"

        exists = pickle_file_path.exists() and info_file_path.exists()

        log.debug(
            "Checking development cache '%s' for key '%s': exists = %s",
            self.name,
            key,
            exists,
        )

        if not exists:
            return False

        current_hash = hash_fn(composer, key)
        with open(fn_hash_path, "rb") as f:
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

        pickle_file_path = self.cache_root / f"{key}.data"
        info_file_path = self.cache_root / f"{key}.info.json"

        with open(info_file_path, "r") as f:
            information = json.load(f)

        format = information["format"]

        with open(pickle_file_path, "rb") as f:
            if format == "pickle":
                return pickle.load(f)
            elif format == "pandas-parquet":
                import pandas as pd

                return pd.read_parquet(f)
            else:
                raise Exception(f"Unknown caching fn_graph format: {format}")

    def set(self, composer, key, value):

        log.debug("Writing to development cache '%s' for key '%s'", self.name, key)
        pickle_file_path = self.cache_root / f"{key}.data"
        info_file_path = self.cache_root / f"{key}.info.json"
        fn_hash_path = self.cache_root / f"{key}.fn.hash"

        # This is a low-fi way to checK the type without adding requirements
        # I am concerned it is fragile
        if str(type(value)) == "<class 'pandas.core.frame.DataFrame'>":
            format = "pandas-parquet"
        else:
            format = "pickle"

        saved = False
        if format == "pandas-parquet":
            try:
                with open(pickle_file_path, "wb") as f:
                    value.to_parquet(f)
                saved = True
            except:
                saved = False

        if not saved:
            format = "pickle"
            with open(pickle_file_path, "wb") as f:
                pickle.dump(value, f)

        with open(fn_hash_path, "wb") as f:
            f.write(hash_fn(composer, key))

        with open(info_file_path, "w") as f:
            json.dump({"format": format}, f)

    def invalidate(self, composer, key):
        log.debug("Invalidation in development cache '%s' for key '%s'", self.name, key)
        paths = [
            self.cache_root / f"{key}.data",
            self.cache_root / f"{key}.fn.hash",
            self.cache_root / f"{key}.info.json",
        ]

        for path in paths:
            if path.exists():
                path.unlink()
