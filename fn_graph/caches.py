import hashlib
import inspect
import json
import pandas as pd
import pickle
from datetime import datetime
from io import BytesIO
from logging import getLogger
from pathlib import Path
from typing import Union, Dict


PathorString = Union[str, Path]
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

    if use_id:
        log.debug(f"Fn Value on %s for value %s is ID", key, id(value))
        return id(value)

    if callable(value):
        log.debug("Fn Value on %s for value %r is callable", key, value)
        buffer = fn_value(value).encode("utf-8")
        return hashlib.sha256(buffer).digest()
    else:
        log.debug("Fn Value on %s for value %r is not callable", key, value)
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
        self.cache_root.mkdir(parents=True, exist_ok=True)

    @property
    def cache_root(self):
        return self.cache_dir / self.name

    def valid(self, composer, key):
        self.cache_root.mkdir(parents=True, exist_ok=True)
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

        current_hash = hash_fn(composer, key, False)
        with open(fn_hash_path, "rb") as f:
            previous_hash = f.read()

        if current_hash != previous_hash:

            log.debug(
                "Hash difference in cache '%s' for key '%s' is current %r vs previous %r",
                self.name,
                key,
                current_hash,
                previous_hash,
            )

            return False

        log.debug("Valid development cache '%s' for key '%s'", self.name, key)

        return True

    def get(self, composer, key):

        log.debug("Retrieving from development cache '%s' for key '%s'", self.name, key)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        pickle_file_path = self.cache_root / f"{key}.data"
        info_file_path = self.cache_root / f"{key}.info.json"

        with open(info_file_path, "r") as f:
            information = json.load(f)

        format = information["format"]

        with open(pickle_file_path, "rb") as f:
            if format == "pickle":
                return pickle.load(f)
            elif format == "pandas-parquet":
                return pd.read_parquet(f)
            else:
                raise Exception(f"Unknown caching fn_graph format: {format}")

    def set(self, composer, key, value):

        log.debug("Writing to development cache '%s' for key '%s'", self.name, key)
        self.cache_root.mkdir(parents=True, exist_ok=True)
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
        self.cache_root.mkdir(parents=True, exist_ok=True)
        paths = [
            self.cache_root / f"{key}.data",
            self.cache_root / f"{key}.fn.hash",
            self.cache_root / f"{key}.info.json",
        ]

        for path in paths:
            if path.exists():
                path.unlink()


class FuncOuputCache(NullCache):
    """
    Cache functionality to store all function outputs of a composer.

    IMPORTANT:
    For this to work the composer needs to set a paramater called funcoutput,
    that can either be 'save' or 'load'. 
    """

    def __init__(self, name):
        self.name = name
        self.df = '%Y-%m-%d'
        self.init_date = datetime.today().strftime(self.df)
        self.cache_parent = Path('data/func_out_cache')
        self.cache_dir = self.cache_parent / Path(self.init_date)
        
    @property
    def cache_root(self):
        return self.cache_dir
    
    @property
    def latest_saved_date(self):
        glob_date_pattern = '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
        dates = [datetime.strptime(
            x.name, self.df) for x in self.cache_parent.glob(glob_date_pattern
                )]
        if len(dates) > 0:
            dates.sort()
            return dates[-1].strftime(self.df)
        else:
            return None

    def get(self, composer, key):
        if self.latest_saved_date != datetime.today().strftime(self.df):
            log.error("The function output cache was created on a different date")
        log.debug(f"Retrieving function output {self.cache_dir / self.name / key}")
        data_folder_path = self.cache_dir / self.name / key
        info_file_path = data_folder_path / f"{key}.info.json"
        with open(info_file_path) as json_file:
            params = json.load(json_file)
        file_path = (data_folder_path / key).with_suffix('.parquet')
        try:
            if params['format'] ==  "<class 'pandas.core.series.Series'>":
                return pd.read_parquet(file_path).iloc[:, 0]
            elif params['format'] ==  "<class 'pandas.core.frame.DataFrame'>":
                return pd.read_parquet(file_path)
        except:
            raise Exception(f"Function output data not found: {key}")

    def set(self, composer, key, value):
        params = {}
        self.cache_root.mkdir(parents=True, exist_ok=True)
        log.debug(f"Saving function output {self.cache_dir / self.name / key}")
        data_folder_path = self.cache_dir / self.name / key
        data_folder_path.mkdir(parents=True, exist_ok=True)
        info_file_path = data_folder_path / f"{key}.info.json"
        file_path = (data_folder_path / key).with_suffix('.parquet')
        params["format"] = str(type(value))
        if type(value) == pd.core.frame.DataFrame:
            #parquet must have string column names
            value.columns = value.columns.map(str)
            value.to_parquet(file_path)
        elif type(value) == pd.core.series.Series:
            value.to_frame().to_parquet(file_path)
        else:
            raise Exception(f'Format not supported for {key}')
        with open(info_file_path, "w") as f:
            json.dump(params, f)