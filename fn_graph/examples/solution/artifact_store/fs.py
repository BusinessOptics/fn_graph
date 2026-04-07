import os
from pathlib import Path
from typing import Any

import cloudpickle

from .base import BaseArtifactStore


class LocalFSArtifactStore(BaseArtifactStore):
    def __init__(self, base_dir: str, run_id: str):
        self.base_dir = Path(base_dir)
        self.run_id = run_id
        self._run_dir = self.base_dir / run_id
        self._run_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self._run_dir / f"{key}.pkl"

    def put(self, key: str, value: Any) -> None:
        path = self._path(key)
        print(f"[LocalFSArtifactStore] writing {key} to {path}", flush=True)
        tmp_path = path.with_suffix(".tmp")
        data = cloudpickle.dumps(value, protocol=4)
        tmp_path.write_bytes(data)
        os.replace(tmp_path, path)
        print(f"[LocalFSArtifactStore] {key} written, size: {len(data)} bytes", flush=True)

    def get(self, key: str) -> Any:
        path = self._path(key)
        print(f"[LocalFSArtifactStore] loading {key} from {path}", flush=True)
        result = cloudpickle.loads(path.read_bytes())
        print(f"[LocalFSArtifactStore] {key} loaded, type: {type(result).__name__}", flush=True)
        return result

    def exists(self, key: str) -> bool:
        result = self._path(key).exists()
        print(f"[LocalFSArtifactStore] exists({key}): {result}", flush=True)
        return result

    def delete(self, key: str) -> None:
        path = self._path(key)
        if path.exists():
            path.unlink()

    def metadata(self, key: str) -> dict:
        stat = os.stat(self._path(key))
        return {"size": stat.st_size, "mtime": stat.st_mtime}
