from dataclasses import dataclass, field
from inspect import signature
from typing import Any, Dict, Iterable, List, Optional

from shared.utils import normalize_id
from syncs.games import sync as games_sync
from syncs.movies import sync as movies_sync
from syncs.music import sync as music_sync
from syncs.books import sync as books_sync


@dataclass
class TargetAdapter:
    name: str
    module: object
    _run_fn: Any = field(init=False, repr=False)
    _run_param_names: List[str] = field(init=False, repr=False)

    def __post_init__(self):
        self._run_fn = getattr(self.module, "run_sync")
        self._run_param_names = list(signature(self._run_fn).parameters.keys())

    def validate_environment(self) -> bool:
        return getattr(self.module, "validate_environment")()

    def run_sync(self, **options):
        filtered = {
            key: options[key]
            for key in self._run_param_names
            if key in options
        }
        return self._run_fn(**filtered)

    def database_ids(self) -> List[str]:
        ids_fn = getattr(self.module, "get_database_ids", None)
        if not ids_fn:
            return []
        return ids_fn()


_TARGETS: Dict[str, TargetAdapter] = {
    "games": TargetAdapter(name="games", module=games_sync),
    "movies": TargetAdapter(name="movies", module=movies_sync),
    "music": TargetAdapter(name="music", module=music_sync),
    "books": TargetAdapter(name="books", module=books_sync),
}


def available_targets() -> List[str]:
    return list(_TARGETS.keys())


def get_target(name: str) -> TargetAdapter:
    return _TARGETS[name]


def default_target() -> str:
    return next(iter(_TARGETS))


def find_target_for_page(page: Dict) -> Optional[TargetAdapter]:
    parent = page.get("parent", {})
    database_id = parent.get("database_id")
    return find_target_for_database_id(database_id)


def iter_targets() -> Iterable[TargetAdapter]:
    return _TARGETS.values()


def find_target_for_database_id(database_id: Optional[str]) -> Optional[TargetAdapter]:
    if not database_id:
        return None
    normalized = normalize_id(database_id)
    if not normalized:
        return None
    for adapter in _TARGETS.values():
        if normalized in adapter.database_ids():
            return adapter
    return None


