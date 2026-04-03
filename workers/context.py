from dataclasses import dataclass
from typing import Optional


@dataclass
class Context:
    correlation_id: str
    job_id: str
    source_id: Optional[str] = None

    def get(self, key: str, default=None):
        return getattr(self, key, default)
