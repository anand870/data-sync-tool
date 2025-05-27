import json
from dataclasses import dataclass, field, asdict
from typing import List, Any, Optional, Union

@dataclass
class BlockHashMeta:
    partition_column: str
    strategy: str
    partition_column_type: str
    hash_column: Optional[str] = None
    order_column: Optional[str] = None
    fields: Optional[List['Field']] = field(default_factory=list)

@dataclass
class BlockNameMeta:
    level: int
    partition_column: str
    strategy: str
    partition_column_type: str
    intervals: Optional[List[int]]

   

@dataclass
class Filter:
    column: str
    operator: str
    value: Any

@dataclass
class Field:
    expr: str
    alias: Optional[str] = None
    type: str = 'column'        # 'column', 'blockhash', 'blockname'
    metadata: Optional[Union[BlockHashMeta, BlockNameMeta]] = None

@dataclass
class Table:
    table: str
    schema: Optional[str] = None
    alias: Optional[str] = None

@dataclass
class Join:
    table: str
    on: str
    type: str = 'inner'           # 'inner', 'left', 'right', 'full'
    alias: Optional[str] = None

@dataclass
class Query:
    select: List[Field]
    table: Table
    joins: Optional[List[Join]] = field(default_factory=list)
    filters: Optional[List[Filter]] = field(default_factory=list)
    group_by: Optional[List[Field]] = field(default_factory=list)
    order_by: Optional[List[str]] = field(default_factory=list)
    limit: Optional[int] = None

    @property
    def json(self) -> str:
        """
        Return the Query object and its nested structure as a JSON string.
        """
        return json.dumps(asdict(self), default=str)