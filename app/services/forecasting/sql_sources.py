from __future__ import annotations

from .sql_aggregations import AggregationQueryBuilder, QueryBuilder
from .sql_sources_query import SourceQuerySqlMixin
from .sql_sources_registry import SourceQueryRegistryMixin


class SourceQueryBuilder(SourceQueryRegistryMixin, SourceQuerySqlMixin, QueryBuilder):
    def __init__(self, aggregations: AggregationQueryBuilder, hook_resolver=None) -> None:
        super().__init__(hook_resolver)
        self._aggregations = aggregations
