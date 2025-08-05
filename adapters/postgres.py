from core.config import ReconciliationConfig
from core.query import RowHashMeta, BlockHashMeta, BlockNameMeta
from typing import Dict, Tuple
import psycopg2

from core.config import HASH_MD5_HASH, MD5_SUM_HASH
from .base import Adapter
from core.query import Query, Field
from engine.sql_builder import SqlBuilder

class PostgresAdapter(Adapter):
    def connect(self):
        cfg = self.store_config
        # import pdb;pdb.set_trace()
        self.conn = psycopg2.connect(
            host=cfg['host'], port=cfg['port'],
            user=cfg['username'], password=cfg['password'], dbname=cfg['database']
        )
        self.cursor = self.conn.cursor()

    def _build_group_name_expr(self, field: Field) -> str:
        metadata: BlockNameMeta = field.metadata
        level = metadata.level
        intervals = metadata.intervals
        partition_column = metadata.partition_column
        partition_column_type = metadata.partition_column_type

        if partition_column_type == "int":
            # For integer partition columns, we divide by the interval
            segments = []
            for idx in range(level):
                fct = intervals[idx]
                if idx == 0:
                    expr = f"FLOOR({partition_column} / {intervals[idx]})"
                else:
                    prev = intervals[idx-1]
                    expr = f"FLOOR(mod({partition_column}, {prev}) / {intervals[idx]})"
                segments.append(f"{expr}::text")
            return " || '-' || ".join(segments)

        elif partition_column_type == "datetime":
            segments = []
            for idx in range(level):
                fct = intervals[idx]
                if idx == 0:
                    expr = f"FLOOR(EXTRACT(EPOCH FROM {partition_column}) / {fct})"
                else:
                    prev = intervals[idx-1]
                    expr = f"FLOOR(((EXTRACT(EPOCH FROM {partition_column})::bigint) %% {prev}) / {fct})"
                    
                segments.append(f"{expr}::text")
            return " || '-' || ".join(segments)
        else:
            raise ValueError(f"Unsupported partition type: {partition_column}")

    def _build_rowhash_expr(self, field: Field) -> str:
        metadata: RowHashMeta = field.metadata
        expr=""
        if metadata.hash_column:
            expr = metadata.hash_column
        elif metadata.strategy == MD5_SUM_HASH:
            concat = ",".join([f"{x.expr}" for x in metadata.fields])
            expr = f"(('x'||substr(md5(CONCAT({concat})),1,8))::bit(32)::int)"
        elif metadata.strategy == HASH_MD5_HASH:
            concat = ",".join([f"{x.expr}" for x in metadata.fields])
            expr = f"md5(CONCAT({concat}))"
        return expr
    
    def _build_blockhash_expr(self, field: Field):
        metadata: BlockHashMeta = field.metadata
        inner_expr = self._build_rowhash_expr(field)
        if metadata.strategy == MD5_SUM_HASH:
            expr = f"sum({inner_expr}::numeric)"
        elif metadata.strategy == HASH_MD5_HASH:
            expr = f"md5(string_agg({inner_expr},',' order by {metadata.order_column}))"
        return expr


    def _rewrite_query(self, query: Query) -> Query:
        rewritten = []
        for f in query.select:
            if f.type == 'blockhash':
                expr = self._build_blockhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            elif f.type == "blockname":
                expr = self._build_group_name_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            elif f.type == "rowhash":
                expr = self._build_rowhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            else:
                rewritten.append(f)
        query.select = rewritten

        # Ensure group_by fields are included in select
        # query.select.extend(query.group_by)
        return query

    def _build_sql(self, query: Query) -> Tuple[str, list]:
        q = self._rewrite_query(query)
        return SqlBuilder.build(q)

    def fetch(self, query: Query, op_name: str="") -> list:
        # import pdb;pdb.set_trace()
        sql, params = self._build_sql(query)
        # import pdb;pdb.set_trace()
        # print(sql,params)
        self.cursor.execute(sql, params)
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]
    
    def fetch_one(self, query: Query, op_name: str="") -> Dict:
        return self.fetch(query, op_name=op_name)[0]

    def execute(self, sql: str, params=None):
        self.cursor.execute(sql, params or [])
        self.conn.commit()
    
    def load_data(self, data, r_config: ReconciliationConfig):
        pass
        

    # def insert_or_update(self, table: str, row: dict):
    #     cols, vals = zip(*row.items())
    #     ph = ','.join(['%s'] * len(cols))
    #     up = ','.join([f"{c}=EXCLUDED.{c}" for c in cols])
    #     stmt = (
    #         f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) "
    #         f"ON CONFLICT ({','.join(self.config.get('unique_keys', cols))}) DO UPDATE SET {up};"
    #     )
    #     self.execute(stmt, vals)

    def close(self):
        self.cursor.close()
        self.conn.close()
