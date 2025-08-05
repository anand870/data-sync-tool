from typing import Dict
import psycopg2
from .base import Adapter
from core.query import Query, Field
from engine.sql_builder import SqlBuilder

class PostgresAdapter(Adapter):
    def connect(self):
        cfg = self.store_config
        self.conn = psycopg2.connect(
            host=cfg['host'], port=cfg['port'],
            user=cfg['username'], password=cfg['password'], dbname=cfg['database']
        )
        self.cursor = self.conn.cursor()

    def _build_group_name_expr(self, group_field: Field) -> str:
        part_type, interval, factor, uuid_len = group_field.hash_fields
        if part_type == 'datetime':
            factors = []
            cur = int(interval)
            while cur >= 1:
                factors.append(cur)
                cur = cur // int(factor)
                if cur == 0:
                    factors.append(1)
                    break
            segments = []
            for idx, fct in enumerate(factors):
                if idx == 0:
                    expr = f"FLOOR(EXTRACT(EPOCH FROM {group_field.expr}) / {fct})"
                else:
                    prev = factors[idx-1]
                    expr = f"FLOOR((EXTRACT(EPOCH FROM {group_field.expr}) % {prev}) / {fct})"
                segments.append(f"LPAD(({expr})::text, 2, '0')")
            return " || '-' || ".join(segments)
        elif part_type == 'int':
            factors = []
            cur = int(interval)
            while cur >= 1:
                factors.append(cur)
                cur = cur // int(factor)
                if cur == 0:
                    factors.append(1)
                    break
            segments = []
            base = group_field.expr
            for idx, fct in enumerate(factors):
                if idx == 0:
                    expr = f"FLOOR({base} / {fct})"
                else:
                    prev = factors[idx-1]
                    expr = f"FLOOR(({base} % {prev}) / {fct})"
                segments.append(f"LPAD(({expr})::text, 2, '0')")
            return " || '-' || ".join(segments)
        elif part_type == 'uuid':
            return f"SUBSTR({group_field.expr},1,{uuid_len})"
        else:
            raise ValueError(f"Unsupported partition type: {part_type}")

    def _rewrite_query(self, query: Query) -> Query:
        rewritten = []
        for f in query.select:
            if f.type == 'hash':
                concat = "||'|'||".join(f.hash_fields)
                expr = f"('x'||substr(md5({concat}),1,8))::bit(32)::int"
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            else:
                rewritten.append(f)
        query.select = rewritten

        new_group = []
        for g in query.group_by:
            if g.type == 'group_name':
                expr = self._build_group_name_expr(g)
                new_group.append(Field(expr=expr, type='column'))
            else:
                new_group.append(g)
        query.group_by = new_group

        # Ensure group_by fields are included in select
        query.select.extend(query.group_by)
        return query

    def _build_sql(self, query: Query) -> str:
        rewritten_query = self._rewrite_query(query)
        return SqlBuilder.build(rewritten_query)

    def fetch(self, query: Query, query_name: str) -> list:
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]
    
    def fetch_one(self, query: Query, query_name) -> Dict:
        return self.fetch(query, query_name)[0]

    def execute(self, sql: str, params=None):
        self.cursor.execute(sql, params or ())
        self.conn.commit()

    def insert_or_update(self, table: str, row: dict):
        cols, vals = zip(*row.items())
        ph = ','.join(['%s'] * len(cols))
        up = ','.join([f"{c}=EXCLUDED.{c}" for c in cols])
        sql = (
            f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) "
            f"ON CONFLICT ({','.join(self.config.get('unique_keys', cols))}) DO UPDATE SET {up};"
        )
        self.execute(sql, vals)

    def close(self):
        self.cursor.close()
        self.conn.close()