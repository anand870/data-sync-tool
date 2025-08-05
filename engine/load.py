from typing import List
from core.query import Filter
from core.query import Query
from core.config import MD5_SUM_HASH, HASH_MD5_HASH
from typing import Literal
from core.query import RowHashMeta, Field
from typing import Union
from core.config import StateConfig, SinkConfig, SourceConfig, Block, ReconciliationConfig
from adapters.base import Adapter
from engine.query_builder import build_joins_from_config, build_filters_from_config, build_table_from_config

def build_select_from_config(config: Union[SourceConfig, SinkConfig], strategy, fields=None):
    # Build filters
    select = []
    partition_column = config.meta_columns.partition_column
    hash_column = config.meta_columns.partition_column
    if fields is None:
        fields = [f.column for f in config.fields]
    for field_cfg in config.fields or []:
        if field_cfg.column in fields or field_cfg.alias in fields:
            select.append(Field(
                expr=field_cfg.column,
                alias=field_cfg.alias
            ))
    if not hash_column:
        select.append(Field(
            expr="",
            alias="hash__",
            type="rowhash",
            metadata=RowHashMeta(
                strategy=strategy,
                fields=[Field(expr=f.column) for f in config.fields if f.track]
            )
        ))
    else:
        select.append(
            Field(
                expr=hash_column,
                alias="hash__",
                type="column"
            )
        )        
    return select
    
def build_data_block_fetch_query(config: Union[SourceConfig, SinkConfig, StateConfig], block: Block, strategy, fields=None):
    # get min/max of partition field from source
    partition_column = config.meta_columns.partition_column
    filters: List[Filter]= build_filters_from_config(config)
    filters.extend([
        Filter(column=partition_column, operator=">=", value=block.start),
        Filter(column=partition_column, operator="<", value=block.end),
    ])
    return Query(
        select = build_select_from_config(config, strategy, fields=fields),
        table= build_table_from_config(config),
        joins= build_joins_from_config(config),
        filters= filters
    )


def transform_row(row, fields):
    """
    Transform a source row based on sink field configurations.
    
    Args:
        row: Source data row
        fields: List of field configurations from sink
        
    Returns:
        Dictionary containing transformed data
    """
    import jinja2
    from core.config import parse_lambda_from_string
    
    transformed = {}
    
    for field in fields:
        column = field.column
        source_col = field.source_column
        
        # Skip external sources for now
        if field.source:
            continue
            
        # Handle different transformation types
        if source_col is None:
            # Direct column mapping
            transformed[column] = row.get(field.alias or column)
        elif isinstance(source_col, str):
            if source_col.startswith('TMPL('):
                # Jinja template
                template_str = source_col[5:-1]  # Remove TMPL() wrapper
                template = jinja2.Template(template_str)
                # Replace dots with double underscores in variable names for Jinja
                context = {k.replace('.', '__'): v for k, v in row.items()}
                transformed[column] = template.render(**context)
            elif source_col.strip().startswith('lambda'):
                # Lambda function
                lambda_fn = parse_lambda_from_string(source_col)
                transformed[column] = lambda_fn(row)
            else:
                # Regular column mapping
                transformed[column] = row.get(source_col)
        else:
            # Handle callable source_column (already parsed)
            transformed[column] = source_col(row)
                
    return transformed

def categorize_rows(src_rows, sink_rows, src_unique_columns, sink_unique_columns):
    """
    Categorize rows as added, modified, or deleted based on unique keys.
    
    Args:
        src_rows: List of rows from source
        sink_rows: List of rows from sink
        unique_columns: List of column names that form the unique key
        
    Returns:
        Tuple of (added_rows, modified_rows, deleted_rows)
    """
    # Create dictionaries for efficient lookup
    src_dict = {}
    sink_dict = {}
    
    for row in src_rows:
        key_values = tuple(row.get(col) for col in src_unique_columns)
        src_dict[key_values] = row
    
    for row in sink_rows:
        key_values = tuple(row.get(col) for col in src_unique_columns)
        sink_dict[key_values] = row
    
    # Categorize rows
    added_rows = []
    modified_rows = []
    deleted_rows = []
    
    # Find added and modified rows
    for key, src_row in src_dict.items():
        if key not in sink_dict:
            added_rows.append(src_row)
        elif src_row.get('hash__') != sink_dict[key].get('hash__'):
            modified_rows.append(src_row)
    
    # Find deleted rows
    for key, sink_row in sink_dict.items():
        if key not in src_dict:
            deleted_rows.append(sink_row)
            
    return added_rows, modified_rows, deleted_rows


def load_data_block(source: Adapter, sink: Adapter, block: Block, r_config: ReconciliationConfig):
    """
    Compare source and sink rows based on unique keys and apply transformations.
    
    Args:
        source: Source adapter
        sink: Sink adapter
        block: Data block to process
        r_config: Reconciliation configuration
        
    Returns:
        Dictionary containing added, modified, and deleted rows with transformations applied
    """
    source_config: SinkConfig = source.adapter_config
    sink_config: SinkConfig = sink.adapter_config
    
    # Fetch data from source and sink
    source_query: Query = build_data_block_fetch_query(source_config, block, r_config.strategy, fields=None)
    
    # Fetch only hash_column and unique keys from sink
    fields = []+sink_config.meta_columns.unique_columns
    if sink_config.meta_columns.hash_column:
        fields.append(sink_config.meta_columns.hash_column)
    sink_query: Query = build_data_block_fetch_query(sink_config, block, r_config.strategy, fields=fields)
    
    src_rows = source.fetch(source_query, op_name="data_fetch")
    sink_rows = sink.fetch(sink_query, op_name="data_fetch")

    # Get unique columns from sink configuration
    src_unique_columns = source_config.meta_columns.unique_columns
    sink_unique_columns = sink_config.meta_columns.unique_columns
    
    # Categorize rows
    added_rows, modified_rows, deleted_rows = categorize_rows(src_rows, sink_rows, src_unique_columns, sink_unique_columns)
    
    # Apply transformations to added and modified rows
    transformed_added = [transform_row(row, sink_config.fields) for row in added_rows]
    transformed_modified = [transform_row(row, sink_config.fields) for row in modified_rows]

    sink.load_data({"added":transformed_added,"modified":transformed_modified,"deleted":deleted_rows}, r_config)
    
    
    
def load_data_blocks(pipeline: 'Pipeline', r_config: ReconciliationConfig, blocks: List[Block], statuses: List[str]):
    source, sink = pipeline.source, pipeline.sink
    sourcestate, sinkstate = pipeline.sourcestate, pipeline.sinkstate
    for block, status in zip(blocks, statuses):
        if status in ('A', 'M'):
            source.insert(block)
