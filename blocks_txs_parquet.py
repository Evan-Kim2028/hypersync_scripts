import asyncio
import hypersync
import polars as pl
import time

from hypersync import ColumnMapping, DataType, TransactionField, BlockField, TraceField, TransactionSelection


async def historical_blocks_txs_sync():
    """
    Use hypersync to query blocks and transactions and write to a LanceDB table. Assumes existence of a previous LanceDB table to
    query for the latest block number to resume querying.
    """
    # hypersync client
    client = hypersync.HypersyncClient(
        hypersync.ClientConfig(url='http://1.backup.hypersync.xyz')
    )

    # set to_block and from_block to query the desired block range.
    to_block: int = 20050000
    # fetch the latest block number from the blocks table. Alternatively can forgo the database query and set the to_block manually
    from_block: int = 20000000

    # # add +/-1 to the block range because the query is exclusive to the block number
    query = hypersync.Query(
        from_block=from_block-1,
        to_block=to_block+1,
        include_all_blocks=True,
        transactions=[TransactionSelection()],
        field_selection=hypersync.FieldSelection(
            block=[e.value for e in BlockField],
            transaction=[e.value for e in TransactionField],
        )
    )
    # Setting this number lower reduces client sync console error messages.
    query.max_num_transactions = 1_000  # for troubleshooting

    config = hypersync.StreamConfig(
        hex_output=hypersync.HexOutput.PREFIXED,
        column_mapping=ColumnMapping(
            transaction={
                TransactionField.GAS_USED: DataType.FLOAT64,
                TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                TransactionField.GAS_PRICE: DataType.FLOAT64,
                TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                TransactionField.NONCE: DataType.INT64,
                TransactionField.GAS: DataType.FLOAT64,
                TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                TransactionField.VALUE: DataType.FLOAT64,
            },
            block={
                BlockField.GAS_LIMIT: DataType.FLOAT64,
                BlockField.GAS_USED: DataType.FLOAT64,
                BlockField.SIZE: DataType.FLOAT64,
                BlockField.BLOB_GAS_USED: DataType.FLOAT64,
                BlockField.EXCESS_BLOB_GAS: DataType.FLOAT64,
                BlockField.BASE_FEE_PER_GAS: DataType.FLOAT64,
                BlockField.TIMESTAMP: DataType.INT64,
            }
        )
    )

    return await client.collect_parquet('data', query, config)

start_time = time.time()
data = asyncio.run(historical_blocks_txs_sync())
end_time = time.time()

print(f"Time taken: {end_time - start_time}")


# Convert ArrowResponse to an Arrow Table
txs_df = pl.scan_parquet('data/transactions.parquet')
blocks_df = pl.scan_parquet('data/blocks.parquet')


txs_blocks_joined = txs_df.join(
    blocks_df,
    on='block_number',
    how='left',
    coalesce=True,
    suffix='_block'
).unique()

txs_blocks_joined_shortened = txs_blocks_joined.select(
    'block_number',
    'extra_data',
    'base_fee_per_gas',
    'timestamp',
    'hash',
    'from',
    'to',
    'gas',
    'transaction_index',
    'gas_price',
    'effective_gas_price',
    'gas_used',
    'cumulative_gas_used',
    'max_fee_per_gas',
    'max_priority_fee_per_gas',
).collect()

# AVAILABLE COLUMNS
# ['block_hash', 'block_number', 'from', 'gas', 'gas_price', 'hash', 'input', 'nonce', 'to',
# 'transaction_index', 'value', 'v', 'r', 's', 'max_priority_fee_per_gas', 'max_fee_per_gas', 'chain_id',
# 'cumulative_gas_used', 'effective_gas_price', 'gas_used', 'contract_address', 'logs_bloom', 'type', 'root',
# 'status', 'y_parity', 'access_list', 'l1_fee', 'l1_gas_price', 'l1_gas_used', 'l1_fee_scalar', 'gas_used_for_l1',
# 'max_fee_per_blob_gas', 'blob_versioned_hashes', 'hash_right', 'parent_hash', 'nonce_right', 'sha3_uncles', 'logs_bloom_right',
# 'transactions_root', 'state_root', 'receipts_root', 'miner', 'difficulty', 'total_difficulty', 'extra_data', 'size', 'gas_limit',
# 'gas_used_right', 'timestamp', 'uncles', 'base_fee_per_gas', 'blob_gas_used', 'excess_blob_gas', 'parent_beacon_block_root', 'withdrawals_root',
# 'withdrawals', 'l1_block_number', 'send_count', 'send_root', 'mix_hash']

print(txs_blocks_joined.columns)
print(txs_blocks_joined_shortened.head(15))

print('counting nulls')
print(txs_blocks_joined_shortened.select('block_number', 'base_fee_per_gas').unique().group_by(
    'base_fee_per_gas').agg(pl.len().alias('count')).sort(by='count', descending=True))
# print(txs_blocks_joined_shortened.select('block_number', 'base_fee_per_gas').unique().with_columns(
#     pl.col('base_fee_per_gas').is_null().len()).shape)
# print(txs_blocks_joined_shortened.select('block_number', 'base_fee_per_gas').unique().with_columns(
#     pl.col('base_fee_per_gas').is_not_null().len()).shape)
