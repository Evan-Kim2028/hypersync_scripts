import asyncio
import hypersync
import time

from hypersync import ColumnMapping, DataType, TransactionField, BlockField, TraceField, TransactionSelection, TraceSelection


async def historical_blocks_txs_sync():
    """
    Use hypersync to query blocks and transactions and write to a LanceDB table. Assumes existence of a previous LanceDB table to
    query for the latest block number to resume querying.
    """
    # hypersync client
    client = hypersync.HypersyncClient(
        hypersync.ClientConfig(url="http://167.235.0.227:2104"))

    # set to_block and from_block to query the desired block range.
    to_block: int = 20025000
    # fetch the latest block number from the blocks table. Alternatively can forgo the database query and set the to_block manually
    from_block: int = 20000000

    # # add +/-1 to the block range because the query is exclusive to the block number
    # query = hypersync.preset_query_blocks_and_transactions(
    #     from_block-1, to_block+1)

    query = hypersync.Query(
        from_block=from_block-1,
        to_block=to_block+1,
        include_all_blocks=True,
        transactions=[TransactionSelection()],
        traces=[TraceSelection()],
        field_selection=hypersync.FieldSelection(
            block=[e.value for e in BlockField],
            transaction=[e.value for e in TransactionField],
            trace=[e.value for e in TraceField]
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
            },
            trace={
                TraceField.GAS_USED: DataType.FLOAT64,
            }
        )
    )

    await client.collect_parquet("data", query, config)


start_time = time.time()
asyncio.run(historical_blocks_txs_sync())
end_time = time.time()

print(f"Time taken: {end_time - start_time}")
