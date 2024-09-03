# hypersync version 0.8.3
import hypersync
import polars as pl
import asyncio
from hypersync import (
    TransactionField,
    ClientConfig,
    TransactionSelection,
    BlockField,
    ColumnMapping, 
    DataType
)




async def main():
    client = hypersync.HypersyncClient(ClientConfig())

    # The query to run
    query = hypersync.Query(
        from_block=0,
        field_selection=hypersync.FieldSelection(
            block=[e.value for e in BlockField],
            transaction=[e.value for e in TransactionField],
        ),
        transactions=[
            TransactionSelection(
                hash=[
                    "0x410eec15e380c6f23c2294ad714487b2300dd88a7eaa051835e0da07f16fc282",
                    "0x110753637c9ead4b97c37a7a6a36c30015ffcd0effa5736c574de05d4f7adeb5",   
                ]
            )
        ],
    )
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
    print("Running the query...")

    # Run the query once, the query is automatically paginated so it will return when it reaches some limit (time, response size etc.)
    # there is a next_block field on the response object so we can set the from_block of our query to this value and continue our query until
    # res.next_block is equal to res.archive_height or query.to_block in case we specified an end block.
    data = await client.collect_arrow(query, config)

    # Convert ArrowResponse to an Arrow Table
    txs_df = pl.from_arrow(data.data.transactions)
    blocks_df = pl.from_arrow(data.data.blocks)

    print(txs_df)
    print(blocks_df)

asyncio.run(main())