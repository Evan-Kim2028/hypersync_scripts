import hypersync
import asyncio


# ALL OF THESE EVENTS ARE IN MAIN BRanch.
# event_1 = "Staked(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
# event_2 = "StakeAdded(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount, uint256 newBalance)"
# event_3 = "Unstaked(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
# event_4 = "StakeWithdrawn(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
# event_5 = "Slashed(address indexed msgSender, address indexed slashReceiver, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
# event_6 = "MinStakeSet(address indexed msgSender, uint256 newMinStake)"
# event_7 = "SlashAmountSet(address indexed msgSender, uint256 newSlashAmount)"
# event_8 = "SlashOracleSet(address indexed msgSender, address newSlashOracle)"
# event_9 = "SlashReceiverSet(address indexed msgSender, address newSlashReceiver)"
# event_10 = (
#     "UnstakePeriodBlocksSet(address indexed msgSender, uint256 newUnstakePeriodBlocks)"
# )

# events_set = [
#     event_1,
#     event_2,
#     event_3,
#     event_4,
#     event_5,
#     event_6,
#     event_7,
#     event_8,
#     event_9,
#     event_10,
# ]


# ALL OF THESE EVENTS ARE IN 0.5.1 BRANCH
event_1 = "Staked(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
event_2 = "StakeAdded(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount, uint256 newBalance)"
event_3 = "Unstaked(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
event_4 = "StakeWithdrawn(address indexed msgSender, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
event_5 = "Slashed(address indexed msgSender, address indexed slashReceiver, address indexed withdrawalAddress, bytes valBLSPubKey, uint256 amount)"
event_6 = "MinStakeSet(address indexed msgSender, uint256 newMinStake)"
event_7 = "SlashAmountSet(address indexed msgSender, uint256 newSlashAmount)"
event_8 = "SlashOracleSet(address indexed msgSender, address newSlashOracle)"
event_9 = "SlashReceiverSet(address indexed msgSender, address newSlashReceiver)"
event_10 = (
    "UnstakePeriodBlocksSet(address indexed msgSender, uint256 newUnstakePeriodBlocks)"
)

events_set = [
    event_1,
    event_2,
    event_3,
    event_4,
    event_5,
    event_6,
    event_7,
    event_8,
    event_9,
    event_10,
]


async def main():
    # Create hypersync client using the mainnet hypersync endpoint (default)
    client = hypersync.HypersyncClient(
        hypersync.ClientConfig(url="https://holesky.hypersync.xyz")
    )

    smart_contract = "0x5d4fC7B5Aeea4CF4F0Ca6Be09A2F5AaDAd2F2803".lower()

    for event in events_set:
        # topic0 of transaction event signature (hash of event signature)
        # query will return logs of this event
        topic = hypersync.signature_to_topic0(event)

        query = hypersync.preset_query_logs_of_event(
            smart_contract, topic, 0, 2_279_200
        )

        print("Running the query...")

        # Run the query once, the query is automatically paginated so it will return when it reaches some limit (time, response size etc.)
        # there is a next_block field on the response object so we can set the from_block of our query to this value and continue our query until
        # res.next_block is equal to res.archive_height or query.to_block in case we specified an end block.
        res = await client.get(query)

        print(
            f"Query returned {len(res.data.logs)} logs of transfer events from contract {smart_contract}"
        )


asyncio.run(main())
