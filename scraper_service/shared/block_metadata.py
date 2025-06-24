from shared.clickhouse.utils import get_clickhouse_client
from shared.substrate import get_substrate_client

timestamps = dict()


def refresh_timestamp_dict(n):
    """
    Caches n -> n+10k timestamps
    """
    clickhouse = get_clickhouse_client()
    timestamps.clear()

    # Fetch 10k timestamps at a time
    query = f"""
        SELECT timestamp, block_number
        FROM shovel_block_timestamps
        WHERE block_number >= {n} AND block_number < {n + 10_000}
    """
    r = clickhouse.execute(query)
    for (timestamp, block_number) in r:
        timestamps[block_number] = timestamp


def get_block_timestamp(n, block_hash):
    """
    First tries to fetch from cache, then chain.
    """
    if n not in timestamps:
        refresh_timestamp_dict(n)

    if n in timestamps:
        substrate = get_substrate_client()
        return int(timestamps[n].timestamp())
    else:
        print("WARN: Block n timestamp not found in Clickhouse, falling back to chain")
        substrate = get_substrate_client()
        return int(
            substrate.query(
                "Timestamp",
                "Now",
                block_hash=block_hash,
            ).serialize()
            / 1000
        )


def get_block_metadata(n):
    """
    Gets block metadata (timestamp, blockhash) for the latest block.

    TODO: In the future it will be adjusted to first check an in-memory cache, then Clickhouse, then Substrate.
    """

    substrate = get_substrate_client()
    block_hash = substrate.get_block_hash(n)

    # If still not there, just get it from the chain
    block_timestamp = get_block_timestamp(n, block_hash)

    return (block_timestamp, block_hash)
