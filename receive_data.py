import asyncio

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore,
)


import dotenv
import os

dotenv.load_dotenv(dotenv.find_dotenv())

STORAGE_CONNECTION_STR = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENT_HUB_NAME")


async def on_event(partition_context, event):
    # Put your code here.
    # Do some sync or async operations. If the operation is i/o intensive,
    # async will have better performance.
    print(event)
    # print(event.body_as_str(encoding='UTF-8'))
    await partition_context.update_checkpoint(event)


async def main(client):
    async with client:
        await client.receive(on_event)

if __name__ == '__main__':
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        STORAGE_CONNECTION_STR,
        container_name=BLOB_CONTAINER_NAME,
    )
    client = EventHubConsumerClient.from_connection_string(
        CONNECTION_STR,
        consumer_group='$Default',
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store
    )
    asyncio.run(main(client))