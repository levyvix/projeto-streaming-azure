import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from dotenv import dotenv_values

# Load environment variables
config = dotenv_values(".env")

# Get environment variables
STORAGE_CONNECTION_STR = config["BLOB_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER_NAME = config["BLOB_CONTAINER_NAME"]
CONNECTION_STR = config["EVENT_HUB_CONNECTION_STR"]
EVENTHUB_NAME = config["EVENT_HUB_NAME"]

# Event handler function
async def on_event(partition_context, event):
    # Print the event data
    print(event)
    # Update the checkpoint so that the program doesn't read the event again in case of a restart
    await partition_context.update_checkpoint(event)

# Main function
async def main(client):
    # Start the client and receive events
    async with client:
        await client.receive(on_event)

# Entry point of the program
if __name__ == '__main__':
    # Create a checkpoint store using Blob storage
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        STORAGE_CONNECTION_STR,
        container_name=BLOB_CONTAINER_NAME,
    )
    # Create an Event Hub consumer client
    client = EventHubConsumerClient.from_connection_string(
        CONNECTION_STR,
        consumer_group='$Default',
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store
    )
    # Run the main function
    asyncio.run(main(client))