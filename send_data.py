import asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import dotenv
import os
import random

# Load environment variables
dotenv.load_dotenv(dotenv.find_dotenv())

# Get environment variables for event hub connection
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# Define asynchronous function to send data to event hub
async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    # Start the producer client
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        for i in range(50):
            event_data_batch.add(EventData(str({
                "CodCAM": "CM" + str(i).zfill(3),
                "Temperatura": random.randint(-10, 5),
                "Odometro": random.randint(0, 100),
            })))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)
        print(f"Sent a batch of 50 events to the event hub.")

# Run the function
asyncio.run(run())