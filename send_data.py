import asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

import dotenv
import os

import random

dotenv.load_dotenv(dotenv.find_dotenv())

event_data = [{
    "CodCAM": "CM" + str(i).zfill(3),
    "Temperatura": random.randint(-10, 5),
    "Odometro": random.randint(0, 100),
} for i in range(50)]

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        # event_data_batch.add(EventData("First event"))
        # event_data_batch.add(EventData("Second event"))
        # event_data_batch.add(EventData("Third event"))

        for event in event_data:
            event_data_batch.add(EventData(str(event)))


        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)
        print(f"Sent a batch of {len(event_data)} events to the event hub.")

asyncio.run(run())