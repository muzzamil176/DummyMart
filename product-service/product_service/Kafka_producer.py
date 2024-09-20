from aiokafka import AIOKafkaProducer
from product_service import settings
from fastapi import FastAPI



async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

