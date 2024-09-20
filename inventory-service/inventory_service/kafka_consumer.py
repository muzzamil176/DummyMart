from aiokafka import AIOKafkaConsumer
from inventory_service import settings
from sqlmodel import Session
from inventory_service.db import engine
from inventory_service import inventory_pb2
from inventory_service.model import Products
import logging


async def consume_messages():
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_TOPIC,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        # session_timeout_ms=30000,  # Example: Increase to 30 seconds
        # max_poll_records=10,
    )
    # Start the consumer.
    await consumer.start()
    try:
        with Session(engine) as session:
        # Continuously listen for messages.
            async for msg in consumer:
                if msg.value is not None:
                    try:
                        product = inventory_pb2.product()
                        product.ParseFromString(msg.value)            
                        data_from_producer=session.get(Products,product.id)
                        print(f"Recieve msg from kafka:{msg.value}")
                        # if product.type == inventory_pb2.Operation.CREATE:
                        if data_from_producer:
                            data_from_producer.quantity  += product.quantity
                            session.add(data_from_producer)
                            session.commit()
                            session.refresh(data_from_producer)
                            print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')
                        
                    

                    except:
                        logging.error(f"Error deserializing messages")
                else :
                    print("Msg has no value")
    except Exception as e: 
        logging.error(f"Error consuming messages: {e}")
    finally:
        # Ensure to close the consumer when done.
            await consumer.stop()