from order_service import order_pb2
from aiokafka import AIOKafkaConsumer
from order_service import settings
from order_service.model import Order, Products
import logging
from sqlmodel import Session
from order_service.db import engine



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
    print("consumer started")
    try:
        async for msg in consumer:
            if msg.value is not None:
                try:
                    order = order_pb2.order()
                    order.ParseFromString(msg.value)            
                    data_from_producer=Order(id = order.id, product_id=order.product_id, product_name=order.product_name, user_id=order.user_id, quantity=order.quantity, total_price=order.total_price)
                    print(f"Recieve msg from kafka:{msg.value}")
                    with Session(engine) as session:
                        session.add(data_from_producer)
                        session.commit()
                        session.refresh(data_from_producer)
                        print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')
                    db_product = session.get(Products, order.product_id)
                    if db_product:
                        db_product.quantity  += -(order.quantity)
                        session.add(db_product)
                        session.commit()
                        session.refresh(db_product)
                except:
                        logging.error(f"Error deserializing messages")
            else :
                    print("Msg has no value")
    except Exception as e: 
        logging.error(f"Error consuming messages: {e}")
    finally:
        # Ensure to close the consumer when done.
            await consumer.stop()