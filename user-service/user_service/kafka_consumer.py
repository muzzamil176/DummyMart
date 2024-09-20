from aiokafka import AIOKafkaConsumer

from user_service import settings
from sqlmodel import Session
from user_service.db import engine
import logging
from user_service.model import Users
from user_service import user_pb2
from user_service.db import get_session






    # Start the consumer.
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
        # Continuously listen for messages.
        async for msg in consumer:
            if msg is not None:
                try:
                    user = user_pb2.user()
                    user.ParseFromString(msg.value)            
                    data_from_producer=Users(id=user.id, name= user.username, email= user.email, password=user.password,user_type=user.usertype) 
                    print(f"Received message from Kafka: {msg.value}")
                    # Here you can add code to process each message.
                    with Session(engine) as session:
                        if user.type == user_pb2.Operation.CREATE:
                            session.add(data_from_producer)
                            session.commit()
                            session.refresh(data_from_producer)
                            print(f'''Stored Protobuf data in database with ID: {user.id}''')
                            
                           
                        elif user.type == user_pb2.Operation.PUT:
                            updated_user = session.get(Users, user.id)
                            if updated_user:
                                updated_user.name = user.username
                                updated_user.email = user.email
                                updated_user.password = user.password
                                session.commit()
                                session.refresh(updated_user)
                                print(f'''Updated Protobuf data in database with ID: {user.id}''')
                            else:
                                print(f"User with ID: {data_from_producer.id} not found")
                           
                        elif user.type == user_pb2.Operation.DELETE:
                            deleted_user = session.get(Users, user.id)
                            if deleted_user:
                                session.delete(deleted_user)
                                session.commit()
                                print(f"Deleted Protobuf data in database with ID: {user.id}")
                            else:
                                print(f"User with ID: {data_from_producer.id} not found")
                except Exception as e:  # Catch deserialization errors
                    print(f"Error processing message: {e}")
            else:
                print("No message received from Kafka.")
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
