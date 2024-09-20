from aiokafka import AIOKafkaConsumer
from notification_service import notification_pb2
from notification_service import settings
import logging
from notification_service.model import Notification
from twilio.rest import Client
from . import settings

account_sid = settings.ACCOUNT_ID
auth_token = settings.AUTH_TOKEN

client = Client(account_sid, auth_token)

async def sendNotification(message:Notification):
    message = client.messages.create(
        from_='whatsapp:+14155238886',
        body=message,
        to=settings.PHONE_NUMBER
    )
    print(message.sid)

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
                    product = notification_pb2.order()
                    product.ParseFromString(msg.value)
                    message = f"new order placed {product.id,product.product_id,product.product_name,product.user_id,product.quantity,product.total_price}"    
                    await sendNotification(message)        
                    
                except Exception as e:
                    print(f"Error processing messages: {e}")
            else :
                print("Msg has no value")
    except Exception as e: 
        logging.error(f"Error consuming messages: {e}")
    finally:
        # Ensure to close the consumer when done.
            await consumer.stop()