from fastapi import FastAPI,Depends,HTTPException
from sqlmodel import SQLModel,table,Session,create_engine,Field,select
from aiokafka import AIOKafkaClient,AIOKafkaConsumer,AIOKafkaProducer
from . import settings
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from contextlib import asynccontextmanager
import asyncio
from typing import Optional,List,Annotated
from order_service import order_pb2
import logging
from order_service.model import Order, Orders, Products
from order_service.kafka_consumer import consume_messages
from order_service.db import create_tables,engine,get_session
from order_service.topic import create_topic
from order_service.kafka_producer import kafka_producer




@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    print('Creating Tables')
    create_tables()
    await create_topic()
    print("Tables Created")
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task


app = FastAPI(lifespan=lifespan,
              title="Zia Mart User Service...",
              version='1.0.0'
              )


@app.get('/')
async def root():
   return{"welcome to zia mart","order_service"}



async def Product_id(id: int, session : Session):
    product = session.get(Products,id)
    if product:   
       return product

@app.post('/order',response_model=Orders)
async def Orders(newOrder : Orders,producer : Annotated[AIOKafkaProducer, Depends(kafka_producer)],
                 
                 ):
    pb_Order = order_pb2.order()
    pb_Order.id = newOrder.id
    pb_Order.product_id = newOrder.product_id
    pb_Order.quantity = newOrder.quantity
    
    
    checkProduct = await Product_id(id = pb_Order.product_id, session = Session(engine) )
    if checkProduct is not None:
        if checkProduct.quantity >= newOrder.quantity:
            pb_Order.total_price = newOrder.quantity*checkProduct.price
            pb_Order.product_name = checkProduct.name
            serializedtostring = pb_Order.SerializeToString()
            await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC,serializedtostring)
            await producer.send_and_wait(settings.KAFKA_NOTIFICATION_TOPIC,serializedtostring)
            return newOrder
        else :
            raise HTTPException(status_code=404, detail="not enough quantity")
    else:
        raise HTTPException(status_code=404,detail="product not found")



@app.get('/order',response_model=List[Order] )
async def getOrder(session:Annotated[Session,Depends(get_session)]):
    
    
    order = session.exec(select(Order)).all()
    return order
   

@app.get('/order/{id}',response_model=Order)
async def singleOrder(id:int,session:Annotated[Session, Depends(get_session)]):
   
    order = session.get(Order,id)
    return order
  