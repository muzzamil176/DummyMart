from fastapi import FastAPI,Depends,HTTPException
from sqlmodel import SQLModel,table,Session,create_engine,Field,select
from aiokafka import AIOKafkaClient,AIOKafkaConsumer,AIOKafkaProducer
from . import settings

from contextlib import asynccontextmanager
import asyncio
from typing import Optional,List,Annotated
from inventory_service import inventory_pb2
from inventory_service.model import inventory_item,Products
from inventory_service.db import create_tables, get_session
from inventory_service.topic import create_topic
from inventory_service.kafka_consumer import consume_messages
from inventory_service.kafka_producer import kafka_producer



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
   return{"welcome to zia mart","user_service"}





@app.get('/Product/{product_id}',response_model=inventory_item)
async def view_quantity(id:int, session : Annotated[Session, Depends(get_session)],
                       ):
    product = session.get(Products,id)
    
    if product is None:
        raise HTTPException(status_code=404, detail="product not found")
    
    return inventory_item(product_id=product.id, quantity=product.quantity)


@app.post('/product/{product_id}',response_model=inventory_item)
async def change_quantity(pRoduct_id: int,Quantity:int,
                          producer : Annotated[AIOKafkaProducer, Depends(kafka_producer)],
                          session : Annotated[Session,Depends(get_session)] ):
    product = session.get(Products,pRoduct_id)
    if product:
        new_quantity = inventory_pb2.product()
        new_quantity.id = pRoduct_id
        new_quantity.quantity = Quantity
        

        serializedtostring = new_quantity.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC,serializedtostring)

        return inventory_item(product_id=product.id, quantity=Quantity)
        
    else:
        raise HTTPException(status_code=404, detail="Product not found")

