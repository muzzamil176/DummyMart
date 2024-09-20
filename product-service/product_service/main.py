from product_service import product_pb2
from product_service.model import Products,ProductBase
from product_service.Kafka_producer import kafka_producer
from fastapi import Depends
from typing import Annotated,List
from aiokafka import AIOKafkaProducer
from product_service import settings
from product_service.db import get_session,create_tables
from sqlmodel import Session, select
from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI,HTTPException
from product_service.kafka_consumer import consume_messages
from product_service.Topic import create_topic



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


@app.post('/create_product',response_model=Products)
async def Create_products(
    new_product : Products,
    Producer : Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    
):
    
    pb_product=product_pb2.product()
    pb_product.id=new_product.id
    pb_product.name=new_product.name
    pb_product.price=new_product.price
    pb_product.quantity = new_product.quantity
    pb_product.type = product_pb2.Operation.CREATE

    serialized_product = pb_product.SerializeToString()
    await Producer.send_and_wait(settings.KAFKA_ORDER_TOPIC,serialized_product)

    return new_product
   

@app.get('/products', response_model=List[Products])
async def get_products(session: Annotated[Session, Depends(get_session)]
                       ):
    products = session.exec(select(Products)).all()
    return products

@app.get ('/products/{product_id}', response_model=Products)
async def product(product_id : int,session : Annotated[Session, Depends(get_session)]):
    single = session.get(Products, product_id)
    return single

@app.delete('/products')
async def delete_product(product_id : int, producer : Annotated[AIOKafkaProducer, Depends(kafka_producer)],
                         db: Session = Depends(get_session)):
    if db.get(Products, product_id) is None:
        raise HTTPException(status_code=404, detail="No Product was found With that ID...")
    else:
        delete_product=product_pb2.product()
        delete_product.id = id
        delete_product.type = product_pb2.Operation.DELETE

        serialized_product = delete_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC,serialized_product)

        return {"product":"deleted"}
    


@app.put('/products',response_model=ProductBase)
async def update_products(product_id : int ,updated_product : ProductBase,producer : Annotated[AIOKafkaProducer,Depends(kafka_producer)],db: Session = Depends(get_session)):
    if db.get(Products, product_id) is None:
        raise HTTPException(status_code=404, detail="No Product was found With that ID...")
    else:
        new_product = product_pb2.product()
        new_product.id = product_id
        new_product.name = updated_product.name
        new_product.price = updated_product.price
        new_product.quantity = updated_product.quantity
        new_product.type = product_pb2.Operation.PUT
        
        
        serialized_product = new_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC,serialized_product)
        return updated_product
        
    
