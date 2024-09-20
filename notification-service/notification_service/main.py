from fastapi import FastAPI,Depends,HTTPException
from sqlmodel import SQLModel,table,Session,create_engine,Field,select
from aiokafka import AIOKafkaClient,AIOKafkaConsumer,AIOKafkaProducer
from . import settings
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from contextlib import asynccontextmanager
import asyncio
from typing import Optional,List,Annotated
from notification_service import notification_pb2
import logging
from notification_service.model import Notification
from notification_service.kafka_consumer import consume_messages
from notification_service.db import create_tables
from notification_service.topic import create_topic




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
   return{"welcome to zia mart","notification_service"}



