from typing import Annotated
import asyncio
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from user_service.kafka_consumer import consume_messages
from user_service.topic import create_topic
from user_service.kafka_producer import kafka_producer
from user_service import settings
from sqlmodel import Session, select
from user_service import user_pb2
from typing import Annotated, List, Optional
from user_service.function import authenticate_user
from user_service.auth import create_access_token, decode_token, ALGORITHM, SECRET_KEY, verify_password, get_password_hash, get_current_user
from user_service.model import Users, TokenResponse,User,Edit
from user_service.db import create_tables, get_session, engine
from fastapi.middleware.cors import CORSMiddleware



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
              title="Zia Mart User Service..."
              )
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods, including OPTIONS
    allow_headers=["*"],  # Allow all headers
)

@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart"}

@app.post('/register', response_model=User)
async def Create_User(
    New_user:User,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    hashed_password = get_password_hash(New_user.password)

    pb_product = user_pb2.user()
    pb_product.id = New_user.id
    pb_product.username = New_user.name
    pb_product.email = New_user.email
    pb_product.password = hashed_password
    pb_product.usertype = user_pb2.Type.CUSTOMER
    pb_product.type = user_pb2.Operation.CREATE
    serialized_product = pb_product.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

    return New_user





@app.post('/Register_Admin', response_model=User)
async def Create_User(
    New_user:User,
    SECRET_PASSWORD : str,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    if SECRET_PASSWORD != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=404,detail=("This is not you portal"))
    else:
        hashed_password = get_password_hash(New_user.password)

        pb_product = user_pb2.user()
        pb_product.id = New_user.id
        pb_product.username = New_user.name
        pb_product.email = New_user.email
        pb_product.password = hashed_password
        pb_product.type = user_pb2.Operation.CREATE
        pb_product.usertype = user_pb2.Type.ADMIN
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

        return New_user
    
# GET token to fetch all products
@app.post('/token', response_model=TokenResponse)
async def login_user(form_data: OAuth2PasswordRequestForm = Depends(),
 db: Session = Depends(get_session)):
    user = authenticate_user(session=db, username=form_data.username, password=form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid User Credentials...")
    access_token = create_access_token(data={"id": user.id, "name": user.name, "email": user.email, "usertype": user.user_type})
    return  {"access_token": access_token, "token_type": "bearer"}
    
# Post endpoint to fetch all products
@app.get('/users', response_model=List[Users])
async def get_all_users(session: Annotated[Session, Depends(get_session)], current_user: Annotated[Users, Depends(get_current_user)]):
    print(current_user)
    if current_user.get("usertype") != user_pb2.Type.ADMIN:
        raise HTTPException(status_code=404, detail="You are not Admin")
    else:    
        all_users = session.exec(select(Users)).all()
        return all_users

@app.get('/users/{user_id}', response_model=Users)
async def get_user(user_id: int, db: Session = Depends(get_session),
                   current_user : Users = Depends(get_current_user)):
    user = db.get(Users, user_id)
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    elif current_user.get("id") != user_id:
            raise HTTPException(status_code=401,detail="you are not allowed to see other users")
    return user



@app.delete('/users/{user_id}')
async def delete_user(
    user_id: int,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user : Annotated[Users, Depends(get_current_user)]
):
    if current_user.get("id") != user_id:
        raise HTTPException(status_code=404,detail = "you are not allowed to delete the id")
    else:   
        pb_product = user_pb2.user()
        pb_product.id=user_id
        pb_product.type=user_pb2.Operation.DELETE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return {"message": "User Deleted"}

@app.put('/Edit/{user_id}',response_model=Edit)
async def Edits (user_id : int ,
                Update : Edit,
                producer : Annotated[AIOKafkaProducer,Depends(kafka_producer)],
                session : Annotated[Session,Depends(get_session)],
                current_user : Annotated[Users, Depends(get_current_user)]):
    
    edit_user = session.get(Users,user_id)
    if edit_user is None:
        raise HTTPException(status_code=404,detail="Please provide valid id")
    elif current_user.get("id") != user_id:
        raise HTTPException(status_code=401, detail="Kindly Edit your own id")
    else:
        hashed_password = get_password_hash(Update.password)

        pb_product = user_pb2.user()
        pb_product.id = user_id
        pb_product.username = Update.name
        pb_product.email = Update.email
        pb_product.password = hashed_password
        pb_product.type = user_pb2.Operation.PUT
       
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

        return Update