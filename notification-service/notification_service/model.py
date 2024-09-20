from pydantic import BaseModel
from sqlmodel import SQLModel,Field
from typing import Optional





class Notification (BaseModel):
    message : str 
    

class Order(SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    product_id : int = Field()
    product_name : str =Field()
    user_id : int = Field()
    quantity : int = Field()
    total_price : float = Field()

class Orders(SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    product_id : int =Field()
    user_id : int = Field()
    quantity : int = Field()
