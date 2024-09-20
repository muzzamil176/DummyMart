from typing import Optional
from sqlmodel import SQLModel,Field

class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()
    quantity : int = Field() 
    

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
    quantity : int = Field()


