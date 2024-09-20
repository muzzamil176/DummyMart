from typing import Optional
from sqlmodel import SQLModel,Field
from pydantic import BaseModel


class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()
    quantity : int = Field() 

class ProductBase(BaseModel):
    name: str
    price: float
    quantity: int


