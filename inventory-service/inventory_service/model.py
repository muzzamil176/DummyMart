from sqlmodel import SQLModel,Field
from typing import Optional



class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()
    quantity : int = Field() 
    


class inventory_item(SQLModel):
    product_id : int = Field()
    quantity : int = Field()
    





