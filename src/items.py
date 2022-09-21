
from scrapy import Item, Field

from dataclasses import dataclass
import typing
import pandas as pd

from util import get_abs_url
pd.NA

class data_pl(Item):
    description = Field()
    rating = Field()
    n_votes = Field()
    price = Field()
    
class data_pp(Item):
    description = Field()
    rating = Field()
    n_votes = Field()
    price = Field()
    availability = Field()


@dataclass
class Item_base:
    def __post_init__(self):
        raise NotImplementedError("Must be overridded!")
    
    def __call__(self) -> dict:
        feed_dict = {}
        
        #for variable in [v for v in dir(self) if not v.startswith("_")]:
        for variable in list(filter(lambda a: not a.startswith("_"), dir(self))):
            var_value = getattr(self, variable)
            if variable in self._processors_map:
                feed_dict[variable] = self._processors_map[variable](var_value) if var_value is not None else "pd.NA"
            else:
                feed_dict[variable] = var_value
        
        return feed_dict 

@dataclass
class Page_item(Item_base):
    url: typing.AnyStr
    description: typing.Any
    rate: typing.Any
    votes_number: typing.Any
    price: typing.Any
    availability: typing.Any
    
    _processors_map: typing.ClassVar[typing.Dict[str, typing.Callable]] = {"description": lambda a: str(a),
                                                                           "rate": lambda a: float(str(a).split(" out of ")[0]),
                                                                           "votes_number": lambda a: float(str(a).split(' ')[0].replace(",", '.')),
                                                                           "price": lambda a: float(str(a)),
                                                                           "availability": lambda a: "in stock" in str(a).strip().lower() }
    
    def __post_init__(self):
        pass
    
@dataclass
class List_item(Item_base):
    url: typing.AnyStr
    description: typing.Any
    rate: typing.Any
    votes_number: typing.Any
    price: typing.Any
    
    _processors_map: typing.ClassVar[typing.Dict[str, typing.Callable]] = {"description": lambda a: str(a),
                                                                           #"rate": lambda a: float(str(a).split(" out of ")[0]),
                                                                           "rate": lambda a: float(str(a).replace(",", ".").split(" de ")[0]),
                                                                           "votes_number": lambda a: float(str(a).replace(",", '.')),
                                                                           "price": lambda a: float(str(a.replace("\xa0", "")).replace(".", "").replace("R", "").replace("$", "").replace(",", "."))}
    
    def __post_init__(self):
        pass


#!a = Page_item("super fast ssd", "  87 out of 98.1   ", "123,12 votes pos", "87.12", " in stock ")
#!print(a())
#!
#!a = List_item("super fast ssd", "  87 out of 98.1 starts  ", "123,12", "$ 87.12",)
#!print(a())

#class Page_item:
#    
#    __process_data_map = {"description": lambda a: str(a),
#                          "rate": lambda a: float(str(a).split(" out of ")[0]),
#                          "votes_number": lambda a: float(str(a).split(' ')[0].replace(",", '.')),
#                          "price": lambda a: float(str(a)),
#                          "availability": lambda a: "in stock" in str(a).strip().lower() }
#    
#    def __init__(self, description, rate, votes_number, price, availability) -> None:
#        
#        self.description = description
#        self.rate = rate
#        self.votes_number = votes_number
#        self.price = price
#        self.availability = availability
#
#    def __call__(self) -> dict:
#        feed_dict = {}
#        
#        #for variable in [v for v in dir(self) if not v.startswith("_")]:
#        for variable in list(filter(lambda a: not a.startswith("_"), dir(self))):
#            var_value = getattr(self, variable)
#            feed_dict[variable] = Page_item.__process_data_map[variable](var_value) if var_value is not None else pd.NA
#        
#        return feed_dict

