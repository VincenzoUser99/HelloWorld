from pymongo import MongoClient
import pandas as pd
import requests
from io import BytesIO
from io import StringIO
import pandas as pd
from pymongo import MongoClient
import json
import numpy as np
from pymongo import InsertOne
from pymongo import UpdateMany

client=MongoClient ('mongodb://localhost:27017/')
db=client.MyDatabase

df=pd.DataFrame(db.test_1.find({}))
df1=pd.DataFrame(db.Elnco_comuni.find({}))

# Rimuovi la colonna '_id' dal DataFrame se esiste


#Droppiamo tutta la  colonne _id

# Inizializza il contatore delle nuove inserzioni

#


print(df.info())



