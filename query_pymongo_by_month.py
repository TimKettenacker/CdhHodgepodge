# -*- coding: utf-8 -*-
import pandas as pd, pymongo, datetime
from pymongo import MongoClient
from datetime import datetime

# set up the connection
password = 'thepassword'
client = MongoClient('mongodb://nameofuserdatabase:' + password + '@127.0.0.1', tz_aware=True)
db = client['nameofdb']

january2015 = list()

for i in range(1,31):
  pipeline = [
     {
          "$match": {
                "validated.child.date_of_birth" : datetime(2015, 1, i),
                "validated.email_address.opt_in_out.state": "opt_in"
          }
     },
     {
          "$group": {
          "_id": "null",
          "count": { "$sum": 1}
          }
     }
     ]
  cursor = db.record_actual.aggregate(pipeline, useCursor=False)
  for c in cursor:
    january2015.append(c)