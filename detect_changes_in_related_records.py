# -*- coding: utf-8 -*-
"""
Different sources integrated into our hub provide (nearly) the same information.
Sometimes, our clients receive data from external providers which they have 
already stored. Tracking down updates and origins on these sources manually is 
a difficult and time-consuming task. 

This python script compares raw information of siblings (records that share the 
same Golden Record): For any given record, it 1. looks up its raw version,
2. remembers its datetime and 3. checks if there is an earlier datetime among
its siblings. If so, it 4. compares the content of the raw record with the 
content of the younger sibling. 5. It creates a csv report of it.

"""

import pandas as pd, pymongo
from pymongo import MongoClient

# read in a file providing source and key, matching the regular output format
# of cdh-csv-exporter. Double-check the working directory. 
data = pd.read_csv('cdh-records.csv')

# set up the connection
client = MongoClient()   
db = client['test']

def get_current_record(recordsource, recordkey):
    return db.record_actual.find_one({ '_id': { "s" : recordsource, "k" : recordkey }})

current_doc = get_current_record('acs-actie','BABYBOOM_19538')

def get_revision(revision_number):
    return db.revision.find_one({ '_id': revision_number})

get_revision(current_doc['raw']['revision'])

 
 