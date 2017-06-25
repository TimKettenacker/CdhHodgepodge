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
content of the younger sibling. 5. It creates a csv report of records providing 
no additional value on raw level.

"""

import pandas as pd, pymongo, datetime, json
from pymongo import MongoClient

# define methods
def get_current_record(recordsource, recordkey):
  return db.record_actual.find_one({ '_id': { "s" : recordsource, "k" : recordkey }})
    
def get_revision(revision_number):
  return db.revision.find_one({ '_id': revision_number})
  
def omit_during_compare(dict):
  ignore = ('source', 'key', 'revision', 'type', 'step')
  for k in ignore:
    dict.pop(k, None)
  return dict

#compare raw content
class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        return self.set_current - self.intersect 
    def removed(self):
        return self.set_past - self.intersect 
    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])

fmt = '%Y-%m-%d %H:%M:%S %Z%z'  

# set up the connection
password = '5eFRuMAprA8r'
client = MongoClient('mongodb://UserName:' + password + '@127.0.0.1', tz_aware=True)
db = client['DatabaseName']

# read in a file providing source and key, matching the regular output format
# of cdh-csv-exporter. Double-check the working directory. 
data = pd.read_csv('cdh-records.csv', dtype=str)
out = []
  
for row_id in range(1, len(data)+1): 
    current_doc = get_current_record(data['record.source'][row_id], data['record.key'][row_id])
    current_doc_datetime = get_revision(current_doc['raw']['revision'])
    for sibling in current_doc['merged']['sibling']:
        try:
            sibling_doc = get_current_record(sibling['source'], sibling['key'])
            sibling_datetime = get_revision(sibling_doc['raw']['revision'])
            # if timestamp of sibling is younger compare raw content of records
            if (current_doc_datetime['date'].strftime(fmt) > sibling_datetime['date'].strftime(fmt))==True:
                current_doc['raw'] = omit_during_compare(current_doc['raw'])
                sibling_doc['raw'] = omit_during_compare(sibling_doc['raw'])
                d = DictDiffer(current_doc['raw'], sibling_doc['raw'])
                differ_list = []
                differ_list.append(json.dumps(current_doc['_id']))
                differ_list.append(current_doc_datetime['date'].strftime(fmt))
                differ_list.append(json.dumps(sibling_doc['_id']))
                differ_list.append(sibling_datetime['date'].strftime(fmt))
                differ_list.append(json.dumps(list(d.changed())))
                differ_list.append(json.dumps(list(d.unchanged())))
                out.append(differ_list)   
        except KeyError:
            print "skip attempt to compare raw version to Golden Record"

        
df = pd.DataFrame(out)    
df.to_csv("output.csv", sep = ";", index=False, header = ['requested record', 'entered system on', 'sibling of requested record', 'entered system on', 'new info','same info'], quoting = False)
print str(row_id) + " records from the input file have been processed, " + str(len(df)) + " are marked as dubious results in the output file"
