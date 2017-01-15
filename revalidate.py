# import relevant libraries

import os
import csv
import pandas
import elasticsearch
from elasticsearch import Elasticsearch
es = Elasticsearch()

# read in all zip files from depots
os.chdir('C:\\Users\\Administrator\\Documents')
file = pandas.read_csv('Depot_PLZ_Zuordnung_2016_12.csv', sep = ';', converters={'depot': str, 'postcode': str})
frag1 = '{"query": { "match": { "zip":"'
frag2 = '"}}}'

total_count = []
zipCollector = []

# query all zips in file and count records respectively
for zip in file['postcode']:
    zipCollector.append(zip)
    match = frag1 + zip + frag2
    # choose valid index name
    res = es.count(index="customer-d01", body=match)
    total_count.append(res['count'])
    if total_count > 400000:
       break

# write collected zips to delta.csv to be picked up by cdh
print("Requested amount of zips has been fetched. Writing to file path now. Starting revalidation process.")
zipCollector = pandas.DataFrame(zipCollector)
pandas.DataFrame.to_csv(zipCollector, path_or_buf="C:\\uniserv\\cdh\\temp\\plzdelta\\delta.csv", index = False, encoding="UTF-8", header=False)
# slice data to contain everything from last cutoff and write to wd
crtrow = file[file['postcode'] == zip].index.tolist()
out_df = file[(crtrow[0]+1):len(file)]
path = os.getcwd() + "\\Depot_PLZ_Zuordnung_2016_12.csv"
pandas.DataFrame.to_csv(out_df, path_or_buf=path, index = False, encoding="UTF-8", header=False, sep = ";")
# invoke shell commands to call revalidation and update-histnames
os.chdir("C:\\Uniserv\\cdh\\tools")
os.system("admin help")
# choose valid command
os.system('curl.exe' + ' -X' + ' POST ' + 'http://localhost:6441/current/database/xxx/_revalidate-by-zip?SingleCsv=C:\\uniserv\\cdh\\temp\\plzdelta\\delta.csv' + ' --data' + ' " "')
print("Revalidation of zip code has finished. Starting to update historical names.")
# choose valid command
os.system('curl.exe' + ' -X' + ' POST ' + 'http://localhost:6441/current/database/xxx/_revalidate-by-zip?SingleCsv=C:\\uniserv\\cdh\\temp\\plzdelta\\delta.csv' + ' --data' + ' " "')
for zip in zipCollector:
    os.system('curl.exe' + ' -X' + ' POST ' + 'http://localhost:6441/current/database/dpd/_histname-update-batch?ZipCode=' + zip + ' --data' + ' " "')
print("End of processing")




