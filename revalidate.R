# place this file in C:\Program Files\R\"whatever R version is installed"\bin>
# in the command prompt, go and run in C:\Program Files\R\"whatever R version is installed"\bin>r -f revalidate.R

# iteratively execute curl commands to revalidate and update records in CDH
# iteration frequency is based on records retrieved from elasticsearch

# download packages and establish system connection
require(elastic)
require(httr)
require(dplyr)
connect()

# read in all zips from depots
setwd("C:/Users/Administrator/Documents")
file <- read.csv("Depot_PLZ_Zuordnung_2016_12.csv", sep=";", colClasses = c("depot"="character", "postcode"="character"))
total_count <- 0
zip_collector <- list()

# query all zips in file and count affected records
for(zip in file[,1]){
  frag1_q <- '{ "query": { "match": { "zip": "'
  frag2_q <- '"}}}'
  match <- paste0(frag1_q, as.character(zip), frag2_q, "")
  # choose a valid index name
  res <- Search(index="customer-d01", body=match, scroll="5m", search_type = "scan")
  total_count <- total_count + res$hits$total
  zip_collector <- append(zip_collector, zip)
  if(total_count > 350000){
    break
  }
}
# write zips to file to be picked up by admin command
print("Requested amount of zips has been fetched. Writing to file path now. Starting revalidation process.")
Sys.sleep(2)
df_zip_collector <- t(as.data.frame(zip_collector))
out_file <- paste("C:\\uniserv\\cdh\\temp\\plzdelta\\delta.csv", sep="")
write.csv2(df_zip_collector, out_file, row.names = FALSE, quote = FALSE)
# adapt origin file so it will pick up correctly when program is scheduled 
row_number_current_zip <- which(file$postcode == zip)
outf.df <- slice(file, row_number_current_zip:length(file[,1]))
setwd("C:/Users/Administrator/Documents")
out_file <- paste(getwd(), "/Depot_PLZ_Zuordnung_2016_12.csv", sep="")
write.csv2(outf.df, out_file, row.names = FALSE, quote = FALSE)
# system command
shell(paste("cd",setwd("C:/uniserv/cdh/tools"),sep=" "))
Sys.sleep(2)
# enter valid curl command
shell(paste('curl.exe', ' -X', ' POST ', 'http://localhost:6441/current/database/xxx/_revalidate-by-zip?SingleCsv=C:\\uniserv\\cdh\\temp\\plzdelta\\delta.csv', ' --data', ' " "', sep=""))
print("Revalidation of zip code has finished. Now starting to update historical names.")
Sys.sleep(2)
shell(paste("cd",setwd("C:/uniserv/cdh/tools"),sep=" "))
# call update of historical names based on processed zips
for(zip in zip_collector){
  print(paste0("updating historical data to zip code ", as.character(zip), sep = ""))
  shell(paste('curl.exe', ' -X', ' POST ', 'http://localhost:6441/current/database/dpd/_histname-update-batch?ZipCode=', as.character(zip), ' --data', ' " "', sep=""))
}
print("That's all for today folks!")




