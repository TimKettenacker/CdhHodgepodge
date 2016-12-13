# iteratively execute a curl command to revalidate records in CDH
# iteration frequency is based on records retrieved from elasticsearch

# download packages and establish system connection
require(elastic)
require(httr)
require(dplyr)
connect()

# read in all zips from depots
setwd("C:/Users/Administrator/Documents")
file <- read.csv("Depot_PLZ_Zuordnung_20150504.csv", sep=";", colClasses = c("depot"="character", "postcode"="character"))

# loop: send zip in a query to elastic
#       compute count of Golden Records that match the query
total_count <- 0

for(zip in file[,1]){
  match_frag1 <- '{ "query": { "bool" : { "must": { "prefix": { "_id" : "system" }}, "must": { "match": { "zip" : "'
  match_frag2 <- '" } } } } }'
  match <- paste0(match_frag1, zip, match_frag2, "")
  #enter valid index name
  res <- Search(index="customer-x01", body=match, scroll="5m", search_type = "scan")
  out <- list()
  hits <- 1
  while(hits != 0){
    res <- scroll(scroll_id = res$`_scroll_id`)
    hits <- length(res$hits$hits)
    if(hits > 0)
      out <- c(out, res$hits$hits)
  }
  total_count <- total_count + length(out)
  print(total_count)
  if(total_count > 100000){
    break
  }
  # system call
  shell(paste("cd",setwd("C:/uniserv/cdh/tools"),sep=" "))
  Sys.sleep(3)
  #enter valid curl command
  shell(paste('curl.exe', ' -X', ' POST ', 'http://localhost:6441/current/database/xxx/_histname-update-batch?ZipCode=', as.character(zip), ' --data', ' " "', sep=""))
}
# change directory to file input
setwd("C:/Users/Administrator/Documents")
row_number_current_zip <- which(file$postcode == zip)
outf.df <- slice(file, row_number_current_zip:length(file[,1]))
out_file <- paste(getwd(), "/Depot_PLZ_Zuordnung_20150504.csv", sep="")
write.csv2(outf.df, out_file, row.names = FALSE, quote = FALSE)
print("that's all for today, folks!")







