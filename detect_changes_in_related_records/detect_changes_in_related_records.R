# the way to go in mongo
require(mongolite)
require(stringi)

# first, retrieve the record you intend to compare
# db.record_actual.find({ _id: { s : "c4c", k : "1011406" } } ).pretty()
# this will contain a) the revision number of the raw version
# {
#   "_id" : {
#     "s" : "c4c",
#     "k" : "1011406"
#   },
#   "raw" : {
#     "source" : "c4c",
#     "key" : "1011406",
#     "revision" : NumberLong(4551),
#
# and b) the siblings
# "merged" : {
#   "source" : "c4c",
#   "key" : "1011406",
#   "sibling" : [
#     {
#       "source" : "siebel",
#       "key" : "1-9S45"
#     }

db <- "" # enter valid database name 
s <- "" # enter valid source
k <- "" # enter valid key

get_current_record <- function(s, k) {
  m_2c_record_actual <- mongo(collection = "record_actual", db = db, url = "mongodb://localhost")
  match <- paste0('{\"_id\":{\"s\":\"', s, '\",\"k\":\"', k, '\"}}')
  current_record <- m_2c_record_actual$find(match)
  return(current_record)
}

actual <- get_current_record(s, k)


# you can then use the revision number to retrieve a date 
# db.revision.find({"_id" : 4551})
# { "_id" : NumberLong(4551), "user" : "api", "source" : "api", "date"
#   : ISODate("2017-02-20T16:13:44.758Z"), "revision_type" : "Update" }

get_revision_datetime <- function(revision) {
  m_2c_revision <- mongo(collection = "revision", db = db, url = "mongodb://localhost")
  res <- m_2c_revision$iterate(paste0('{\"_id\":', revision, '}'))
  revision_datetime <- res$one()$date
  return(revision_datetime)
}

record_datetime <- get_revision_datetime(actual$raw$revision) 


# then, you kind of do the same thing for all the siblings of the record: retrieve the revision of the raw version,
# and use this number to retrieve the date. Finally, compare all the dates

siblings <- as.data.frame(actual$merged$sibling)
for(r in 1:nrow(siblings)){
  sibling <- get_current_record(s = siblings[r,1], k = siblings[r,2])
  sibling_datetime <- get_revision_datetime(sibling$raw$revision) 
}

has_younger_siblings <- record_datetime > sibling_datetime


# if there is a younger sibling, check if the actual record at least delivers different information 

compare_net_info <- function(record_actual, record_sibling) {
  recordA <- unlist(record_actual, use.names = FALSE)
  recordB <- unlist(record_sibling, use.names = FALSE)
  comparison_results <- stri_detect_fixed(recordA, recordB, case_insensitive = TRUE)
  return(comparison_results)
}

if(has_younger_siblings == TRUE){
  for(r in 1:nrow(siblings)){
    sibling <- get_current_record(s = siblings[r,1], k = siblings[r,2])
    comparison_results <- compare_net_info(actual$raw[,-c(1:5)], sibling$raw[,-c(1:5)])
    if(FALSE %in% comparison_results == TRUE){print("content of raw versions differs")}
  }
}