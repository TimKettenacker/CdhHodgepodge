## this code needs some further examination once the requirements have been refined...

## list all records that have the same email address, but are in fact different persons 
## also keep information like source, part of healthcare, ... 

## load required packages

require(readr)
require(data.table)
require(dplyr)

raw_data <- read_delim("cdh-records.csv", col_names = TRUE, locale = locale(encoding = "UTF-8"), guess_max = 1000, delim = ",",
                       col_types = cols(email_address.address="c", record.key="c", postal_address.housenumber="c"))
raw_data <- as.data.table(raw_data)

# remove rows with duplicated keys introduced by cartesian product
raw_data <- raw_data %>% distinct(record.key, .keep_all = TRUE)

# extract unique email addresses (works also with the pipe-operator, but I want a vector to iterate over)
dnct_emails <- unique(raw_data$email_address.address)

# look for (multiple) occurrences of email addresses in the data table and store row indices in a list
# site note: ".I" can be used to access the row indices a subset of data in a data table (?"special-symbols")
email_indices <- vector(mode = "list", length(dnct_emails))
for(i in 1:length(dnct_emails)){
  email_indices[[i]] <- raw_data[, .I[which(email_address.address == dnct_emails[i],)],]
}

# set emails that appeared only once to NA and delete them afterwards
n <- 1
for(n in 1:length(email_indices)){
if(length(email_indices[[n]]) <= 1){
  email_indices[[n]] <- NA
 }
}

bad <- is.na(email_indices)
email_indices <- email_indices[!bad]

# add grouping id to all indices, so it is possible to find together what belongs together
raw_data$group.id <- as.integer()
group_ids <- seq_along(1:length(email_indices))
for(group_id in group_ids){
  raw_data[unlist(email_indices[[group_id]])]$group.id <- group_id
}


# create a data table containing all the records with duplicated email addresses
emails_vc <- unlist(email_indices)
all_mail_dpl_dt <- raw_data[c(emails_vc)]