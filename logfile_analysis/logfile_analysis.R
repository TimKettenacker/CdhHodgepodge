# aiming at analysis of logfiles for cdh
# the logfile concatenates all relevant information in single lines per action
# this is not very handy for analysis and visualization

# to-do: some of it should be handled more gracefully, i.e. missing values
#        some of the tasks can be capsuled in the toTime function

require(ggplot2)
require(plotly)

# read in lines delimited by "/n"
raw <- read.csv2("web-2017-01-25.log", sep = "\n", header = FALSE)

# do a first cleanup
cleansed_lines <- vector(mode = "list", length = nrow(raw))
for(line in raw){
  cleansed_lines <- gsub("\\[#1] |\\[01FF] INFO     Record saved: record\\(source=", replacement = "", line)
}

# extract date + time combination and grep keys
datetime <- list()
keys <- vector(mode = "list", length = nrow(raw))
for(i in 1:length(cleansed_lines)){
  key <- sub(".*?key=(.*?) .*", "\\1", cleansed_lines[i])
  keys[[i]] <- key
  cleansed_line_stripped <- unlist(strsplit(cleansed_lines[i], " ", fixed = TRUE))
  datetime <- append(datetime, paste(cleansed_line_stripped[1], cleansed_line_stripped[2], sep = " "))
}

# function to display CET and fractional seconds
toTime <- function(val){
  op <- options(digits.secs = 3)
  op
  line <- sub(",", ".", val, fixed = TRUE)
  time <- strptime(line, "%Y-%m-%d %H:%M:%OS")
  return(time)
}

# iterate over toTime function to calculate time intervals
store_time <- vector(mode = "list", length = length(datetime))
for(i in 1:length(datetime)){
  store_time[[i]] <- toTime(datetime[i])
}

time_interval <- list()
for(n in 1:length(store_time)){
  tryCatch({
    z <- store_time[[n+1]] - store_time[[n]]
    time_interval <- append(time_interval, as.numeric(z))
  }, error=function(e){cat(conditionMessage(e), " for the last time interval; this is logical, continue")})
}

# store all results in data frame
time_interval[[i]] <- NA
df_timelapse <- data.frame(observation_point = 1:length(datetime), keys = unlist(keys), datetime = unlist(datetime), time_interval = unlist(time_interval))

# visualize time intervals
a <- ggplot(df_timelapse, aes(observation_point, time_interval))
ggplotly(a + geom_line() + theme_bw() + xlab("number of processed records") + ylab("elapsed time between records in sec"))

# visualize density of time elapsed
b <- ggplot(df_timelapse, aes(time_interval))
ggplotly(b + geom_histogram(bins = 100, fill = "#0dc4b3") + theme_bw() + xlab("interval in s") + ylab("frequency"))

# write results to csv
out_file <- paste(getwd(), "\\log_assessment.csv", sep="")
write.csv2(df_timelapse, out_file, row.names = FALSE, quote = FALSE)
print(paste0("created csv in ", getwd()))
