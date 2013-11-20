args <- commandArgs(trailingOnly = TRUE)
PATH <- args[1]
DBNAME <- args[2]

load_from_db <- function(query)
{
    start_time = proc.time();
    library(RPostgreSQL);
    drv <- dbDriver("PostgreSQL");
    con <- dbConnect(drv, dbname=DBNAME);
    rs <- dbSendQuery(con, query); 
    res = fetch(rs, n=-1);
    return(res);
}

source(paste(PATH, "/db_external_R_benchmark.R", sep=""))
