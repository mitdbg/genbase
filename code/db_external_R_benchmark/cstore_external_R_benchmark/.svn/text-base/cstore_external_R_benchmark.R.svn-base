args <- commandArgs(trailingOnly = TRUE)
PATH <- args[1]
DBNAME <- args[2]
USER <- args[3]
PWORD <- args[4]

load_from_db <- function(query)
{
    library(RJDBC)
    drv <- JDBC("com.vertica.jdbc.Driver",
           "/opt/vertica/java/lib/vertica-jdk5-6.1.2-0.jar",
           identifier.quote="`")
    con <- dbConnect(drv, paste("jdbc:vertica://localhost/", DBNAME, sep=""), user=USER, pwd=PWORD);
    rs <- dbSendQuery(con, query); 
    res <- fetch(rs, n=-1);
    return(res)
}

source(paste(PATH, "/db_external_R_benchmark.R", sep=""))
