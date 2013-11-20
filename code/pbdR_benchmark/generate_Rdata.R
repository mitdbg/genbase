args <- commandArgs(trailingOnly = TRUE)
PATH <- args[1]
NGENES <- args[2]
NPATIENTS <- args[3]
GEO <- paste(PATH, '/GEO-', NGENES, '-', NPATIENTS, sep="")
GO <- paste(PATH, '/GO-', NGENES, '-', NPATIENTS, sep="")
GENES <- paste(PATH, '/GeneMetaData-', NGENES, '-', NPATIENTS, sep="")
PATIENTS <- paste(PATH, '/PatientMetaData-', NGENES, '-', NPATIENTS, sep="")
RDATA <- '.Rdata'
PART <- paste('-part', comm.rank(), sep="")

library(pbdDMAT, quiet=TRUE)

load_from_db <- function(query)
{
    start_time = proc.time();
    library(RPostgreSQL);
    drv <- dbDriver("PostgreSQL");
    con <- dbConnect(drv, dbname='biodb30k');
    rs <- dbSendQuery(con, query);
    res = fetch(rs, n=-1);
    return(res);
}

get.grid <- function () {
  # this function gets the block-cyclic grid to create a ddmatrix
  blacs_ <- blacs(0)

  if (blacs_$MYROW == -1 || blacs_$MYCOL == -1) {
    return(NULL)
  }  

  # determine which rows each process should grab
  row.mod <- blacs_$NPROW * bldim[1]    
  myrows <- blacs_$MYROW * bldim[1]
  myrows <- seq(myrows, myrows+bldim[1]-1)
  myrows <- myrows %% row.mod

  # determine which columns each process should grab
  col.mod <- blacs_$NPCOL * bldim[2]    
  mycols <- blacs_$MYCOL * bldim[2]
  mycols <- seq(mycols, mycols+bldim[2]-1)
  mycols <- mycols %% col.mod

  list(row.mod=row.mod, myrows=myrows, col.mod=col.mod, mycols=mycols)
}

init.grid() 

# ScaLAPACK blocking dimension
bldim <- c(4, 4)

grid <- get.grid()
query1 <- paste("SELECT patientid, geneid, expr_value FROM geo ",
      " WHERE patientid % ", grid$row.mod, " IN (", paste(grid$myrows, collapse=", "), ") ",
      " AND geneid % ", grid$col.mod, " IN (", paste(grid$mycols, collapse=", "), ") ",
      " ORDER BY patientid, geneid",sep="")
geo <- load_from_db(query1)
save(geo, file=paste(GEO, PART, RDATA, sep=""))

query2 <- "SELECT max(goid) from go_matrix"
num.go <- load_from_db(query2) + 1
query3 <- paste("SELECT goid, geneid, belongs from go_matrix gm WHERE goid >= ", (num.go * comm.rank())/comm.size(),
      " AND goid < ", (num.go * (comm.rank()+1))/comm.size(),
      " ORDER BY goid, geneid", sep="")
go <- load_from_db(query3)
save(go, file=paste(GO, PART, RDATA, sep=""))

if(comm.rank() == 0) {
  query4 <- "SELECT * FROM genes"
  genes <- load_from_db(query4)
  save(genes, file=paste(GENES, RDATA, sep=""))

  query5 <- "SELECT * FROM patients"
  patients <- load_from_db(query5)
  save(patients, file=paste(PATIENTS, RDATA, sep=""))
}

# shut down the MPI communicators
finalize()