# Before sourcing this file, define the load_from_db() function for your database
library(pbdDMAT, quiet=TRUE)

get.grid <- function () {
  # this function gets the block-cyclic grid to create a ddmatrix
  blacs_ <- blacs(0)

  if (blacs_$MYROW == -1 || blacs_$MYCOL == -1) {
    return(NULL)
  }  

  # determine which rows each process should grab
  row.mod <- blacs_$NPROW * bldim[1]    
  myrows <- blacs_$MYROW * bldim[1]
  myrows <- seq(myrows, myrows+bldim[1]-1) + 1
  myrows <- myrows %% row.mod

  # determine which columns each process should grab
  col.mod <- blacs_$NPCOL * bldim[2]    
  mycols <- blacs_$MYCOL * bldim[2]
  mycols <- seq(mycols, mycols+bldim[2]-1) + 1
  mycols <- mycols %% col.mod

  list(row.mod=row.mod, myrows=myrows, col.mod=col.mod, mycols=mycols)
}

regression <- function()
{
  datamgt.time <- system.time({

    if(comm.rank()==0) {
      query0 <- "SELECT count(*) FROM patients"
      num.patients <- load_from_db(query0)

      query1 <- "SELECT count(*) FROM genes WHERE func < 250"
      num.genes <- load_from_db(query1)
    
      query2 <- "SELECT response FROM patients ORDER BY id"
      response <- load_from_db(query2)
      response <- as.matrix(response)
      dimnames(response) <- NULL
    }
    else {
      num.patients <- 0
      num.genes <- 0
      response <- NULL
    }

    num.patients <- allreduce(as.integer(num.patients), op='max')
    num.genes <- allreduce(as.integer(num.genes), op='max')

    grid <- get.grid()

    query3 <- paste("SELECT g.patientid, g.geneid, g.expr_value FROM geo g, genes ge, ",
      " (SELECT geneid, rank() OVER(ORDER BY geneid) AS rank FROM genes WHERE func < 250) AS gr WHERE g.geneid = ge.geneid ",
      " AND ge.func < 250 AND g.patientid % ", grid$row.mod, " IN (", paste((grid$myrows-1) %% grid$row.mod, collapse=", "), ") ",
      " AND gr.geneid = ge.geneid ",
      " AND gr.rank % ", grid$col.mod, " IN (", paste(grid$mycols, collapse=", "), ") ",
      " ORDER BY g.patientid, g.geneid",sep="")
    geo <- load_from_db(query3)

    library(reshape2)
    geo <- acast(geo, list(names(geo)[1], names(geo)[2]))
    dimnames(geo) <- NULL
    dA <- new("ddmatrix", Data=geo, dim=c(num.patients, num.genes), ldim=dim(geo),
              bldim=bldim, ICTXT=0)
    
  })
	
  comm.print(sprintf('Regression data management: %f\n', datamgt.time['elapsed']))
 
  analytics.time <- system.time({
    dr <- as.ddmatrix(x=response, bldim=bldim)

    # Run regression in parallel
    dsol <- lm.fit(x=dA, y=dr)

    # Undistribute solutions to process 0
    sol <- as.matrix(dsol$coefficients, proc.dest=0)
  })

  comm.print(sprintf('Regression analytics: %f\n', analytics.time['elapsed']))
}

covariance <- function()
{
  datamgt.midtm <- system.time({
    if(comm.rank()==0) {
      query0 <- "SELECT count(*) from patients WHERE disease = 5"
      num.patients <- load_from_db(query0)

      query1 <- "SELECT count(*) FROM genes"
      num.genes <- load_from_db(query1)
    }
    else {
      num.patients <- 0
      num.genes <- 0
    }

    num.patients <- allreduce(as.integer(num.patients), op='max')
    num.genes <- allreduce(as.integer(num.genes), op='max')

    grid <- get.grid()

    # load the filtered data
    query2 <- paste("SELECT g.patientid, g.geneid, g.expr_value FROM geo g, patients p, ",
      " (SELECT id, rank() OVER(ORDER BY id) AS rank FROM patients WHERE disease = 5) AS pr WHERE g.patientid = p.id ",
      " AND p.disease = 5 AND pr.rank % ", grid$row.mod, " IN (", paste(grid$myrows, collapse=", "), ") ",
      " AND pr.id = p.id ",
      " AND g.geneid % ", grid$col.mod, " IN (", paste((grid$mycols-1) %% grid$col.mod, collapse=", "), ") ",
      " ORDER BY g.patientid, g.geneid",sep="")
    geo <- load_from_db(query2)

    library(reshape2)
    geo <- acast(geo, list(names(geo)[1], names(geo)[2]))
    dimnames(geo) <- NULL
    dA <- new("ddmatrix", Data=geo, dim=c(num.patients, num.genes), ldim=dim(geo),
              bldim=bldim, ICTXT=0)

  })

  analytics.time <- system.time({
    # calculate covariance in parallel
    covar.dist <- cov(dA)

    max.covar <- max(covar.dist)
    covar <- covar.dist@Data
  })

  comm.print(sprintf('Covariance analytics: %f\n', analytics.time['elapsed']))

  datamgt.tm <- system.time({
 
    query3 <- paste("SELECT *, rank() OVER(ORDER BY geneid) AS rank FROM genes ",
     " WHERE geneid % ", grid$row.mod, " IN (", paste((grid$myrows-1) %% grid$row.mod, collapse=", "), ") ",
     " ORDER BY geneid",sep="")
    genes.row <- load_from_db(query3)

    query4 <- paste("SELECT *, rank() OVER(ORDER BY geneid) AS rank FROM genes ",
     " WHERE geneid % ", grid$col.mod, " IN (", paste((grid$mycols-1) %% grid$col.mod, collapse=", "), ") ",
     " ORDER BY geneid",sep="")
    genes.col <- load_from_db(query4)

    covar <- which(covar>0.01*(max.covar), arr.ind=T)
     
    g1 <- merge(covar, genes.row, by.x="row", by.y="rank")
    g2 <- merge(g1, genes.col, by.x="col", by.y="rank")

    #sol <- gather(g2)
    sol <- list()
    for(i in 1:16) {
      sol[[i]] <- gather(g2[g2$row %% 16 + 1 == i,])
    }
  })

  comm.print(sprintf('Covariance data management: %f\n', datamgt.midtm['elapsed'] + datamgt.tm['elapsed']))
}

biclustering <- function()
{
  datamgt.time <- system.time({

    query0 <- "SELECT count(*) from genes"
    num.genes <- load_from_db(query0)

    # load the filtered data
    query1 <- paste("SELECT g.patientid, g.geneid, g.expr_value FROM geo g, patients p WHERE g.patientid = p.id ",
      " AND p.age <= 40 AND p.gender = 1 AND g.geneid >= ", (num.genes * comm.rank())/comm.size(),
      " AND g.geneid < ",(num.genes * (comm.rank()+1))/comm.size(),
      " ORDER BY g.patientid, g.geneid",sep="")
    geo <- load_from_db(query1)

    library(reshape2)
    geo <- acast(geo, list(names(geo)[1], names(geo)[2]))
    A <- gather(geo)

    # format data on process 0
    if (comm.rank()==0) {
      A <- do.call(cbind, A)
      dimnames(A) <- NULL
    } 
    else {
      A <- NULL
    }
  })

  comm.print(sprintf('Biclust data management: %f\n', datamgt.time['elapsed']))

  analytics.time <- system.time({
    if (comm.rank()==0) {
      # run biclustering
      library(biclust)
      library("s4vd")
      biclust(A, method=BCssvd, K=1)
    }
  })

  comm.print(sprintf('Biclust analytics: %f\n', analytics.time['elapsed']))
}


svd_irlba <- function()
{
  datamgt.time <- system.time({

    query0 <- "SELECT max(id) from patients"
    num.patients <- load_from_db(query0) + 1

    # load the filtered data
    query1 <- paste("SELECT g.patientid, g.geneid, g.expr_value FROM geo g, genes ge WHERE g.geneid = ge.geneid ",
      " AND ge.func < 250 AND g.patientid >= ", (num.patients * comm.rank())/comm.size(),
      " AND g.patientid < ",(num.patients * (comm.rank()+1))/comm.size(),
      " ORDER BY g.patientid, g.geneid",sep="")
    geo <- load_from_db(query1)

    library(reshape2)
    geo <- acast(geo, list(names(geo)[1], names(geo)[2]))
    A <- gather(geo)

    # load/format data on process 0, then distribute to the others
    if (comm.rank()==0) {
      A <- do.call(rbind, A)
      dimnames(A) <- NULL
    } 
    else {
      A <- NULL
    }
  })

  comm.print(sprintf('SVD data management: %f\n', datamgt.time['elapsed']))

  analytics.time <- system.time({
    if (comm.rank()==0) {
      # run svd
      library(irlba)
      irlba(A, nu=50, nv=50, sigma="ls")
    }
  })  

  comm.print(sprintf('SVD analytics: %f\n', analytics.time['elapsed']))
}

svd_full <- function()
{
  datamgt.time <- system.time({

    if(comm.rank()==0) {
      query0 <- "SELECT count(*) FROM patients"
      num.patients <- load_from_db(query0)

      query1 <- "SELECT count(*) FROM genes WHERE func < 250"
      num.genes <- load_from_db(query1)
    }
    else {
      num.patients <- 0
      num.genes <- 0
    }

    num.patients <- allreduce(as.integer(num.patients), op='max')
    num.genes <- allreduce(as.integer(num.genes), op='max')

    grid <- get.grid()

    query3 <- paste("SELECT g.patientid, g.geneid, g.expr_value FROM geo g, genes ge, ",
      " (SELECT geneid, rank() OVER(ORDER BY geneid) AS rank FROM genes WHERE func < 250) AS gr WHERE g.geneid = ge.geneid ",
      " AND ge.func < 250 AND g.patientid % ", grid$row.mod, " IN (", paste((grid$myrows-1) %% grid$row.mod, collapse=", "), ") ",
      " AND gr.geneid = ge.geneid ",
      " AND gr.rank % ", grid$col.mod, " IN (", paste(grid$mycols, collapse=", "), ") ",
      " ORDER BY g.patientid, g.geneid",sep="")
    geo <- load_from_db(query3)

    library(reshape2)
    geo <- acast(geo, list(names(geo)[1], names(geo)[2]))
    dimnames(geo) <- NULL
    dA <- new("ddmatrix", Data=geo, dim=c(num.patients, num.genes), ldim=dim(geo),
              bldim=bldim, ICTXT=0)
    
  })

  comm.print(sprintf('Full SVD data management: %f\n', datamgt.time['elapsed']))

  analytics.time <- system.time({
    # calculate svd in parallel
    svd.dist <- svd(dA)

    # Undistribute solutions to process 0
    u <- as.matrix(svd.dist$u, proc.dest=0)
    v <- as.matrix(svd.dist$v, proc.dest=0)
    d <- svd.dist$d
  })

  comm.print(sprintf('Full SVD analytics: %f\n', analytics.time['elapsed']))
}

stats <- function()
{
  datamgt.time <- system.time({

    if(comm.rank()==0) {
      query0 <- "SELECT count(*) FROM genes"
      num.genes <- load_from_db(query0)

      query1 <- "SELECT max(goid) from go_matrix"
      num.go <- load_from_db(query1) + 1
    }
    else {
      num.genes <- 0
      num.go <- 0
    }

    num.genes <- allreduce(as.integer(num.genes), op='max')
    num.go <- allreduce(as.integer(num.go), op='max')

    # load the filtered data
    query2 <- paste("SELECT patientid, geneid, expr_value FROM geo ",
      " WHERE patientid < 0.0025 * (SELECT max(id) FROM patients) ",
      " AND geneid >= ", (num.genes * comm.rank())/comm.size(),
      " AND geneid < ", (num.genes * (comm.rank()+1))/comm.size(),
      " ORDER BY patientid, geneid", sep="")
    geo <- load_from_db(query2)

    query3 <- paste("SELECT goid, geneid, belongs from go_matrix gm WHERE goid >= ", (num.go * comm.rank())/comm.size(),
      " AND goid < ", (num.go * (comm.rank()+1))/comm.size(),
      " ORDER BY goid, geneid", sep="")
    go <- load_from_db(query3)

    library(reshape2)
    geo <- acast(geo, list(names(geo)[1], names(geo)[2]))
    A <- allgather(geo)

    go <- acast(go, list(names(go)[1], names(go)[2]))

    A <- do.call(cbind, A)
    dimnames(A) <- NULL
    dimnames(go) <- NULL
  })

  comm.print(sprintf('Stats data management: %f\n', datamgt.time['elapsed']))

  analytics.time <- system.time({
    # run wilcoxon rank sum test
    wilcox <- function(go, A) { 
      set1 <- A[(go[] == 1)]
      set2 <- A[(go[] == 0)]
      wilcox.test(set1, set2, alternative="less")
    }

    wilcox1 <- function(go) { 
      lapply(as.list(data.frame(t(A))), wilcox, go=go)
    }

    dsol <- lapply(as.list(data.frame(t(go))), wilcox1) 
    sol <- gather(dsol)
  })

  comm.print(sprintf('Stats analytics: %f\n', analytics.time['elapsed']))
}

init.grid() 

# ScaLAPACK blocking dimension
bldim <- c(4, 4)

regr.time <- system.time(regression(), gcFirst=T)['elapsed']
comm.print(paste('Regression: ', regr.time, sep=''));

svd.time <- system.time(svd_irlba(), gcFirst=T)['elapsed']
comm.print(paste('SVD: ', svd.time, sep=''));

#full.svd.time <- system.time(svd_full(), gcFirst=T)['elapsed']
#comm.print(paste('Full SVD: ', full.svd.time, sep=''));

cov.time <- system.time(covariance(), gcFirst=T)['elapsed']
comm.print(paste('Covariance: ', cov.time, sep=''));

biclust.time <- system.time(biclustering(), gcFirst=T)['elapsed']
comm.print(paste('Biclustering: ', biclust.time, sep=''));

stats.time <- system.time(stats(), gcFirst=T)['elapsed']
comm.print(paste('Stats: ', stats.time, sep=''));

# shut down the MPI communicators
finalize()