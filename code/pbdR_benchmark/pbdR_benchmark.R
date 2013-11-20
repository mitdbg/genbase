args <- commandArgs(trailingOnly = TRUE)
PATH <- args[1]
NGENES <- args[2]
NPATIENTS <- args[3]

library(Matrix)
library(data.table)
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
  myrows <- seq(myrows, myrows+bldim[1]-1)
  myrows <- myrows %% row.mod

  # determine which columns each process should grab
  col.mod <- blacs_$NPCOL * bldim[2]    
  mycols <- blacs_$MYCOL * bldim[2]
  mycols <- seq(mycols, mycols+bldim[2]-1)
  mycols <- mycols %% col.mod

  list(row.mod=row.mod, myrows=myrows, col.mod=col.mod, mycols=mycols)
}

get.mycol.procs <- function() {
  # get a list of the processes that have the same columns in the block cyclic layout 
  # as this process

  blacs_ <- blacs(0)

  procs <- seq(0,comm.size()-1)
  procs <- procs[procs %% blacs_$NPCOL == blacs_$MYCOL & procs != comm.rank()]

  return(procs)
}

get.myrow.procs <- function() {
  # get a list of the processes that have the same rows in the block cyclic layout 
  # as this process

  blacs_ <- blacs(0)

  procs <- seq(0,comm.size()-1)
  procs <- procs[procs %/% blacs_$NPROW == blacs_$MYROW & procs != comm.rank()]

  return(procs)
}

regression <- function()
{
  datamgt.time <- system.time({

    # load data  
    load(GEO)
    load(GENES)
    load(PATIENTS)

    # filter/project data
    sub_gmd = genes[genes$func < 250,]
    response = patients[,"response"]

    # convert to data tables
    colnames(sub_gmd)[1] = "geneid"
    sub_gmd_dt = data.table(sub_gmd, key="geneid")
    geo_dt = data.table(geo, key="geneid")
 
    # join
    A = merge(geo_dt, sub_gmd_dt)[,c("patientid", "geneid", "expr_value"), with=F]

    # send/receive data between processes sharing the same rows in block cyclic layout
    procs <- get.myrow.procs()
    received_A <- data.frame(patientid=integer(0), geneid=integer(0), expr_value=numeric(0))
    for(i in 0:comm.size()-1) {
      if(i %in% procs) {
        send(A, rank.dest = i)
      }
      else if(comm.rank() == i && length(procs) > 0) {
        for(j in 1:length(procs)) {
	  received_A <- rbind(received_A, recv(A, rank.source = procs[j]))
	}
      }
    }

    A = rbind(A, received_A)
    A = A[order(A$geneid, A$patientid),]

    # format as a matrix
    library(reshape2)
    A <- acast(A, list(names(A)[1], names(A)[2]))
    dimnames(A) <- NULL

    # filter out columns corresponding to block cyclic layout of new matrix
    cols <- seq(dim(A)[2])
    grid <- get.grid()
    cols <- cols[(cols-1) %% grid$col.mod %in% grid$mycols]
    A <- A[,cols]

    # create a distributed matrix
    dA <- new("ddmatrix", Data=A, dim=c(dim(patients)[1], dim(sub_gmd)[1]), ldim=dim(A),
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

    # load data  
    load(GEO)
    load(GENES)
    load(PATIENTS)

    # filter data
    sub_pmd = patients[patients$disease==5,]

    # convert to data tables
    colnames(sub_pmd)[1] = "patientid"
    sub_pmd_dt = data.table(sub_pmd, key="patientid")
    geo_dt = data.table(geo, key="patientid")
    gmd_dt = data.table(genes, key='geneid')

    # join
    A = merge(geo_dt, sub_pmd_dt)[,c("patientid", "geneid", "expr_value"), with=F]

    # send/receive data between processes sharing the same columns in block cyclic layout
    procs <- get.mycol.procs()
    received_A <- data.frame(patientid=integer(0), geneid=integer(0), expr_value=numeric(0))
    for(i in 0:comm.size()-1) {
      if(i %in% procs) {
        send(A, rank.dest = i)
      }
      else if(comm.rank() == i && length(procs) > 0) {
        for(j in 1:length(procs)) {
	  received_A <- rbind(received_A, recv(A, rank.source = procs[j]))
	}
      }
    }

    A = rbind(A, received_A)
    received_A <- NULL
    A = A[order(A$geneid, A$patientid),]

    # format as a matrix
    library(reshape2)
    A = acast(A, list(names(A)[1], names(A)[2]))
    dimnames(A) <- NULL

    # filter out rows corresponding to block cyclic layout of new matrix
    rows <- seq(dim(A)[1])
    grid <- get.grid()
    rows <- rows[(rows-1) %% grid$row.mod %in% grid$myrows]
    A <- A[rows,]

    # create a distributed matrix
    dA <- new("ddmatrix", Data=A, dim=c(dim(sub_pmd)[1], dim(gmd_dt)[1]), ldim=dim(A),
              bldim=bldim, ICTXT=0)
    A <- NULL    

  })

  analytics.time <- system.time({
    # calculate covariance in parallel
    covar.dist <- cov(dA)

    max.covar <- max(covar.dist)
    covar <- covar.dist@Data
  })

  comm.print(sprintf('Covariance analytics: %f\n', analytics.time['elapsed']))

  datamgt.tm <- system.time({
 
    # get genes corresponding to block cyclic layout of covariance matrix
    genes.row <- gmd_dt[gmd_dt$geneid %% grid$row.mod %in% grid$myrows,]
    genes.col <- gmd_dt[gmd_dt$geneid %% grid$col.mod %in% grid$mycols,]
    genes.row <- cbind(genes.row, rownum_row=row(as.matrix(genes.row$geneid)))
    genes.col <- cbind(genes.col, rownum_col=row(as.matrix(genes.col$geneid)))

    # filter covariance matrix
    covar <- which(covar>0.01*(max.covar), arr.ind=T)
     
    # 3-way join
    g1 <- merge(covar, genes.row, by.x="row", by.y="rownum_row.V1")
    g2 <- merge(g1, genes.col, by.x="col", by.y="rownum_col.V1")

    # This basically does the following: sol <- gather(g2)
    # Necessary or else mpi fails on larger data sets
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

    # load data  
    load(GEO)
    load(PATIENTS)
  
    # filter data
    sub_pmd = patients[patients$gender==1 & patients$age<=40,]

    # convert to data tables
    colnames(sub_pmd)[1] = "patientid"
    sub_pmd_dt = data.table(sub_pmd, key="patientid")
    geo_dt = data.table(geo, key="patientid")

    # join
    A = merge(geo_dt, sub_pmd_dt)[,c("patientid", "geneid", "expr_value"), with=F]

    # This basically does the following: A <- gather(A)
    # Necessary or else mpi fails on larger data sets
    A_list <- list()
    for(i in 1:16) {
      A_list[[i]] <- gather(A[A$geneid %% 16 + 1 == i,])
    }

    # format data on process 0
    if (comm.rank()==0) {
      A <- do.call(rbind, unlist(A_list, recursive=FALSE))
      A_list <- NULL
      A <- A[order(A$geneid, A$patientid),]

      # store as matrix
      library(reshape2)
      A <- acast(A, list(names(A)[1], names(A)[2]));
      dimnames(A) <- NULL
    } 
    else {
      A <- NULL
      A_list <- NULL
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

    # load data  
    load(GEO)
    load(GENES)

    # filter data
    sub_gmd = genes[genes$func < 250,]

    # convert to data tables
    colnames(sub_gmd)[1] = "geneid"
    sub_gmd_dt = data.table(sub_gmd, key="geneid")
    geo_dt = data.table(geo, key="geneid")

    # join
    A = merge(geo_dt, sub_gmd_dt)[,c("patientid", "geneid", "expr_value"), with=F]

    # This basically does the following: A <- gather(A)
    # Necessary or else mpi fails on larger data sets
    A_list <- list()
    for(i in 1:16) {
      A_list[[i]] <- gather(A[A$patientid %% 16 + 1 == i,])
    }
    
    # format data on process 0
    if (comm.rank()==0) {
      A <- do.call(rbind, unlist(A_list, recursive=FALSE))
      A_list <- NULL
      A <- A[order(A$geneid, A$patientid),]
     
      # store as matrix
      library(reshape2)
      A <- acast(A, list(names(A)[1], names(A)[2]));
      dimnames(A) <- NULL
    } 
    else {
      A <- NULL
      A_list <- NULL
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

    # load data  
    load(GEO)
    load(GENES)
    load(PATIENTS)

    # filter data
    sub_gmd = genes[genes$func < 250,]

    # convert to data tables
    colnames(sub_gmd)[1] = "geneid"
    sub_gmd_dt = data.table(sub_gmd, key="geneid")
    geo_dt = data.table(geo, key="geneid")
 
    # join
    A = merge(geo_dt, sub_gmd_dt)[,c("patientid", "geneid", "expr_value"), with=F]

    # send/receive data between processes sharing the same rows in block cyclic layout
    procs <- get.myrow.procs()
    received_A <- data.frame(patientid=integer(0), geneid=integer(0), expr_value=numeric(0))
    for(i in 0:comm.size()-1) {
      if(i %in% procs) {
        send(A, rank.dest = i)
      }
      else if(comm.rank() == i && length(procs) > 0) {
        for(j in 1:length(procs)) {
	  received_A <- rbind(received_A, recv(A, rank.source = procs[j]))
	}
      }
    }
    
    A = rbind(A, received_A)
    A = A[order(A$geneid, A$patientid),]

    # format as a matrix
    library(reshape2)
    A <- acast(A, list(names(A)[1], names(A)[2]))
    dimnames(A) <- NULL

    # filter out columns corresponding to block cyclic layout of new matrix
    cols <- seq(dim(A)[2])
    grid <- get.grid()
    cols <- cols[(cols-1) %% grid$col.mod %in% grid$mycols]
    A <- A[,cols]

    # create a distributed matrix
    dA <- new("ddmatrix", Data=A, dim=c(dim(patients)[1], dim(sub_gmd)[1]), ldim=dim(A),
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

    # load data  
    load(GEO)
    load(GO)

    # filter data
    geo = geo[geo$patientid < 0.0025*max(geo$patientid),]

    # combine data on all processes
    A <- allgather(geo)  
    A <- do.call(rbind, A)
    A <- A[order(A$geneid, A$patientid),]

    # store as matrix
    library(reshape2)
    A <- acast(A, list(names(A)[1], names(A)[2]))
    go <- acast(go, list(names(go)[1], names(go)[2]))

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

RDATA <- '.Rdata'
PART <- paste('-part', comm.rank(), sep="")
GEO <- paste(PATH, '/GEO-', NGENES, '-', NPATIENTS, PART, RDATA, sep="")
GO <- paste(PATH, '/GO-', NGENES, '-', NPATIENTS, PART, RDATA, sep="")
GENES <- paste(PATH, '/GeneMetaData-', NGENES, '-', NPATIENTS, RDATA, sep="")
PATIENTS <- paste(PATH, '/PatientMetaData-', NGENES, '-', NPATIENTS, RDATA, sep="")

regr.time <- system.time(regression(), gcFirst=T)['elapsed']
comm.print(paste('Regression: ', regr.time, sep=''));

#full.svd.time <- system.time(svd_full(), gcFirst=T)['elapsed']
#comm.print(paste('Full SVD: ', full.svd.time, sep=''));

svd.time <- system.time(svd_irlba(), gcFirst=T)['elapsed']
comm.print(paste('SVD: ', svd.time, sep=''));

biclust.time <- system.time(biclustering(), gcFirst=T)['elapsed']
comm.print(paste('Biclustering: ', biclust.time, sep=''));

cov.time <- system.time(covariance(), gcFirst=T)['elapsed']
comm.print(paste('Covariance: ', cov.time, sep=''));

stats.time <- system.time(stats(), gcFirst=T)['elapsed']
comm.print(paste('Stats: ', stats.time, sep=''));

# shut down the MPI communicators
finalize()
