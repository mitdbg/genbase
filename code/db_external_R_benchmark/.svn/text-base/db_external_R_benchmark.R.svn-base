# Before sourcing this file, define the load_from_db() function for your database

regression <- function()
{
  ptm <- proc.time()
  # load the filtered data
  query1 <- "SELECT g.patientid, g.geneid, g.expr_value FROM geo g, genes ge WHERE g.geneid = ge.geneid AND ge.func < 250 ORDER BY g.patientid, g.geneid"
  geo <- load_from_db(query1)

  query2 <- "SELECT response FROM patients ORDER BY id"
  response <- load_from_db(query2)
  response <- as.matrix(response)

  # convert geo data to matrix
  library(reshape2)
  A <- acast(geo, list(names(geo)[1], names(geo)[2]))
  cat(sprintf('Regression data management: %f\n', (proc.time() - ptm)['elapsed']))
  ptm <- proc.time()

  # run regression
  lm.fit(x=A, y=response)
  cat(sprintf('Regression analytics: %f\n', (proc.time() - ptm)['elapsed']))
}


covariance <- function()
{
  ptm <- proc.time()
  # load the filtered data
  query1 <- "SELECT g.patientid, g.geneid, g.expr_value FROM geo g, patients p WHERE g.patientid = p.id AND p.disease = 5 ORDER BY g.patientid, g.geneid"
  geo <- load_from_db(query1)

  # convert geo data to matrix
  library(reshape2)
  A <- acast(geo, list(names(geo)[1], names(geo)[2]))
  midtm <- (proc.time() - ptm)['elapsed']
  ptm <- proc.time()

  # calculate covariance
  covar <- cov(A)
  cat(sprintf('Covariance analytics: %f\n', (proc.time() - ptm)['elapsed']))
  ptm <- proc.time()
  covar <- which(covar>0.01*(max(covar)), arr.ind=T)
  covar[,'row'] <- covar[,'row']-1
  covar[,'col'] <- covar[,'col']-1

  # get the correct genes 
  query2 <- "SELECT * from genes"
  genes <- load_from_db(query2)

  g1 <- merge(covar, genes, by.x="row", by.y="geneid")
  g2 <- merge(g1, genes, by.x="col", by.y="geneid")
  cat(sprintf('Covariance data management: %f\n', (proc.time() - ptm)['elapsed'] + midtm))
}


biclustering <- function()
{
  ptm <- proc.time()
  # load the filtered data
  query1 <- "SELECT g.patientid, g.geneid, g.expr_value FROM geo g, patients p WHERE g.patientid = p.id AND p.age <= 40 AND p.gender = 1 ORDER BY g.patientid, g.geneid"
  geo <- load_from_db(query1)

  # convert geo data to matrix
  library(reshape2)
  A <- acast(geo, list(names(geo)[1], names(geo)[2]))
  cat(sprintf('Biclust data management: %f\n', (proc.time() - ptm)['elapsed']))
  ptm <- proc.time()

  # run biclustering
  library(biclust)
  library("s4vd")
  biclust(A, method=BCssvd, K=1)
  cat(sprintf('Biclust analytics: %f\n', (proc.time() - ptm)['elapsed']))
}


svd_irlba <- function()
{
  ptm <- proc.time()
  library(irlba)

  # load the filtered data
  query1 <- "SELECT g.patientid, g.geneid, g.expr_value FROM geo g, genes ge WHERE g.geneid = ge.geneid AND ge.func < 250 ORDER BY g.patientid, g.geneid"
  geo <- load_from_db(query1)

  # convert geo data to matrix
  library(reshape2)
  A <- acast(geo, list(names(geo)[1], names(geo)[2]))
  cat(sprintf('SVD data management: %f\n', (proc.time() - ptm)['elapsed']))
  ptm <- proc.time()

  # run svd
  irlba(A, nu=50, nv=50, sigma="ls")
  cat(sprintf('SVD analytics: %f\n', (proc.time() - ptm)['elapsed']))
}


stats <- function()
{
  ptm <- proc.time()
  library(Matrix)
  library(multicore)
  library(doMC)
  library(foreach)

  registerDoMC(16) # 16 cores

  # load the filtered data
  query1 <- "SELECT patientid, geneid, expr_value FROM geo WHERE patientid < 0.0025 * (SELECT max(id) FROM patients)"
  geo <- load_from_db(query1)
  geo[,1] <- geo[,1]+1
  geo[,2] <- geo[,2]+1
  A <- sparseMatrix(geo[,1], geo[,2], x=geo[,3])

  # load the go_matrix
  go <- load_from_db("SELECT * from go_matrix")
  go[,1] <- go[,1]+1
  go[,2] <- go[,2]+1
  go <- sparseMatrix(go[,1], go[,2], x=go[,3])
  cat(sprintf('Stats data management: %f\n', (proc.time() - ptm)['elapsed']))
  ptm <- proc.time()

  # run wilcoxon rank sum test
  foreach (ii=1:dim(go)[2]) %dopar%
  {
    foreach(jj=1:dim(A)[1]) %dopar%
    {
      set1 <- A[jj,(go[,ii] == 1)]
      set2 <- A[jj,(go[,ii] == 0)]
      wilcox.test(set1, set2, alternative="less")
    }
  }
  cat(sprintf('Stats analytics: %f\n', (proc.time() - ptm)['elapsed']))
}

print(paste('Regression: ', system.time(regression(), gcFirst=T)['elapsed'], sep=''));
print(paste('SVD: ', system.time(svd_irlba(), gcFirst=T)['elapsed'], sep=''));
print(paste('Covariance: ', system.time(covariance(), gcFirst=T)['elapsed'], sep=''));
print(paste('Biclustering: ', system.time(biclustering(), gcFirst=T)['elapsed'], sep=''));
print(paste('Stats: ', system.time(stats(), gcFirst=T)['elapsed'], sep=''));
