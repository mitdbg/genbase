# Basic user defined R functions for the Vertica Genomics Benchmark
# Author: Rebecca Taft, 3/21/13

###
# @brief Runs linear regression with the first n-1 columns
#        of the matrix as the independent variables, and the
#        last column as the dependent variables
#
# @param x (input) - data frame representing a matrix of the form (row_num, col_num, value)
# @return - the coefficient vector
###
linregr_udf <- function(x)
{
	library(reshape2)
	A <- acast(x, list(names(x)[1], names(x)[2]))
	
	ptm <- proc.time()
	res <- lm.fit(x=A[,1:dim(A)[2]-1], y=A[,dim(A)[2]])$coefficients
	print(proc.time() - ptm)
	res
}

linregrFactory <- function()
{
        list(name=linregr_udf,udxtype=c("transform"),intype=c("int","int","float"), outtype=c("float"), 
	   outnames=c("coeff"))
}

###
# @brief Runs biclustering on the rows and columns of a matrix
#
# @param x (input) - data frame representing a matrix of the form (row_num, col_num, value)
# @return - location of the the biclusters found 
###
bicluster_udf <- function(x)
{
        library(reshape2)
        A <- acast(x, list(names(x)[1], names(x)[2]))

        library(biclust)
        library("s4vd")
	ptm <- proc.time()
        S <- biclust(A, method=BCssvd, K=1)
        print(proc.time() - ptm)
        res <- melt(list(c=attr(S, "NumberxCol"), r=attr(S, "RowxNumber")))
        res
}

biclustFactory <- function()
{
        list(name=bicluster_udf,udxtype=c("transform"),intype=c("int","int","float"), outtype=c("int","int","int","char"),
                outnames=c("row_num", "col_num", "val", "matrix"))
}

###
# @brief Compute all-pairs covariance
#
# @param x (input) - data frame representing a matrix of the form (row_num, col_num, value)
# @return - the matrix resulting from all-pairs covariance
###
covar_udf <- function(x)
{
        library(reshape2)
        A <- acast(x, list(names(x)[1], names(x)[2]))

	ptm <- proc.time()
        S <- cov(A)
        print(proc.time() - ptm)
        res <- melt(S)
        res
}

covarFactory <- function()
{
	list(name=covar_udf,udxtype=c("transform"),intype=c("int","int","float"), outtype=c("int","int","float"), 
                outnames=c("row_num", "col_num", "val"))
}

###
# @brief run the Wilcoxon rank-sum test
#
# @param x (input) - data frame with the first column representing the category (either 0 or 1) 
#          and the second column representing a column in the matrix
# @return - The value of the W statistic and the p-value
###
wilcox_udf <- function(x)
{
	# Note: no timing here because this function is called 60,000 times
        go <- x[,1]
        vals <- x[,2]

        set1 <- vals[(go[] == 1)]
        set2 <- vals[(go[] == 0)]
        p <- wilcox.test(set1, set2, alternative="less")
        res <- cbind(p$statistic, p$p.value)
        res
}

wilcoxFactory <- function()
{
        list(name=wilcox_udf,udxtype=c("transform"),intype=c("int","float"), outtype=c("int", "float"),
            outnames=c("W","p_value"))
}



###
# @brief Calculates the singluar value decomposition for a rank 50 approximation
#
# @param x (input) - data frame representing a matrix of the form (row_num, col_num, value)
# @return - the matrices u and v, and the vector of singular values d for a rank 50 approximation
###
svd_udf <- function(x)
{
    	library(reshape2)
    	A <- acast(x, list(names(x)[1], names(x)[2]))

	library(irlba)
	ptm <- proc.time()
    	S <- irlba(A, nu=50, nv=50, sigma="ls")
    	print(proc.time() - ptm)
    	res <- melt(list(u=S$u, v=S$v, d=as.matrix(S$d)))
    	res
}

svdFactory <- function()
{
	list(name=svd_udf,udxtype=c("transform"),intype=c("int","int","float"), outtype=c("int","int","float","char"),
		outnames=c("row_num", "col_num", "val", "matrix"))
}

