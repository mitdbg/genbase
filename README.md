genbase
=======

GenBase: a Complex Analytics benchmark for Genomics

Why genomics:
=============

Genomics is quickly becoming a major source of big data due to advances in sequencing technology. It is now possible to sequence over 2000 people per day at a sequencing facility. At 3 GB/genome, such a facility produces 6TB of data per day. Furthermore, it has gotten exponentially cheaper to sequence full genomes. In spite of the large amount of data available, we are unable to analyze it at scale.

Why do we need a new benchmark:
===============================

Existing database benchmarks focus on database operations and basic analytics (aggregates, rankings). However, genomics workloads are made of complex analytics not covered by traditional DBMS benchmarks. For instance, researchers often build regression models, use statistics to find enrichment in datasets etc. These workloads involve a mix of traditional database, statistics and linear algebra operations. As a result, we have a new benchmark based on complex analytics.

NOTE: This benchmark does not cover all possible operations performed in genomics. In particular, we chose to not focus on processing of raw sequence data and instead focus on higher level processing.

Benchmark:
==========

This benchmark was developed in collaboration with Novartis and Broad Institute scientists. The code was developed by the MIT Database Group and the Intel Parallel Computing Lab.

Data:
-----

This benchmark focuses on microarray (i.e. gene expression) data augmented by gene and patient metadata and gene ontology. Our results were generated on 4 data sizes:

- Small: 5K X 5K
- Medium: 15K X 20K
- Large: 30K X 40K
- Extra Large: 60K X 70K (* none of the systems we tested on our experimental set up were able to run on the extra large dataset. Read more in experimental setup)

Experimental microarray data is available at http://www.ncbi.nlm.nih.gov/geo/. We chose to write our own data generator based on experimental data so that we could generate variable size data and generate the associated metadata.

Queries:
--------

Based on the operations commonly performed by genomics researchers, we selected five representative queries that have a mix of DB ops, statistics and linear algebra. The general workflow looks as follows:
(a) select a subset of the input datasets using traditional DB ops (selects, joins)
(b) perform linear algebra or statistics operations (see below)
(c) combine results of step (b) with initial dataset

We focus on the linear algebra and stats operations below: 

- Linear Regression: build regression model to predict drug response from expression data

- Covariance: determine which pairs of genes have expression values that are correlated

- SVD: reduce the dimensionality of the problem to the top 50 components

- Biclustering: simultaneously cluster rows and columns in the expression matrix to find related genes

- Statistics: determine if certain sets of genes are highly expressed compared to the entire set of genes

Systems:
--------

As part of this work, we have tested the benchmark queries on a variety of systems, specifically:
- R
- pbdR
- Postgres+R
- Postgres+MADlib
- Column DBMS+R
- SciDB
- Hadoop+Mahout

Code:
-----

We implemented the benchmark queries in all the systems above and code for all systems except SciDB is freely available in this repository. We've made efforts to optimize the code but there are certainly ways to do this better, so please feel free to optimize the code further and submit your results.

Experimental Setup:
-------------------

We ran out benchmark code for all combinations of data sizes and # of nodes (in our case, 1, 2 or 4 nodes). Each machine has the following configuration: Intel Xeon E5-2620 processors with 2-sockets of 6 cores each and 48 GB RAM, 6 2-TB disks configured as 3 virtual 4-TB disks (RAID 0).

Paper:
------

The paper, presented at SIGMOD 2014, can be found <a href= "http://dl.acm.org/citation.cfm?id=2595633&CFID=593829267&CFTOKEN=38043504">here</a>.

Contact:
--------

genbase@mit.edu





