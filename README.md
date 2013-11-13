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

Queries:
--------

We focus on 5 queries that have a mix of DB ops, statistics and linear algebra.

- Linear Regression
- Covariance
- SVD
- Biclustering
- Statistics

Systems:
--------

As part of this work, we have tested the benchmark queries on a variety of systems, specifically:
- R
- pbdR
- Postgres+R
- Column DBMS+R
- Column DBMS+MADlib
- SciDB

Code:
-----

The code for all the above systems is available in this repository. Note that the code is not guaranteed to be optimized. Please feel free to optimize the code further and submit your results.

The data generator is also available in the data/generator folder. 

Experimental Setup:
-------------------

All the code was run on a 4-node cluster with each machine having the following configuration.

Our setup was: all data sizes X 1, 2, 4-node clusters.

Paper:
------

Please read more about this work in our paper. The work is currently in submission.

Contact:
--------

genbase@mit.edu





