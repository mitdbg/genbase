\timing

DROP LIBRARY BenchmarkFunctions CASCADE;
CREATE LIBRARY BenchmarkFunctions AS 'cstore_R_benchmark_functions.R' LANGUAGE 'R';
CREATE TRANSFORM FUNCTION covar AS LANGUAGE 'R' NAME 'covarFactory' LIBRARY BenchmarkFunctions;
CREATE TRANSFORM FUNCTION linregr AS LANGUAGE 'R' NAME 'linregrFactory' LIBRARY BenchmarkFunctions;
CREATE TRANSFORM FUNCTION biclust AS LANGUAGE 'R' NAME 'biclustFactory' LIBRARY BenchmarkFunctions;
CREATE TRANSFORM FUNCTION svd AS LANGUAGE 'R' NAME 'svdFactory' LIBRARY BenchmarkFunctions;
CREATE TRANSFORM FUNCTION wilcox AS LANGUAGE 'R' NAME 'wilcoxFactory' LIBRARY BenchmarkFunctions;

-----------------
-- Covariance ---
-----------------
CREATE LOCAL TEMP TABLE covariance ON COMMIT PRESERVE ROWS AS /*+direct*/ 
WITH exp_data AS (
     SELECT g.patientid AS row_num, g.geneid AS col_num, g.expr_value AS val
     FROM geo g, patients p WHERE g.patientid = p.id AND p.disease = 5
)
SELECT covar(row_num, col_num, val) OVER() FROM exp_data;

WITH cov_filt AS (
     SELECT row_num, col_num FROM covariance WHERE val > 0.01 * (SELECT max(val) FROM covariance)
)
SELECT g1.*, g2.* FROM genes g1, genes g2, cov_filt c WHERE g1.geneid = c.row_num AND g2.geneid = c.col_num;

------------------------
-- Linear Regression ---
------------------------
DROP VIEW IF EXISTS exp_data CASCADE;
CREATE VIEW exp_data AS
     SELECT g.patientid AS row_num, g.geneid AS col_num, g.expr_value AS val
     FROM geo g, genes ge WHERE g.geneid = ge.geneid AND ge.func < 250
     UNION
     SELECT id AS row_num, (SELECT max(geneid) FROM genes) + 1 AS col_num, response AS val
     FROM patients;

SELECT linregr(row_num, col_num, val) OVER() FROM exp_data;

-------------------
-- Biclustering ---
-------------------
DROP VIEW IF EXISTS exp_data CASCADE;
CREATE VIEW exp_data AS
      SELECT g.patientid AS row_num, g.geneid AS col_num, g.expr_value AS val
      FROM geo g, patients p WHERE g.patientid = p.id AND p.age <= 40 AND p.gender = 1;

SELECT biclust(row_num, col_num, val) OVER() FROM exp_data;

----------
-- SVD ---
----------
DROP VIEW IF EXISTS exp_data CASCADE;
CREATE VIEW exp_data AS
     SELECT g.patientid AS row_num, g.geneid AS col_num, g.expr_value AS val
     FROM geo g, genes ge WHERE g.geneid = ge.geneid AND ge.func < 250;

SELECT svd(row_num, col_num, val) OVER() FROM exp_data;

-----------------------------
-- Wilcoxon Rank Sum Test ---
-----------------------------
CREATE LOCAL TEMP TABLE patients_filt ON COMMIT PRESERVE ROWS AS /*+direct*/
SELECT id FROM patients WHERE id < 0.0025 * (SELECT max(id) FROM patients);

SELECT gm.goid, g.patientid, wilcox(gm.belongs, g.expr_value) OVER(PARTITION BY gm.goid, g.patientid)
       FROM geo g, go_matrix gm, patients_filt p
       WHERE g.geneid = gm.geneid
       AND g.patientid = p.id
       AND gm.goid < 10000
       ORDER BY gm.goid, g.patientid;

SELECT gm.goid, g.patientid, wilcox(gm.belongs, g.expr_value) OVER(PARTITION BY gm.goid, g.patientid)
       FROM geo g, go_matrix gm, patients_filt p
       WHERE g.geneid = gm.geneid
       AND g.patientid = p.id
       AND gm.goid < 20000
       AND gm.goid >= 10000
       ORDER BY gm.goid, g.patientid;

SELECT gm.goid, g.patientid, wilcox(gm.belongs, g.expr_value) OVER(PARTITION BY gm.goid, g.patientid)
       FROM geo g, go_matrix gm, patients_filt p
       WHERE g.geneid = gm.geneid
       AND g.patientid = p.id
       AND gm.goid < 30000
       AND gm.goid >= 20000
       ORDER BY gm.goid, g.patientid;

SELECT gm.goid, g.patientid, wilcox(gm.belongs, g.expr_value) OVER(PARTITION BY gm.goid, g.patientid)
       FROM geo g, go_matrix gm, patients_filt p
       WHERE g.geneid = gm.geneid
       AND g.patientid = p.id
       AND gm.goid < 40000
       AND gm.goid >= 30000
       ORDER BY gm.goid, g.patientid;

SELECT gm.goid, g.patientid, wilcox(gm.belongs, g.expr_value) OVER(PARTITION BY gm.goid, g.patientid)
       FROM geo g, go_matrix gm, patients_filt p
       WHERE g.geneid = gm.geneid
       AND g.patientid = p.id
       AND gm.goid < 50000
       AND gm.goid >= 40000
       ORDER BY gm.goid, g.patientid;

SELECT gm.goid, g.patientid, wilcox(gm.belongs, g.expr_value) OVER(PARTITION BY gm.goid, g.patientid)
       FROM geo g, go_matrix gm, patients_filt p
       WHERE g.geneid = gm.geneid
       AND g.patientid = p.id
       AND gm.goid >= 50000
       ORDER BY gm.goid, g.patientid;
