-- set timing on
\timing on
-----------------
-- Covariance ---
-----------------
WITH exp_data AS (
     SELECT g.patientid AS row_num, g.geneid AS col_num, g.expr_value AS val
     FROM geo g, patients p WHERE g.patientid = p.id AND p.disease = 5
),
covariance AS (
     WITH deviation_scores AS (
          SELECT e.col_num, array_agg((e.val - a.avg_val) ORDER BY row_num) AS col_vals 
          FROM exp_data e, (
               SELECT col_num, AVG(val) AS avg_val
               FROM exp_data
               GROUP BY col_num) AS a
          WHERE e.col_num = a.col_num
          GROUP BY e.col_num
     )
     SELECT madlib.array_dot(i.col_vals::float8[], j.col_vals::float8[])/t.total_rows AS cov,
               i.col_num AS row_num, j.col_num AS col_num
          FROM deviation_scores i, deviation_scores j, 
               (SELECT count(row_num) AS total_rows, col_num FROM exp_data GROUP BY col_num LIMIT 1) AS t
),
cov_filt AS (
     SELECT row_num, col_num FROM covariance WHERE cov > 0.01 * (SELECT max(cov) FROM covariance)
)
SELECT g1.*, g2.* FROM genes g1, genes g2, cov_filt c WHERE g1.geneid = c.row_num AND g2.geneid = c.col_num;

-----------------------------
-- Wilcoxon Rank Sum Test ---
-----------------------------
-- Mann-Whitney/Wilcoxon Rank-Sum test
WITH patients_filt AS (
       SELECT id FROM patients WHERE id < 0.01 * (SELECT max(id) FROM patients)
),
mw_source AS (
       SELECT gm.goid AS gm_col, p.id AS pid, gm.belongs AS cat, g.expr_value AS val
       FROM geo g, go_matrix gm, patients_filt p
       WHERE g.geneid = gm.geneid
       AND g.patientid = p.id
)      
SELECT (madlib.mw_test(cat::boolean, val::float8 ORDER BY val)).*, gm_col, pid 
       FROM mw_source GROUP BY gm_col, pid ORDER BY gm_col, pid;

------------------------
-- Linear Regression ---
------------------------
DROP VIEW IF EXISTS exp_data CASCADE;
CREATE VIEW exp_data AS
     SELECT g.patientid AS row_num, g.geneid AS col_num, g.expr_value AS val
     FROM geo g, genes ge WHERE g.geneid = ge.geneid AND ge.func < 250;

WITH indep_columns AS (
       SELECT array_agg(val ORDER BY col_num) AS data_rows, row_num
       FROM exp_data GROUP BY row_num ORDER BY row_num
), dep_column AS (
       SELECT response, id
       FROM patients ORDER BY id
)
SELECT (madlib.linregr(d.response::float8, i.data_rows::float8[])).coef
FROM dep_column d, indep_columns i WHERE d.id = i.row_num;

----------
-- SVD ---
----------
DROP VIEW IF EXISTS exp_data CASCADE;
CREATE VIEW exp_data AS
     SELECT rank() OVER (PARTITION BY g.geneid ORDER BY g.patientid) AS row_num, 
     	    rank() OVER (PARTITION BY g.patientid ORDER BY g.geneid) AS col_num, g.expr_value AS val
     FROM geo g, genes ge WHERE g.geneid = ge.geneid AND ge.func < 250;

-- Compute a rank 50 approximation of the SVD
SELECT madlib.svdmf_run( 'exp_data', 'col_num', 'row_num', 'val', 50);
