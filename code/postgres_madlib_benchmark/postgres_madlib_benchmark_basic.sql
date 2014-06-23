 -- set timing on
\timing on
----------------------
-- Matrix multiply ---
----------------------
WITH all_cols AS (
       SELECT col_num, array_agg(vals ORDER BY row_num) AS col_vals
       FROM exp_data
       GROUP BY col_num
)
SELECT madlib.array_dot(i.col_vals::float8[], j.col_vals::float8[]), 
         i.col_num AS row_num, j.col_num AS col_num
       FROM all_cols i, all_cols j;

----------
-- SVD ---
----------
-- Compute a rank 3 approximation of the SVD
SELECT madlib.svdmf_run( 'exp_data', 'col_num', 'row_num', 'vals', 3);
