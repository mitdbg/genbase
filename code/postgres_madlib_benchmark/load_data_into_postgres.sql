-- This file loads expression data from a flat file into a table
-- in postgres

-- Note: the todo's below are not needed for the benchmark
-- to run on small data sets, but may be required for the 
-- database to scale to extremely large datasets

-- set timing on
\timing on

CREATE OR REPLACE FUNCTION copy_safe(spec TEXT, "file" TEXT) RETURNS void AS
$$
BEGIN
        EXECUTE 'COPY ' || spec || ' FROM ' || quote_literal(file) || ' CSV HEADER';
END;    
$$ 
LANGUAGE plpgsql;

BEGIN;

-- Create the geo table to hold the expression data
-- @todo possibly create partitions using table inheritance
DROP TABLE IF EXISTS geo CASCADE;
CREATE TABLE geo (
       geneid integer,     -- row number in the matrix
       patientid integer,  -- column number in the matrix
       expr_value real     -- expression values
);

-- Copy the data from a flat file into the geo table
-- @todo increase checkpoint_segments temporarily?
SELECT copy_safe('geo (geneid, patientid, expr_value)', :geo_file);

COMMIT;

BEGIN;

-- Create the go_matrix table to hold gene ontology data
DROP TABLE IF EXISTS go_matrix CASCADE;
CREATE TABLE go_matrix (
       geneid integer, -- geneid from the geo table
       goid integer,   -- gene ontology id
       belongs integer -- whether the gene belongs to the go category (either 0 or 1)
);

-- Copy the data from a flat file into the go_matrix table
SELECT copy_safe('go_matrix (geneid, goid, belongs)', :go_file);

COMMIT;

BEGIN;

-- Create the genes table with metadata about
-- each gene
DROP TABLE IF EXISTS genes CASCADE;
CREATE TABLE genes (
       geneid integer, -- geneid from the geo table
       target integer, -- gene target
       pos bigint,     -- position in the genome
       len integer,    -- length of the gene       
       func integer    -- function of the gene
);

-- Copy the data from a flat file into the genes table
SELECT copy_safe('genes (geneid, target, pos, len, func)', :genes_file);

COMMIT;

BEGIN;

-- Create the patients table with metadata about
-- each patient
DROP TABLE IF EXISTS patients CASCADE;
CREATE TABLE patients (
       id integer,      -- patientid from the geo table
       age integer,     -- patient age (in years)
       gender integer,  -- patient gender (0 or 1)
       zipcode integer, -- patient's zip code
       disease integer, -- patient's disease
       response float   -- drug response
);

-- Copy the data from a flat file into the patients table
SELECT copy_safe('patients (id, age, gender, zipcode, disease, response)', :patients_file);

COMMIT;

-- Create indices on the tables
-- @todo Increase maintenance_work_mem temporarily? (default is 16 MB)
CREATE INDEX geo_genes_index ON geo (geneid, patientid);
CREATE INDEX geo_patients_index ON geo (patientid, geneid);
-- CLUSTER geo USING geo_patients_index; -- Will this help?
CREATE INDEX go_genes_index ON go_matrix (geneid, goid);
CREATE INDEX go_index ON go_matrix (goid, geneid);
-- CLUSTER go_matrix USING go_index; -- Will this help?
CREATE INDEX genes_index ON genes (geneid);
CLUSTER genes USING genes_index;
CREATE INDEX patients_index ON patients (id);
CLUSTER patients USING patients_index;
VACUUM ANALYZE geo;
VACUUM ANALYZE go_matrix;
VACUUM ANALYZE genes;
VACUUM ANALYZE patients;