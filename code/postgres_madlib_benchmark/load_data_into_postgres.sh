# This script loads data into postgres.
PATH=$1
NGENES=$2
NPATIENTS=$3
DATABASE=$4
GEO="${PATH}/GEO-${NGENES}-${NPATIENTS}.txt"
GO="${PATH}/GO-${NGENES}-${NPATIENTS}.txt"
GENES="${PATH}/GeneMetaData-${NGENES}-${NPATIENTS}.txt"
PATIENTS="${PATH}/PatientMetaData-${NGENES}-${NPATIENTS}.txt"

psql $DATABASE -e -v geo_file="'$GEO'" -v go_file="'$GO'" -v genes_file="'$GENES'" \
    -v patients_file="'$PATIENTS'" -f load_data_into_postgres.sql