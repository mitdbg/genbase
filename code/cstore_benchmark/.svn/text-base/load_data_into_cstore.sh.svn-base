# This script loads data into cstore.
PATH=$1
NGENES=$2
NPATIENTS=$3
DATABASE=$4
UNAME=$5
PWORD=$6
GEO="${PATH}/GEO-${NGENES}-${NPATIENTS}.txt"
GO="${PATH}/GO-${NGENES}-${NPATIENTS}.txt"
GENES="${PATH}/GeneMetaData-${NGENES}-${NPATIENTS}.txt"
PATIENTS="${PATH}/PatientMetaData-${NGENES}-${NPATIENTS}.txt"

# Create the tables
vsql -U $UNAME -w $PWORD -d $DATABASE -e -f ${PATH}/load_data_into_cstore.sql

# Load the data
vsql -U $UNAME -w $PWORD -d $DATABASE -c "COPY geo FROM '$GEO' SKIP 1 DELIMITER ',' DIRECT;"
vsql -U $UNAME -w $PWORD -d $DATABASE -c "COPY go_matrix FROM '$GO' SKIP 1 DELIMITER ',' DIRECT;"
vsql -U $UNAME -w $PWORD -d $DATABASE -c "COPY genes FROM '$GENES' SKIP 1 DELIMITER ',' DIRECT;"
vsql -U $UNAME -w $PWORD -d $DATABASE -c "COPY patients FROM '$PATIENTS' SKIP 1 DELIMITER ',' DIRECT;"

# Analyze statistics                                                                                    
vsql -U $UNAME -w $PWORD -d $DATABASE -c "SELECT ANALYZE_STATISTICS('geo');"
vsql -U $UNAME -w $PWORD -d $DATABASE -c "SELECT ANALYZE_STATISTICS('go_matrix');"
vsql -U $UNAME -w $PWORD -d $DATABASE -c "SELECT ANALYZE_STATISTICS('genes');"
vsql -U $UNAME -w $PWORD -d $DATABASE -c "SELECT ANALYZE_STATISTICS('patients');"
