import sys
import random

#create GEO matrix: min and max got from actual GEO data
def createGEOMatrix(nrow, ncol):
    f = open('GEO-' + str(nrow) + '-' + str(ncol) + '.txt', 'w')
    f.write("geneid, patientid, expression value\n")
    for i in range(nrow):
	for j in range(ncol):
	    r = random.uniform(-186677, 2005274)
	    f.write("%d, %d, %.2f\n" %(i, j, r))	

#create GO matrix: assume 60K terms. Currently randomly distributed
def createGOMatrix(nrow, ncol):
    f = open('GO-' + str(nrow) + '-' + str(ncol) + '.txt', 'w')
    f.write("geneid, goid, whether gene belongs to go\n")
    for i in range(nrow):
        for j in range(nGO):
            t = random.randint(0, 1)
            f.write("%d, %d, %d\n" %(i, j, t))

#create gene metadata matrix: gene id (same as in GEO and GO matrix -- currently just an index)
#target gene if any (again an index)
#position
#length
#function (an index for now)
def createGeneMetadataMatrix(nrow, ncol):
    f = open('GeneMetaData-' + str(nrow) + '-' + str(ncol) + '.txt', 'w')
    f.write("id, target, position, length, function\n")
    for i in range(nrow):
        t = random.randint(0, nrow-1) if random.random() < 0.5 else -1 #target gene -- not every gene has a target gene
        p = random.randint(0, nbases-1)# position
        l = random.randint(min_probe_length, max_probe_length)#length
        func = random.randint(0, 1000)#function
        f.write("%d, %d, %d, %d, %d\n" %(i, t, p, l, func))

#create patient metadata matrix: sample if (same as in GEO matrix)
#age
#gender
#zipcode
#disease
#drug response
def createPatientMetadataMatrix(nrow, ncol):
    f = open('PatientMetaData-' + str(nrow) + '-' + str(ncol) + '.txt', 'w')
    f.write("id, age, gender, zipcode, disease, drug response\n")
    for i in range(ncol):
        a = random.randint(15, 95)
        g = random.randint(0, 1)
        z = random.randint(1, 99999)
        d = random.randint(0, 20)
        dr = random.uniform(0, 100) # ic50 values
        f.write("%d, %d, %d, %d, %d, %.2f\n" %(i, a, g, z, d, dr))

if len(sys.argv) < 3:
    print "provide nrow, ncol"
    sys.exit()
nrow = int(sys.argv[1])
ncol = int(sys.argv[2])
nGO = 60000
nbases = 3000000000
min_probe_length = 25
max_probe_length = 1000 #http://nar.oxfordjournals.org/content/32/12/e99.full
createGEOMatrix(nrow, ncol)
createGOMatrix(nrow, ncol)
createGeneMetadataMatrix(nrow, ncol)
createPatientMetadataMatrix(nrow, ncol)
