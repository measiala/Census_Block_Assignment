#!/usr/bin/python3

# Built-in
import sys
import os.path
import time
import csv
import urllib.request
import zipfile
import concurrent.futures

# Third-party
from dbfread import DBF
import xlrd

# Local
from gen_funcs import process_args, create_directory
from gen_fips_lists import load_cty_fips
import config

def download_shp(fips):
    inbase = BASEFILE + fips + SUFXFILE

    inzip  = inbase + '.zip'
    urlzip = BASEURL + inzip
    outzip = os.path.join(ARCHPATH,inzip)

    indbf  = inbase + '.dbf'
    outdbf = os.path.join(INPUTPATH,indbf)

    indict  = 'BlockAssign_' + YYYY + '_' + fips + '.csv'
    outdict = os.path.join(INPUTPATH,indict)
    
    if not os.path.isfile(outdict):
        if not os.path.isfile(outdbf):
            if not os.path.isfile(outzip):
                try:
                    urllib.request.urlretrieve(urlzip,outzip)
                except urllib.error.HTTPError as e:
                    print(e)

            if zipfile.is_zipfile(outzip):
                with zipfile.ZipFile(outzip,'r') as archive:
                    archive.extract(indbf,path=INPUTPATH)

        #### Define as tuple ####
        with DBF(outdbf,recfactory=Record,lowernames=True) as table:
            with open(outdict,'w') as outfile:
                outcsv = csv.writer(outfile)

                block_dict = {}

                block_dict['HEADER'] \
                    = [ 'STATEFP','COUNTYFP','TRACTCE','BLKGRPCE',
                        'COUSUBFP','SUBMCDFP','PLACEFP','CONCTYFP',
                        'CSAFP','CBSAFP','METDIVFP',
                        'CNECTAFP','NECTAFP','NCTADVFP',
                        'CD115FP','SLDUST','SLDLST',
                        'STATEFP10','COUNTYFP10','TRACTCE10','BLOCKCE10',
                        'PUMACE10','UACE10','ZCTA5CE10',
                        'ELSDLEA','SCSDLEA','UNSDLEA',
                        'ANRCFP','AIANNHFP',
                        'AIANNHCE','TTRACTCE','TBLKGPCE' ]

                for r in table:
                    blockid = r.statefp10 + r.countyfp10 + r.tractce10 \
                              + r.blockce10 + r.suffix1ce
                    block_dict[blockid] \
                        = [ r.statefp,r.countyfp,r.tractce,r.blkgrpce,
                            r.cousubfp,r.submcdfp,r.placefp,r.conctyfp,
                            r.csafp,r.cbsafp,r.metdivfp,
                            r.cnectafp,r.nectafp,r.nctadvfp,
                            r.cd115fp,r.sldust,r.sldlst,
                            r.statefp10,r.countyfp10,r.tractce10,r.blockce10,
                            r.pumace10,r.uace10,r.zcta5ce10,
                            r.elsdlea,r.scsdlea,r.unsdlea,
                            r.anrcfp,r.aiannhfp,
                            r.aiannhce,r.ttractce,r.tblkgpce ]
                for key,val in block_dict.items():
                    outcsv.writerow([key,val])
        return 'Exported'
    else:
        return 'Skipped'

def init():
    param_list = process_args(sys.argv,['YYYY'])
    for param in param_list:
        globals()[param[0]] = param[1]

    global ARCHPATH,INPUTPATH,CTYFILE,OUTCTY,BASEURL,BASEFILE,SUFXFILE
    
    ARCHPATH = os.path.join('./webarchive',YYYY)
    INPUTPATH = os.path.join('./input',YYYY)

    create_directory(ARCHPATH)
    create_directory(INPUTPATH)

    CTYFILE = 'fips_cty_list.csv'
    OUTCTY = os.path.join(INPUTPATH,CTYFILE)

    BASEURL = "https://www2.census.gov/geo/tiger/TIGER" + YYYY + "/FACES/"
        
    BASEFILE = "tl_" + YYYY + "_"
    SUFXFILE = "_faces"
    
def main(cty_list):
    with concurrent.futures.ProcessPoolExecutor(max_workers=config.PROCS) \
         as executor:
        for fips, result in zip(cty_list,executor.map(download_shp,cty_list)):
            print( fips + ' is ' + result )

class Record(object):
    def __init__(self, items):
        for (name, value) in items:
            setattr(self, name, value)

if __name__ == '__main__':
    start_time = time.time()

    init()

    fips_cty_list = load_cty_fips(YYYY,CTYFILE,INPUTPATH,ARCHPATH)

    main(fips_cty_list)

    end_time = time.time()
    
    print("Download and extract time %s seconds" % (end_time - start_time))
