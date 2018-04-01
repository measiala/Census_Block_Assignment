#!/usr/bin/python3

import os.path
import urllib.request
import csv

import xlrd

##
## 
##

def load_cty_fips(yyyy,csvfile,csvpath,xlspath):
    # This URL should be verified periodically
    POPSITE = "https://www2.census.gov/programs-surveys/popest/geographies/"

    nyyyy = int(yyyy)

    if nyyyy != 2010 and nyyyy > 2008:
        xlsfile= "all-geocodes-v" + yyyy + ".xlsx"
        if nyyyy < 2015:
            xlsfile.replace('xlsx','xls')
        xlsurl = POPSITE + yyyy + "/" + xlsfile
    else:
        print("The input file is not available for 2010 or prior to 2009.")
        exit(2)

    if not os.path.isdir(csvpath):
        print("%s does not exist." % (csvpath))
        exit(2)
    else:
        csvfullname = os.path.join(csvpath,csvfile)

    if not os.path.isdir(xlspath):
        print("%s does not exist." % (xlspath))
        exit(2)
    else:
        xlsfullname = os.path.join(xlspath,xlsfile)
       
    if not os.path.isfile(csvfullname):
        if not os.path.isfile(xlsfullname):
            try:
                urllib.request.urlretrieve(xlsurl,xlsfullname)
            except urllib.error.HTTPError as e:
                return e
        with xlrd.open_workbook(xlsfullname) as wb:
            ws = wb.sheet_by_index(0)
            fips_cty_set = set()
            for row in range(ws.nrows):
                sumlvl = ws.cell_value(row,0)
                state = ws.cell_value(row,1)
                county = ws.cell_value(row,2)
                if sumlvl == '050':
                    fips_cty_set.add(state + county)
                    fips_cty_list = sorted(list(fips_cty_set))
        with open(csvfullname,'w') as outfile:
            outcsv = csv.writer(outfile)
            outcsv.writerow(fips_cty_list)
        print("%s written to %s" % (csvfile,csvpath))
    else:
        with open(csvfullname,'r') as infile:
            incsv = csv.reader(infile)
            for row in incsv:
                fips_cty_list = row
        print("Existing %s read from %s" % (csvfile,csvpath))
    return fips_cty_list
