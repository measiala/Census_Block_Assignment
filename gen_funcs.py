#!/usr/bin/python3

import os.path

def create_directory(mydir):
    if not os.path.exists(mydir):
        try:
            os.makedirs(mydir)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise

def is_number(s):
    try:
        complex(s)
    except ValueError:
        return False
    return True

def is_integer(s):
    try:
        int(s)
    except ValueError:
        return False
    return True

param_def = {
    'YYYY':   'Year of dataset (4-digit equal to 2005-current)',
    'PER':    'Period of dataset (1-digit equal to 1/3/5)',
    'TBLID':  'Table ID or starting characters (e.g.,B25001,B25,etc.)',
    'SUMLVL': 'Summary Level (3-digits)',
    'CMP':    'Summary Level Component (2-digits) [not supported]',
    'ST':     'FIPS State Code (2-digits) [not supported]'
}

def process_args(argv,mand_list,opt_list=[]):
    arg_list = argv[1:]
    valid_set = set(mand_list + opt_list)
    mand_set = set(mand_list)
    return_list = []
    if arg_list[0] == '-h' or arg_list[0] == '--help':
        print("Usage: " + argv[0] + " [ -h | --help ]")
        print("       " + argv[0] + " [Required Arguments] [Optional Arguments]")
        print("Where:")
        if len(mand_list) > 0:
            print("    Required:")
            for mand in mand_list:
                print("       "+ mand + " is " + param_def[mand])
        if len(opt_list) > 0:
            print("    Optional:")
            for opt in opt_list:
                print("       "+ opt  + " is " + param_def[opt])
        exit(1)
    elif len(arg_list) < len(mand_list) or len(arg_list) > len(valid_set):
        print('Incorrect number of arguments provided.')
        exit(1)
    else:
        param_set = set()
        for arg in arg_list:
            if not '=' in arg:
                print('Parameter ' + arg + ' is not defined correctly.')
                print('Parameters must be passed as PARM=Value.')
                exit(1)
            else:
                [ param, value ] = arg.split(sep='=',maxsplit=1)
                if param not in valid_set:
                    print("The parameter " + param + " is not among the valid set of parameters " + valid_set)
                    exit(2)
                else:
                    param_set.add(param)
                    if param == 'YYYY':
                        if int(value) in range(2005,2017):
                            YYYY = value
                        else:
                            print("The year " + value + " is not between 2005 and 2016.")
                            exit(2)
                    elif param == 'PER':
                          if value in ['1','3','5']:
                            PER = value
                          else:
                            print("Period must be equal to (1/3/5).")
                            exit(2)
                    elif param == 'SUMLVL':
                        # Fix this check
                        if value in ['010','040','050','140','150']:
                            SUMLVL = value
                        else:
                            print("Summary level is not a legal value.")
                            exit(2)
                    elif param == 'CMP':
                        if not len(value) == 2:
                            print("Illegal component code.")
                            exit(2)
                    elif param == 'ST':
                        # Fix this check
                        if int(value) > 0 and int(value) < 73:
                            ST = value
                        else:
                            print("Illegal FIPS State code.")
                            exit(2)
                    print(param + ' = ' + value)
                    return_list.append([param, value])
        if not mand_set.issubset(param_set):
            print("Not all mandatory arguments were provided.")
            print("Specifically, the following parameters were missing: " + (mand_set - param_set))
            exit(3)
        if 'PER' in locals() or 'PER' in globals():
            if 'YYYY' in locals() or 'YYYY' in globals():
                if PER == '5' and int(YYYY) < 2009:
                    print("ACS 5-year data (PER=5) exists only for YYYY=2009 or later.")
                    exit(2)
                elif PER == '3' and not (int(YYYY) > 2006 and int(YYYY) < 2014):
                    print("ACS 3-year data (PER=3) exists only for YYYY=2007-2013.")
                    exit(2)
                if 'CMP' in locals() or 'CMP' in globals():
                    INPUTPATH = './input/' + YYYY + '/' + PER + '/'
                    with open(INPUTPATH + 'sumlvl_dict.csv','r') as infile:
                        incsv = csv.reader(infile)
                        sumlvl_dict = {}
                        for row in incsv:
                            sumlvl_dict[row[0]] = row[1]
                else:
                    # if CMP is not defined then set default component to 00
                    return_list.append(['CMP','00'])
            else:
                # This is only if mandatory list is misspecified.
                print("The parameter PER must be used in conjunction with YYYY.")
                exit(2)
        return return_list


