# Census Block Assignment

The purpose of this project is to create a mapping from each current
census block to the various higher level geographical entities. This
is similar to the Block Assignment Files produced by the Census Bureau
for the 2010 Census but it is defined on the current block.

The source files are the TIGER/Line Shapefile Faces datasets. These
datasets contain all topologically defined polygons along with their
assoicated geographical entities. The attribute datasets in the these
files are then unduplicated by current block to produce a block
assignment file that maps each current block to the higher level
entities.

Currently the output is a current county-level dictionary file of
[key,value] pairs where the key is equal to the 16-digit block id
(state + county + tract + block + suffix). The value is a tuple of
higher level geocodes. The order of the higher level codes is given by
the initial row of the file with key = 'HEADER'.

## Current Requirements:

1. Python3

2. Modules: dbfread, concurrent.futures,csv,xlrd,zipfile

## To-dos:

1. I am not yet wedded to the output format. Alternatively I could
output a simple CSV files with a header row.

2. I plan to add a roll-up to create state-level or a national-level
file for simpler processing.
