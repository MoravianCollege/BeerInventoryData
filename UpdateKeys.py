# Project: Beer Analysis
# Author: Jonah Beers

import csv

# Open files
filename = './Keys/sizes.csv'
newSize = open('./Keys/newSizes.csv', mode='w+')

# Loop through file, search for unique sizes and remove ones that can be categorized as another
with open(filename, newline='') as f:
    reader = csv.reader(f)
    sizeNewID = 1
    for row in reader:
        sizeCurrID = row[0].strip()
        size = row[1].strip()

        if size != "1/2 KEG ($20 DEP)" and size != "1/2 KEG ($35 DEP)" and size != "1/2 KEG ($50 DEP)" and\
            size != "1/2 KEG ($40 DEP)" and size != "1/6 KEG ($40 DEP)" and size != "1/6 KEG ($50 DEP)" and \
                size != "1/6 KEG ($60 KEG)" and size != "1/6 KEG ($35 DEP)" and size != "1/4 KEG ($35 DEP)":
            newSize.write('"' + str(sizeNewID) + '","' + size + '"' + '\n')
            sizeNewID += 1

# Close files
f.close()

# split column into three separate columns
