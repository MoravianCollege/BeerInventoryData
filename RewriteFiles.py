# Project: Beer Analysis
# Author: Jonah Beers

import csv
import os
import time

# Open files (only works for reading one file)
directory = './Files/'
destination = './NewFiles/'
filename = '2017-11-30_19:03:48.318079.csv'

namesFile = './Keys/names.csv'
sizesFile = './Keys/sizes.csv'
categoriesFile = './Keys/categories.csv'

# Create dicts for name, size, and category
names = {}
sizes = {}
categories = {}

# Read Key files and save in dicts
with open(namesFile) as namesFile:
    reader = csv.reader(namesFile)
    for row in reader:
        id = row[0].strip()
        name = row[1].strip()
        names[name] = id
with open(sizesFile) as sizesFile:
    reader = csv.reader(sizesFile)
    for row in reader:
        id = row[0].strip()
        size = row[1].strip()
        sizes[size] = id
with open(categoriesFile) as categoriesFile:
    reader = csv.reader(categoriesFile)
    for row in reader:
        id = row[0].strip()
        category = row[1].strip()
        categories[category] = id

# Loop through file and search for unique names, sizes, and categories
with open(directory + filename, newline='') as f:
    newFile = open(destination + filename, mode='w')
    reader = csv.reader(f)
    trash = next(reader)
    for row in reader:
        name = row[0].strip()
        size = row[1].strip()
        category = row[2].strip()
        quantity_available = row[3].strip()
        retail = row[4].strip()
        case_retail = row[5].strip()
        case_pack = row[6].strip()
        timestamp = row[7].strip()

        # Reassign name, size and category with ID
        nameID = names[name]
        categoryID = categories[category]
        if size == "1/2 KEG ($20 DEP)":
            sizeID = sizes["1/2 KEG"]
        if size == "1/2 KEG ($35 DEP)":
            sizeID = sizes["1/2 KEG"]
        if size == "1/2 KEG ($50 DEP)":
            sizeID = sizes["1/2 KEG"]
        if size == "1/2 KEG ($40 DEP)":
            sizeID = sizes["1/2 KEG"]
        if size == "1/6 KEG ($40 DEP)":
            sizeID = sizes["1/6 KEG"]
        if size == "1/6 KEG ($50 DEP)":
            sizeID = sizes["1/6 KEG"]
        if size == "1/6 KEG ($60 KEG)":
            sizeID = sizes["1/6 KEG"]
        if size == "1/6 KEG ($35 DEP)":
            sizeID = sizes["1/6 KEG"]
        if size == "1/4 KEG ($35 DEP)":
            sizeID = sizes["1/4 KEG"]
        else:
            sizeID = sizes[size]

        # Convert time to epoch time
        datetime, trash = timestamp.split('.')
        p = '%Y-%m-%d %H:%M:%S'
        os.environ['TZ'] = 'EST'
        epoch = int(time.mktime(time.strptime(datetime, p)))

        # Write to new file
        newFile.write(nameID + ',' + sizeID + ',' + categoryID + ',' + quantity_available + ',' +
                      retail + ',' + case_retail + ',' + case_pack + ',' + str(epoch) + '\n')
newFile.close()

# Close files
f.close()
namesFile.close()
sizesFile.close()
categoriesFile.close()

# requests library
