# Project: Beer Analysis
# Author: Jonah Beers

import csv
from pathlib import Path

# Open files/directory path
pathlist = Path("./Files").glob('**/*.csv')
n = open('./Keys/names.csv', mode='w+')
s = open('./Keys/sizes.csv', mode='w+')
c = open('./Keys/categories.csv', mode='w+')

# Create dicts for name, size, and category
names = {}
sizes = {}
categories = {}

# Create IDs for name, size, and category
nameID = 1
sizeID = 1
categoryID = 1

# Loop through file and search for unique names, sizes, and categories
for path in pathlist:
    with open(path, newline='') as f:
        reader = csv.reader(f)
        trash = next(reader)
        for row in reader:
            print(row)
            name = row[0].strip()
            size = row[1].strip()
            category = row[2].strip()
            quantity_available = row[3].strip()
            retail = row[4].strip()
            case_retail = row[5].strip()
            case_pack = row[6].strip()
            timestamp = row[7].strip()

            if name not in names:
                names[name] = nameID
                n.write('"' + str(nameID) + '","' + name + '"' + '\n')
                nameID += 1
            if size not in sizes:
                sizes[size] = sizeID
                s.write('"' + str(sizeID) + '","' + size + '"' + '\n')
                sizeID += 1
            if category not in categories:
                categories[category] = categoryID
                c.write('"' + str(categoryID) + '","' + category + '"' + '\n')
                categoryID += 1

# Close files
n.close()
s.close()
c.close()
