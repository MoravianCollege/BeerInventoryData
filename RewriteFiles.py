# Project: Beer Analysis
# Author: Jonah Beers

import csv
import os
import time


class Converter:

    def __init__(self):
        self.names = {}
        self.sizes = {}
        self.categories = {}

    def convert(self, filename):
        # Open files (only works for reading one file)
        directory = './Files/'
        destination = './NewFiles/'

        namesFile = './Keys/names.csv'
        sizesFile = './Keys/sizes.csv'
        categoriesFile = './Keys/categories.csv'

        with open(namesFile) as namesFile:
            reader = csv.reader(namesFile)
            for row in reader:
                id = row[0].strip()
                name = row[1].strip()
                self.names[name] = id
        with open(sizesFile) as sizesFile:
            reader = csv.reader(sizesFile)
            for row in reader:
                id = row[0].strip()
                size = row[1].strip()
                self.sizes[size] = id
        with open(categoriesFile) as categoriesFile:
            reader = csv.reader(categoriesFile)
            for row in reader:
                id = row[0].strip()
                category = row[1].strip()
                self.categories[category] = id

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
                nameID = self.names[name]
                categoryID = self.categories[category]
                sizeID = self.convert_size(size)

                datetime_str = self.convert_timestamp(timestamp)

                # Write to new file
                newFile.write(nameID + ',' + sizeID + ',' + categoryID + ',' + quantity_available + ',' +
                              retail + ',' + case_retail + ',' + case_pack + ',' + str(datetime_str) + '\n')
        newFile.close()

        # Close files
        f.close()
        namesFile.close()
        sizesFile.close()
        categoriesFile.close()


    def convert_size(self, size):
        if size == "1/2 KEG ($20 DEP)":
            sizeID = self.sizes["1/2 KEG"]
        if size == "1/2 KEG ($35 DEP)":
            sizeID = self.sizes["1/2 KEG"]
        if size == "1/2 KEG ($50 DEP)":
            sizeID = self.sizes["1/2 KEG"]
        if size == "1/2 KEG ($40 DEP)":
            sizeID = self.sizes["1/2 KEG"]
        if size == "1/6 KEG ($40 DEP)":
            sizeID = self.sizes["1/6 KEG"]
        if size == "1/6 KEG ($50 DEP)":
            sizeID = self.sizes["1/6 KEG"]
        if size == "1/6 KEG ($60 KEG)":
            sizeID = self.sizes["1/6 KEG"]
        if size == "1/6 KEG ($35 DEP)":
            sizeID = self.sizes["1/6 KEG"]
        if size == "1/4 KEG ($35 DEP)":
            sizeID = self.sizes["1/4 KEG"]
        else:
            sizeID = self.sizes[size]
        return sizeID

    def convert_timestamp(self, timestamp):
        datetime, trash = timestamp.split('.')
        return datetime
        #p = '%Y-%m-%d %H:%M:%S'
        #os.environ['TZ'] = 'EST'
        #epoch = int(time.mktime(time.strptime(datetime, p)))
        #return epoch


if __name__ == '__main__':
    files = os.listdir('Files')
    count = 0
    total = len(files)

    converter = Converter()

    for file in files:
        print('{}/{} ({}): {}'.format(count, total, count/total, file))
        converter.convert(file)
        count += 1
