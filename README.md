# BeerInventoryData

There are currently 3 scripts: 

- **GenerateKeys** will loop through every beer data file and create key files for unique names, sizes and categories.
- **UpdateKeys** creates another file for sizes that don't include information such as deposites (but this script is not used now).
- **RewriteFiles** loops through every beer data file and will convert everything to a numeric value using the key files and epoch time converter, then rewrites to new files.