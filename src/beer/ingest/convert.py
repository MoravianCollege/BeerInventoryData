
class Converter:

    def __init__(self, products, sizes, categories):
        self.products = products
        self.sizes = sizes
        self.categories = categories

    def convert(self, data):
        # if the file contains any incomplete lines, drop them
        data = data.dropna()

        # Remove fractional part from timestamp
        data['timestamp'] = data['timestamp'].apply(lambda x: x.split('.')[0])

        # If there is a bad line in the file, the type of 'Case Pack' will end up
        # as a float, and writing to the DB will fail.
        data['Case Pack'] = data['Case Pack'].astype(int)

        # Map name, size, and category to integer values
        # Remove commas from the name and then apply the map
        data['Name'] = data['Name'].apply(lambda x: self.products.get_value(x.replace(',', '')))
        data['size'] = data['size'].apply(lambda x: self.sizes.get_value(x))
        data['Category'] = data['Category'].apply(lambda x: self.categories.get_value(x))

        # Rename columns to match database columns
        name_map = {'Name': 'product_id',
                    'size': 'size_id',
                    'Category': 'category_id',
                    'Quantity_Available': 'quantity',
                    'Retail': 'retail',
                    'Case Retail': 'case_retail',
                    'Case Pack': 'case_pack'}
        data = data.rename(columns=name_map)

        # Remove rows that are full duplicates
        data.drop_duplicates(subset=['product_id', 'category_id', 'size_id', 'case_pack', 'quantity',
                                     'retail', 'case_retail'],
                             inplace=True)

        # Aggregate duplicates by adding quantities and taking maximum of retail and case_retail
        data = data.groupby(['product_id', 'category_id', 'size_id', 'case_pack', 'timestamp'],
                            as_index=False).agg({'quantity': 'sum', 'retail': 'max', 'case_retail': 'max'})

        return data
