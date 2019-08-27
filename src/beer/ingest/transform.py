
class Transform:
    """
    Transform a line from a CSV file into a optimized representation.
    """

    def __init__(self, products, sizes, categories):
        self.products = products
        self.sizes = sizes
        self.categories = categories

    def transform(self, row):
        """
        Convert a tuple into its optimized version

        Product names are converted to integers from the products table
          commas are removed
        Sizes are converted into integers from the sizes table
        Categories are converted into integers from the categories table

        For all three fields, leading and trailing white space is removed

        Timestamps have the trailing fraction part removed

        :param row: a tuple in the form [name, size, category, quantity_available, case_retail, case_pack, timestamp]
        :return: a tuple in the same order
        """
        name = row[0].strip().replace(',', '')
        size = row[1].strip()
        category = row[2].strip()
        quantity_available = row[3].strip()
        retail = row[4].strip()
        case_retail = row[5].strip()
        case_pack = row[6].strip()
        timestamp = row[7].strip()

        # If the string is unknown a new value will be added to the database
        # For the first file, this is quite slow.  For subsequent files, it is fast.
        name_id = self.products.get_value(name)
        size_id = self.sizes.get_value(size)
        category_id = self.categories.get_value(category)

        datetime_str = self.convert_timestamp(timestamp)

        return [name_id, size_id, category_id, quantity_available, retail, case_retail, case_pack, datetime_str]

    def convert_timestamp(self, timestamp):
        """
        Remove the fraction part form the timestamp
        """
        datetime, trash = timestamp.split('.')
        return datetime
