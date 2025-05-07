import pandas as pd
import os

HEADERS = {
    'departments': ['department_id', 'department_name'],
    'categories': ['category_id', 'category_department_id', 'category_name'],
    'products': ['product_id', 'product_category_id', 'product_name', 'product_description', 'product_price', 'product_image'],
    'customers': ['customer_id', 'customer_fname', 'customer_lname', 'customer_email', 'customer_password', 'customer_street', 'customer_city', 'customer_state', 'customer_zipcode'],
    'orders': ['order_id', 'order_date', 'order_customer_id', 'order_status'],
    'order_items': ['order_item_id', 'order_item_order_id', 'order_item_product_id', 'order_item_quantity', 'order_item_subtotal', 'order_item_product_price']
}

def read_table_as_df(base_dir, table):
    file_path = os.path.join(base_dir, table, 'part-00000')
    return pd.read_csv(file_path, header=None, names=HEADERS[table])
