#!/usr/bin/env python
# coding: utf-8

# In[2]:


#standard imports
import pandas as pd
import numpy as np


# In[3]:


#importing dataframes. Try blocks are for csvs not readable by default utf-8 decoding
categories_df = pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\categories.csv")
try:
    customers_df = pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\customers.csv", encoding='ISO-8859-1')
except UnicodeDecodeError:
    # If the 'ISO-8859-1' encoding didn't work, try 'cp1252' encoding
    customers_df = pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\customers.csv", encoding='cp1252')
employees_df = pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\employees.csv")
order_details_df =  pd.read_csv(r"\Desktop\Northwind+Traders\Northwind Traders\order_details.csv")
orders_df =  pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\orders.csv")
try:
    products_df = pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\products.csv", encoding='ISO-8859-1')
except UnicodeDecodeError:
    # If the 'ISO-8859-1' encoding didn't work, try 'cp1252' encoding
    products_df = pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\products.csv", encoding='cp1252')

shippers_df =  pd.read_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\shippers.csv")


# In[4]:


#creating a working copy of the orders that will serve as the base for my output
output_df = orders_df.copy()
#merging the order details
output_df = output_df.merge(order_details_df, how='left', on='orderID')


# In[7]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original.
customers_renamed = customers_df.copy()
for col in customers_renamed.columns:
    if "customer" not in col:
        customers_renamed = customers_renamed.rename(columns={col : ("customer_"+col)})


# In[8]:


#merging on the customer info
output_df = output_df.merge(customers_renamed, how='left', on='customerID')


# In[9]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
employees_renamed = employees_df.copy()
for col in employees_renamed.columns:
    if "employee" not in col:
        employees_renamed = employees_renamed.rename(columns={col : ("employee_"+col)})


# In[10]:


#merging on the employee info
output_df = output_df.merge(employees_renamed, how='left', on='employeeID')


# In[11]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
shippers_renamed = shippers_df.copy()
for col in shippers_renamed.columns:
    if "shipper" not in col:
        shippers_renamed = shippers_renamed.rename(columns={col : ("shipper_"+col)})


# In[12]:


#merging on the shipper info
output_df = output_df.merge(shippers_renamed, how='left', on='shipperID')


# In[15]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
uplines_renamed = employees_df.copy()
for col in uplines_renamed.columns:
    if "employee" not in col:
        uplines_renamed = uplines_renamed.rename(columns={col : ("upline_"+col)})


# In[16]:


#merging on the supervisor/team info
output_df = output_df.merge(employees_renamed, how='left', left_on='employee_reportsTo', right_on='employeeID')


# In[20]:


#creating a working copy of the products table
products_info = products_df.copy()
#merging on the product category info
products_info = products_info.merge(categories_df, how='left', on='categoryID')


# In[21]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
for col in products_info.columns:
    if "product" not in col:
        products_info = products_info.rename(columns={col : ("product_"+col)})


# In[22]:


#merging on product info
output_df = output_df.merge(products_info, how='left', on='productID')


# In[24]:


#writing output
output_df.to_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\Processed_DFs.csv", index=False)

