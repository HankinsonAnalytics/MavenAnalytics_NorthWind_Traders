#!/usr/bin/env python
# coding: utf-8

# In[1]:


#standard imports
import pandas as pd
import numpy as np


# In[2]:


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


# In[3]:


#creating a working copy of the orders that will serve as the base for my output
output_df = orders_df.copy()
#merging the order details
output_df = output_df.merge(order_details_df, how='left', on='orderID')


# In[4]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original.
customers_renamed = customers_df.copy()
for col in customers_renamed.columns:
    if "customer" not in col:
        customers_renamed = customers_renamed.rename(columns={col : ("customer_"+col)})


# In[5]:


#merging on the customer info
output_df = output_df.merge(customers_renamed, how='left', on='customerID')


# In[6]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
employees_renamed = employees_df.copy()
for col in employees_renamed.columns:
    if "employee" not in col:
        employees_renamed = employees_renamed.rename(columns={col : ("employee_"+col)})


# In[7]:


#merging on the employee info
output_df = output_df.merge(employees_renamed, how='left', on='employeeID')


# In[8]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
shippers_renamed = shippers_df.copy()
for col in shippers_renamed.columns:
    if "shipper" not in col:
        shippers_renamed = shippers_renamed.rename(columns={col : ("shipper_"+col)})


# In[9]:


#merging on the shipper info
output_df = output_df.merge(shippers_renamed, how='left', on='shipperID')


# In[10]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
supervisors_renamed = employees_df.copy()
for col in supervisors_renamed.columns:
        supervisors_renamed = supervisors_renamed.rename(columns={col : ("supervisor_"+col)})


# In[11]:


#merging on the supervisor/team info
output_df = output_df.merge(supervisors_renamed, how='left', left_on='employee_reportsTo', right_on='supervisor_employeeID')


# In[12]:


print(supervisors_renamed.columns)


# In[13]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
managers_renamed = employees_df.copy()
for col in managers_renamed.columns:
        managers_renamed = managers_renamed.rename(columns={col : ("manager_"+col)})


# In[14]:


#merging on the supervisor/team info
output_df = output_df.merge(managers_renamed, how='left', left_on='supervisor_reportsTo', right_on='manager_employeeID')


# In[15]:


print(output_df['manager_reportsTo'].unique())


# In[16]:


print(output_df.columns)


# In[17]:


#creating a working copy of the products table
products_info = products_df.copy()
#merging on the product category info
products_info = products_info.merge(categories_df, how='left', on='categoryID')


# In[18]:


#renaming columns to prevent duplication and enable dissociation. Using a copy to preserve the original. 
for col in products_info.columns:
    if "product" not in col:
        products_info = products_info.rename(columns={col : ("product_"+col)})


# In[19]:


#merging on product info
output_df = output_df.merge(products_info, how='left', on='productID')


# In[20]:


#writing output
output_df.to_csv(r"C:\Desktop\Northwind+Traders\Northwind Traders\Processed_DFs.csv", index=False)


# In[ ]:



 

