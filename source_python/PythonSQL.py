#!/usr/bin/env python
# coding: utf-8

# # Important imports

# In[ ]:


get_ipython().system('pip install kaggle')


# In[ ]:


import kaggle


# ## Downloading dataset from Kaggle using below link

# In[ ]:


get_ipython().system('kaggle datasets download ankitbansal06/retail-orders -f orders.csv')


# ## Extracting Zip file with Python inbuilt-zip Library

# In[ ]:


import zipfile


# In[ ]:


zip1 = zipfile.ZipFile('orders.csv.zip')


# In[ ]:


zip1.extractall('source_file/raw')


# In[ ]:


zip1.close()


# ## Viewing File with Pandas and doing Exploratory Analysis

# In[1]:


import pandas as pd


# In[15]:


from datetime import date


# ### Reading with using some Nan Parameter

# In[3]:


frame1=pd.read_csv('source_file/raw/orders.csv',na_values=['Not Available', 'unknown'])


# In[4]:


frame1.head()


# In[5]:


frame1.describe()


# ## Seeing unique in a specific Column

# In[6]:


frame1['Ship Mode'].unique()


# ## Renaming Columns

# ### Step 1-- First Converting into Lowercase

# In[7]:


frame1.columns=frame1.columns.str.lower()


# In[8]:


frame1.head()


# ## Step 2 -- replacing space with a Hyphen

# In[9]:


frame1.columns = frame1.columns.str.replace(' ','_')


# In[10]:


frame1.head()


# ## Creating new Columns based on Discount Price and Record date and Sale Price

# In[11]:


frame1['discount_given']=frame1['list_price']*frame1['discount_percent']*0.01
frame1.head()


# In[12]:


frame1['sale_price']=frame1['list_price']-frame1['discount_given']
frame1.head()


# In[13]:


frame1['profit']=frame1['sale_price']-frame1['cost_price']
frame1.head()


# In[16]:


frame1['file_date']=date.today()
frame1.head()


# ### Converting datetime to String

# In[17]:


frame1['order_date']=pd.to_datetime(frame1['order_date'],format='%Y-%m-%d')
frame1['file_date']=pd.to_datetime(frame1['file_date'],format='%Y-%m-%d')


# In[18]:


frame1.head()


# ## Dropping unncessary Columns

# In[19]:


frame1.drop(columns=['cost_price','list_price','discount_percent'], inplace=True)


# In[20]:


frame1.head()


# # Connecting to SQL Server

# In[22]:


dbinfo = pd.read_csv('C:/Users/Apromit/dbdetails.csv')


# In[43]:


servername= dbinfo['server'][0]
dbname = dbinfo['database'][0]


# In[48]:


import sqlalchemy as sl

db_engine = sl.create_engine(f'mssql://{servername}/{dbname}?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
# 
conection=db_engine.connect()


# ## Loading data into MSSQL Server

# In[49]:


frame1.to_sql('test_orders', con=conection, index=False, if_exists='append')


# In[ ]:




