#!/usr/bin/env python
# coding: utf-8

# ## All necesaary Imports

# In[1]:


from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import date


# In[2]:


spark = SparkSession.builder \
        .appName('ETL_Pyspark') \
        .master("local[3]") \
        .getOrCreate()


# ## Reading using Pyspark

# In[3]:


dframe = spark.read.csv('source_file/raw/orders.csv',header=True)


# In[4]:


dframe.printSchema()


# ## Replaced Ship mode 2 values with null

# In[5]:


dframe = dframe.withColumn('Ship Mode',when((dframe['Ship Mode']=='unknown'),None).when((dframe['Ship Mode']=='Not Available'),None).otherwise(dframe['Ship Mode']))


# In[6]:


dframe.select(col('Ship Mode')).distinct().show()


# ## Conversion of Columns name to lowercase

# In[7]:


temp_list=[]
for var1 in dframe.columns:
    var1 = var1.lower()
    var1 = var1.replace(' ','_')
    temp_list.append(var1)

for var2 in range(len(dframe.columns)):
    dframe = dframe.withColumnRenamed(dframe.columns[var2],temp_list[var2])


# In[8]:


dframe.show(5)


# ## Adding Extra columns for analysis

# ### Discount given

# In[9]:


dframe = dframe.withColumn('discount_given',round((dframe['list_price']*dframe['discount_percent']*0.01),2))


# In[10]:


dframe.show(5)


# ### Sale price

# In[11]:


dframe = dframe.withColumn('sale_price',dframe['list_price']-dframe['discount_given'])


# In[12]:


dframe.show(5)


# ### Profit

# In[13]:


dframe = dframe.withColumn('profit',round((dframe['sale_price']-dframe['cost_price']),2))


# In[14]:


dframe.show(5)


# ### Adding File date

# In[15]:


dframe = dframe.withColumn('file_date',lit(date.today()))


# In[16]:


dframe.show(5)


# ### converted orderdate to date type

# In[17]:


dframe = dframe.withColumn('order_date', to_date(dframe['order_date'],'yyyy-MM-dd'))


# ### Dropping columns which is not required

# In[18]:


dframe = dframe.drop('cost_price','list_price','discount_percent')


# In[19]:


dframe.show(5)


# ## Connecting to SQL Server

# In[23]:


dbinfo=spark.read.csv('C:/Users/Apromit/dbdetails.csv',header=True)


# In[30]:


servername= dbinfo.collect()[0][0]
dbname = dbinfo.collect()[0][1]


# In[34]:


import sqlalchemy as sl

db_engine = sl.create_engine(f'mssql://{servername}/{dbname}?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
# 
conection=db_engine.connect()


# ### Converted Pyspark Dataframe to Pandas dataframe because of connectivity issues

# In[35]:


pd_frame = dframe.toPandas()


# In[36]:


pd_frame.to_sql('asd', con=conection, index=False, if_exists='append')


# In[ ]:




