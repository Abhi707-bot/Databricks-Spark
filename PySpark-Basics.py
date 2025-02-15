# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading JSON

# COMMAND ----------

df_json = spark.read.format('json').option('Inferschema', True)\
                    .option('header', True)\
                    .option('multiline', False)\
                    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')        #fs --> to know the filepath 

# COMMAND ----------

df = spark.read.format('csv').option('Inferschema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Defination

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 
                '''

# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header', True)\
            .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC my_strct_schema = ([
# MAGIC                       StructField('Item_Identifier',StringType(),True), 
# MAGIC                       StructField('Item_Weight',StringType(),True), 
# MAGIC                       StructField('Item_Fat_Content',StringType(),True), 
# MAGIC                       StructField('Item_Visibility',StringType(),True), 
# MAGIC                       StructField('Item_MRP',StringType(),True), 
# MAGIC                       StructField('Outlet_Identifier',StringType(),True), 
# MAGIC                       StructField('Outlet_Establishment_Year',StringType(),True), 
# MAGIC                       StructField('Outlet_Size',StringType(),True), 
# MAGIC                       StructField('Outlet_Location_Type',StringType(),True), 
# MAGIC                       StructField('Outlet_Type',StringType(),True), 
# MAGIC                       StructField('Item_Outlet_Sales',StringType(),True)
# MAGIC ])

# COMMAND ----------

# MAGIC %md
# MAGIC df = spark.read.format('csv')\
# MAGIC             .schema(my_strct_schema)\
# MAGIC             .option('header', True)\
# MAGIC             .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(col('item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 2

# COMMAND ----------

df.filter((col('Item_Type')== 'Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ###  withColumn helps to add new column and modify the column in the DF
# MAGIC
# MAGIC ##  Scenerio - 1 (New column in the DF)

# COMMAND ----------

df = df.withColumn('Flag', lit('New')).display()

# COMMAND ----------

df.withColumn('multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 2

# COMMAND ----------

# regexp_replace() --> It is used to replace the column values

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Low Fat','LF')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/orderBy

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 2

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 3

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'], ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 4

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'], ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 1

# COMMAND ----------

df.drop(col('Item_Visibility')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 2

# COMMAND ----------

df.drop('Item_Visibility', 'Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop_Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# Also it called as D_Dup

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union and UnionByName

# COMMAND ----------

# MAGIC %md
# MAGIC ### Predefined DF

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union By Name

# COMMAND ----------

data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Func

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initcap()

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type').alias('Upp_Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Func

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current_Date()

# COMMAND ----------

df = df.withColumn('curr_date',current_date())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date_Add()

# COMMAND ----------

df = df.withColumn('week_after', date_add('curr_date',7))

df.display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DateDiff

# COMMAND ----------

df = df.withColumn('Date_Diff',datediff('week_after','curr_date'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date_Format()

# COMMAND ----------

df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping Nulls

# COMMAND ----------

# any --> uses to drop any null values from any columns
# all --> drop null values which have in the all columns

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

#first checks the null values in the columns if they find null then it will drop only null records of table

# COMMAND ----------

# see half of the rows got deleted after dropping nulls values of records
# but by using this method u will lose ur data so use another method to avoid losing the data

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# it will focused on only 1 columns of table which have any null values in the columns so it will delete records of those perticular columns if any null values encounter in this columns then delete those records only    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filling Nulls

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable',subset=['Outlet_Size']).display()

#It will replace only those null values which is in subset columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split and Indexing

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPLIT

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# we used this command when we want to break the list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array_Contains

# COMMAND ----------

df_exp.display()

#cheking in the col of Outlet_Type whether Type1 is contain or not if yes then yes else no

# COMMAND ----------

df_exp.withColumn('Type1_Flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GroupBY

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio -1

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect_List

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

#collect_list function is an aggregation function that collects values of a column into a list. It is often used when you want to group data by some key and collect all values of a specific column into a list for each group.

#collect_set: Similar to collect_list, but it removes duplicates, so you get unique values only.


# COMMAND ----------

df_book.groupBy('user').agg(collect_set('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### When-Otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 1

# COMMAND ----------

df_wh = df.withColumn('veg_flag', when(col('Item_Type')=='Meat', 'Non-veg').otherwise('veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-Join

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row_Number()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window



# COMMAND ----------

df.withColumn('Row_Col', row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rank Vs Dens_Rank

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Sum

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()
     

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
     

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTIONS (UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP - 1

# COMMAND ----------

def func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step - 2

# COMMAND ----------

my_udf = udf(func)

# COMMAND ----------

df.withColumn('mynewcol',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV

# COMMAND ----------

df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### APPEND

# COMMAND ----------

df.write.format('csv').mode('append').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

df.write.format('csv').mode('append').option('path','/FileStore/tables/CSV/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite

# COMMAND ----------

df.write.format('csv')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error

# COMMAND ----------


