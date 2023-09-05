from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating spark session object 
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\SAI\\Downloads\\world_bank.json"
df=spark.read.format('json').option('mode','dropmalformed').load(data)
df1=df.withColumn("majorsector_percent",explode(col("majorsector_percent")))\
    .withcolumn("mjsector_namenode",explode(col("mjsector_namenode")))\
    .withcolumn("mjtheme",explode(col("mjtheme")))\from pyspark.sql import *
    from pyspark.sql.functions import *
    from pyspark.sql.types import *

    # creating spark session object 
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    sc = spark.sparkContext
    data="C:\\Users\\SAI\\Downloads\\world_bank.json"
    df=spark.read.format('json').option('mode','dropmalformed').load(data)
    df1=df.withColumn("majorsector_percent",explode(col("majorsector_percent")))\
                .withcolumn("mjsector_namenode",explode(col("mjsector_namenode")))\
                    .withcolumn("mjtheme",explode(col("mjtheme")))\





