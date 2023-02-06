from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("assignment2SQl").getOrCreate()
# Q1
Schema = StructType([StructField("Product", StringType(),True),\
                     StructField("Amount", LongType(),True),\
                     StructField("Country", StringType(),True)])
Data = [("Banana",1000,"USA"),\
        ("Carrots",1500,"INDIA"),\
        ("Beans",1600,"SWEDEN"),\
        ("Orange",2000,"UK"),\
        ("Orange",2000,"UAE"),\
        ("Banana",400,"CHINA"),\
        ("Carrots",1200,"CHINA")]

def createDF(Data,Schema):
    df = spark.createDataFrame(data = Data, schema = Schema)
    return df
# createDF(Data,Schema)

# Q2A
def pivotDF(createDF):
    df = createDF(Data,Schema)
    pivDF = df.groupBy("Product").pivot("Country").sum("Amount")
    pivDF.show()
    return pivDF
# Q2B
def unpivotedDF(pivotDF):
    pivDF = pivotDF(createDF)
    func = "stack(6, 'ind', INDIA, 'chn', CHINA, 'usa',USA,'uk',UK,'uae',UAE,'swd',SWEDEN) as (Country,Total)"
    unPivotDF = pivDF.select("Product", expr(func)).where("Total is not null")
    unPivotDF.show()
    return unPivotDF
# unpivotedDF(pivotDF)
