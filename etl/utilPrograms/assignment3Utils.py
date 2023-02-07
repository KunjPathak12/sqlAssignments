from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("assignment2SQl").getOrCreate()
# Q4
Schema = StructType([StructField("Name",StringType(),True),\
                     StructField("Department",StringType(),True),\
                     StructField("Salary",LongType(),True)])

Data = [("James","Sales",3000),\
("Michael","Sales",4600),\
("Robert","Sales",4100),\
("Maria","Finance",3000),\
("Raman","Finance",3000),\
("Scott","Finance",3300),\
("Jen","Finance",3900),\
("Jeff","Marketing",3000),\
("Kumar","Marketing",2000)]

# Q1
def makeDF(Schema,Data):
    df = spark.createDataFrame(data=Data,schema=Schema)
    df.show()
    return df
# makeDF(Schema,Data)

def makeTempView(makeDF):
    df = makeDF(Schema,Data)
    tempView = df.createOrReplaceTempView("empdata")
    return tempView

# Q3/Q2
def highestWageEmp(makeTempView):
    makeTempView(makeDF)
    df = spark.sql("select * from empdata e join(select department as dept, \
    max(salary) as sal from empdata group by department)d \
    on e.department = d.dept and e.salary  = d.sal")
    df.show()
    return df

# Q5
def aggregateDF():
    makeTempView(makeDF)
    df = spark.sql("select distinct department,max(salary),min(salary),mean(salary)\
    from empdata group by department")
    df.show()
    return df