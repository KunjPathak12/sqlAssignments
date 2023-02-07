from etl.utilPrograms.assignment3Utils import *
import unittest
class testCases(unittest.TestCase):
    def testmakeDF(self):
        def checkDF():
            Schema = StructType([StructField("Name", StringType(), True), \
                                 StructField("Department", StringType(), True), \
                                 StructField("Salary", LongType(), True)])

            Data = [("James", "Sales", 3000), \
                    ("Michael", "Sales", 4600), \
                    ("Robert", "Sales", 4100), \
                    ("Maria", "Finance", 3000), \
                    ("Raman", "Finance", 3000), \
                    ("Scott", "Finance", 3300), \
                    ("Jen", "Finance", 3900), \
                    ("Jeff", "Marketing", 3000), \
                    ("Kumar", "Marketing", 2000)]
            df = spark.createDataFrame(data=Data, schema=Schema)
            return df
        self.assertEqual(makeDF(Schema,Data),checkDF().collect())

    def testhighestWageEmp(self):
        def checkDF():
            Schema = StructType([StructField('Name', StringType(), True),\
                                 StructField('Department', StringType(), True),\
                                 StructField('Salary', LongType(), True),\
                                 StructField('dept', StringType(), True),\
                                 StructField('sal', LongType(), True)])
            Data = [("Michael","Sales",4600,"Sales",4600),\
                    ("Jen","Finance",3900,"Sales",3900),\
                    ("Jeff","Marketing",3000,"Sales",3000)]
            df = spark.createDataFrame(data=Data,schema=Schema)
            return df
        self.assertEqual(highestWageEmp(makeTempView).collect(),checkDF().collect())
    def testaggregateDF(self):
        def checkDF():
            Schema = StructType([StructField('department', StringType(), True), StructField('max(salary)', LongType(), True), StructField('min(salary)', LongType(), True), StructField('mean(salary)', DoubleType(), True)])
            data = [("sales",4600,3000,3900.0), \
                    ("Finance",3900,3000,3300.0), \
                    ("Marketing",3000,2000,2500)]
            df = spark.createDataFrame(data=data,schema=Schema)
            return df
        self.assertEqual(aggregateDF().collect(),checkDF().collect())


if __name__ == 'main':
    unittest.main()