from etl.utilPrograms.assignment1Utils import *
import unittest2

class testCases(unittest.TestCase):
    def testSelectStatement(self):
        def checkDF(spark):
            Schema = StructType([StructField('firstName', StringType(), True),\
                                 StructField('lastName', StringType(), True),\
                                 StructField('salary', LongType(), True)])
            data = [("James","Smith",3000), \
                    ("Michael","",20000), \
                    ("Robert","Williams",3000),\
                    ("Maria","Jones",11000),\
                    ("Jen","Brown",10000)]
            df = spark.createDataFrame(data=data,schema=Schema)
            return df
        self.assertEqual(selectStatement().collect(),checkDF(spark).collect())

if __name__ == 'main':
    unittest.main()



