from etl.utilPrograms.assignment1Utils import *
import unittest

class testCases(unittest.TestCase):
    # 1
    def testSelectStatement(self):
        def checkDF():
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
        self.assertEqual(selectStatement().collect(),checkDF().collect())
    # 2
    def testcreateColumns(self):
        def checkDF():
            Schema= StructType([StructField('firstName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('Country', StringType(), False),\
                                StructField('Department', StringType(), False),\
                                StructField('age', StringType(), False)])

            data = [("James", "Smith", 3000,"","",""), \
                    ("Michael", "", 20000,"","",""), \
                    ("Robert", "Williams", 3000,"","",""), \
                    ("Maria", "Jones", 11000,"","",""), \
                    ("Jen", "Brown", 10000,"","","")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df

        self.assertEqual(createColumns().collect(), checkDF().collect())
    # 3
    def testupdateColumns(self):
        def checkDF():
            Schema= StructType([StructField('firstName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('salary', LongType(), True)])
            data = [("James", "Smith", 3000,"","",""), \
                    ("Michael", "", 20000,"","",""), \
                    ("Robert", "Williams", 3000,"","",""), \
                    ("Maria", "Jones", 11000,"","",""), \
                    ("Jen", "Brown", 10000,"","","")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(updateColumns().collect(), checkDF().collect())
    # 4
    def testcastColumn(self):
        def checkDF():
            Schema= StructType([StructField('salary', StringType(), True),\
                                StructField('dob', StringType(), True)])

            data = [(3000,""), \
                    (20000,""), \
                    (3000,""), \
                    (11000,""), \
                    (10000,"")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(updateColumns().collect(), checkDF().collect())
    # 6
    def testhighestWageEmp(self):
        def checkDF():
            Schema= StructType([StructField('firstName', StringType(), True),\
                                StructField('middleName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('salary', LongType(), True)])

            data = [("James", "Smith", 3000,"","",""), \
                    ("Michael", "", 20000,"","",""), \
                    ("Robert", "Williams", 3000,"","",""), \
                    ("Maria", "Jones", 11000,"","",""), \
                    ("Jen", "Brown", 10000,"","","")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(updateColumns().collect(), checkDF().collect())

    # 7
    def testmaxSalary(self):
        def checkDF():
            Schema= StructType([StructField('firstName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('Country', StringType(), False),\
                                StructField('Department', StringType(), False),\
                                StructField('age', StringType(), False)])

            data = [("James", "Smith", 3000,"","",""), \
                    ("Michael", "", 20000,"","",""), \
                    ("Robert", "Williams", 3000,"","",""), \
                    ("Maria", "Jones", 11000,"","",""), \
                    ("Jen", "Brown", 10000,"","","")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(updateColumns().collect(), checkDF().collect())
    # 7
    def testorderBy(self):
        def checkDF():
            Schema= StructType([StructField('dob', LongType(), True),\
                                StructField('gender', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('firstName', StringType(), True),\
                                StructField('middleName', StringType(), True),\
                                StructField('lastName', StringType(), True)])

            data = [("James", "Smith", 3000,"","",""), \
                    ("Michael", "", 20000,"","",""), \
                    ("Robert", "Williams", 3000,"","",""), \
                    ("Maria", "Jones", 11000,"","",""), \
                    ("Jen", "Brown", 10000,"","","")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(updateColumns().collect(), checkDF().collect())
    # 8
    def testdropColumn(self):
        def checkDF():
            Schema= StructType([StructField('gender', StringType(), True),\
                                StructField('firstName', StringType(), True),\
                                StructField('middleName', StringType(), True),\
                                StructField('lastName', StringType(), True)])

            data = [("James", "Smith", 3000,"","",""), \
                    ("Michael", "", 20000,"","",""), \
                    ("Robert", "Williams", 3000,"","",""), \
                    ("Maria", "Jones", 11000,"","",""), \
                    ("Jen", "Brown", 10000,"","","")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(updateColumns().collect(), checkDF().collect())

if __name__ == 'main':
    unittest.main()



