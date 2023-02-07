from etl.utilPrograms.assignmet2Utils import *
import unittest
class testCases(unittest.TestCase):
    def checkDF(self):
        def makeDF():
            Schema = StructType([StructField("Product", StringType(), True), \
                                 StructField("Amount", LongType(), True), \
                                 StructField("Country", StringType(), True)])
            Data = [("Banana", 1000, "USA"), \
                    ("Carrots", 1500, "INDIA"), \
                    ("Beans", 1600, "SWEDEN"), \
                    ("Orange", 2000, "UK"), \
                    ("Orange", 2000, "UAE"), \
                    ("Banana", 400, "CHINA"), \
                    ("Carrots", 1200, "CHINA")]
            df = spark.createDataFrame(data=Data, schema=Schema)
            return df
        self.assertEqual(createDF(Data,Schema).collect(),makeDF().collect())

    def testPivotDF(self):
        def checkDF():
            Schema =StructType([StructField('Product', StringType(), True),\
                                StructField('CHINA', LongType(), True),\
                                StructField('INDIA', LongType(), True),\
                                StructField('SWEDEN', LongType(), True),\
                                StructField('UAE', LongType(), True),\
                                StructField('UK', LongType(), True),\
                                StructField('USA', LongType(), True)])

            data = [("Orange",None,None,None,2000,2000,None),\
                    ("Beans",None,None,None,2000,2000,None)\
                    ("Banana",None,None,None,2000,2000,None)\
                    ("Carrots",None,None,None,2000,2000,None)]
            df = spark.createDataFrame(data = data, schema=Schema)
            return df
        self.assertEqual(pivotDF(createDF).collect(),checkDF().collect())
    def testunpivotedDF(self):
        def checkDF():
            Schema = StructType([StructField('Product', StringType(), True),\
                                 StructField('Country', StringType(), True),\
                                 StructField('Total', LongType(), True)])
            data = [("Orange","uk",2000),\
                    ("Orange","uae",2000),\
                    ("Orange","swd",1600),\
                    ("Orange","chn",400),\
                    ("Orange","usa",1000),\
                    ("Orange","ind",1500),\
                    ("Orange","chn",1200)]
            df = spark.createDataFrame(data=data,schema=Schema)
            return df
        self.assertEqual(unpivotedDF(pivotDF).collect(),checkDF().collect())



if __name__ == "main":
    unittest.main()