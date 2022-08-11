import unittest
from etl.core.utility import *

class UtilityTestCases(unittest.TestCase):

    """
    this is is initial method which the testing
    framework will automatically call for every single test we run
    """
    def setUp(self):
        self.spark = SparkSession \
            .builder \
            .appName("unittest") \
            .master("local[2]") \
            .getOrCreate()

    """
     This is test case Note test case starting from test_  
    """
    def test_getMaxValue(self):
        test = [("a", 5),
                ("b", 3),
                ("c", 1),
                ("d", 4)]

        testSchema = ["id", "val"]

        testDF = self.spark.createDataFrame(data=test, schema=testSchema)
        self.assertEqual(getMaxValue(testDF,"val"), 5)


