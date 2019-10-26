from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import *


def f(x):
    gi = 0
    previous_row = 0
    previous_gi = float(0)
    output = []
    for r in x[1]:
        d = r.asDict()
        marks = int(d["Marks"])
        if previous_row == 0:
            d["Growth"] = 0
        else:
            c = int(previous_row)
            d["Growth"] = int(marks - c / c)
        if previous_row <= 0 or previous_gi <= 0:
            d["ExpectedScore"] = float(marks)
        else:
            d["ExpectedScore"] = round(float(marks + round((marks * previous_gi) / 100, 2)),2)
        gi = d["Growth"] - gi
        d["Growth_Increase"] = gi
        # =IF(F2<0,D3,D3+((D3*F2)/100)
        previous_row = marks
        previous_gi = d["Growth_Increase"]
        output.append(Row(**d))
    return output


df = spark.read.option("header", "true").csv("indatas.csv")
outrdd = df.rdd.groupBy(lambda r: r[0] + r[1]).map(lambda av: (av[0], list(av[1]))).map(f).flatMap(lambda x: x)
schema = StructType([StructField("Name", StringType(), True),
                     StructField("Subject", StringType(), True),
                     StructField("Dateon", StringType(), True),
                     StructField("Marks", StringType(), True),
                     StructField("Growth", IntegerType(), True),
                     StructField("Growth_Increase", IntegerType(), True),
                     StructField("ExpectedScore", DoubleType(), True)])
ds = spark.createDataFrame(outrdd, schema)
ds.show()
