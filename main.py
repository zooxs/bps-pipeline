from BPSPipeline.bpsmodule import *

import pandas as pd

fileName1 = "data/input/excel/dataset1.xlsx"
fileName2 = "data/input/csv/dataset4.csv"

testParseExcel = BPSData(fileName1)
testParseCsv = BPSData(fileName2, separator="_")


# groupedExport(
#     tesParseExcel, pathOutPut="data/output/excel/test_region/", groupBy="region"
# )
testParseExcel.groupedExport("data/output/excel/test_region")
testBulkParse = BulkParse(
    "data/input/csv/*csv", "data/output/csv", separator="_", export=False
)
