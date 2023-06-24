from BPSPipeline.bpsmodule import *

import pandas as pd

fileName1 = "data/input/excel/dataset1.xlsx"
fileName2 = "data/input/csv/dataset4.csv"

tesParseExcel = BPSData(fileName1)
tesParseCsv = BPSData(fileName2, separator="_")


# groupedExport(
#     tesParseExcel, pathOutPut="data/output/excel/test_region/", groupBy="region"
# )

testBulkParse = BulkParse(
    "data/input/csv/*csv", "data/output/csv/", separator="_", export=False
)

print(tesParseCsv.comodity)
