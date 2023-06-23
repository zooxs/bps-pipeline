from BPSPipeline.bpsmodule import *

import pandas as pd

fileName1 = "data/input/excel/dataset1.xlsx"
fileName2 = "data/input/csv/dataset4.csv"

tesParseExcel = BPSData(fileName1)
tesParseCsv = BPSData(fileName2, separator="_")

# print(tesParseExcel.pipeline())
groupedResult = tesParseCsv.pipeline().groupby(by=["Kabupaten/Kota"])
getRegion = lambda Region: Region[0]

# print(groupedResult["Kabupaten/Kota"].unique().apply(getRegion).values)
tesParseCsv.groupedExport(pathOutPut="data/output/csv/test_group/")
# testBulkParse = BulkParse("data/input/csv/*csv", "data/output/csv/", separator="_", export=True)
# testBulkParse.combineResult()
