from BPSPipeline.bpsmodule import *

import pandas as pd

fileName = "data/input/excel/dataset1.xlsx"
fileName = "data/input/csv/dataset4.csv"
tes = BulkParse("data/input/csv/*csv", "data/output/csv/", separator="_", export=True)

tes.combineResult()
