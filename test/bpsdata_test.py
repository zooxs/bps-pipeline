import unittest
from BPSPipeline.bpsmodule import *


class BPSPipelineTestCase(unittest.TestCase):
    def test_csv_file_attributes(self):
        """Several test for csv file to check its attributes."""
        df = BPSData("data/input/csv/dataset1.csv", separator="_")
        self.assertIn(df.extension, df.LIST_EXTENSION)
        self.assertEqual(df.title, "Produksi_Buah-Buahan")
        self.assertEqual(df.unit, "Kuintal")
        self.assertEqual(df.group, "Buah-Buahan")
        self.assertListEqual(
            df.pipeline()["type"].unique().tolist(),
            ["Salak", "Mangga", "Durian", "Jeruk", "Pisang", "Pepaya", "Nanas"],
        )
        self.assertListEqual(
            df.year,
            [2018, 2019, 2020],
        )

    def test_bulk_parse(self):
        """Test for bulking parse."""
        bulk = BulkParse("data/input/csv/*csv", "data/output/csv/", separator="_")
        listCombined = bulk.combineResult()
        self.assertListEqual(
            listCombined[0].columns.to_list(), [2015, 2016, 2017, 2018, 2019, 2020]
        )

    def test_excel_file(self):
        """Test for excel file."""
        df = BPSData("data/input/excel/dataset1.xlsx")
        self.assertIn(df.extension, df.LIST_EXTENSION)


if __name__ == "__main__":
    unittest.main()
