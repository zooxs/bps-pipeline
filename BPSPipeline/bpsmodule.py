from pandas import read_csv, read_excel, concat
from glob import glob
from typing import ClassVar
from dataclasses import dataclass


@dataclass
class BPSData:
    r"""
    Instantiate BPS Data.

    :param fileName: BPS data file name.
    :param separator: Character which separates each word.
    :param delimiter: Character which separates data for csv file such as ',' (comma), ';' (semi-colon) or '\\t' (tab).
    :param fullResult: Option for the pipeline result, if True the result return as dictionary contain dataframe itself and its group type for compilation purposes.
    :param delimiter: Character which separates data for csv file.
    :cvar LIST_EXTENSION: List contain allowable file's type.
    :ivar extention: File's extention.
    :ivar readData: Read dataframe.
    :ivar region: Data's region.
    :ivar title: Data's title.
    :ivar group: Data's commodity groups.
    :ivar unit: Data's commodity unit.
    :ivar listRegion: List of region in data.
    :ivar year: Years of data being recorded.
    :ivar commodity: Data's commodities.

    """

    fileName: str
    separator: str = " "
    delimiter: str = ","
    fullResult: bool = False
    LIST_EXTENSION: ClassVar[list] = ["csv", "xlsx", "txt"]

    def __post_init__(self):
        self.extension = self.fileName.split(".")[-1]

        # Check if the file's extension is in the allowable list.
        assert (
            self.extension in self.LIST_EXTENSION
        ), f"Something wrong: file extension {self.extension} not in the allowable list {self.LIST_EXTENSION}"

        # Checking the file's extension and creating a read atrribute corresponding to it.
        if (self.extension == "csv") | (self.extension == "txt"):
            self.readData = read_csv(self.fileName, sep=self.delimiter)
        elif self.extension == "xlsx":
            self.readData = read_excel(self.fileName)

        # Set data attributs such as its title, region, group & unit
        self.region = self.readData.columns[0]

        """
        Check if unit is in the title section instead of its group.
        - Title usually placed at index 1 in columns name. There is 2 condition, either or not the unit include in this section.
        - Group usually placed at title that give information of ...
        - Unit placed either at title or comodity types.
        """
        if "(" in self.readData.columns[1]:
            self.title = self.separator.join(
                self.readData.columns[1].split("(")[0].split(self.separator)[:-1]
            )
            self.group = self.separator.join(self.title.split(self.separator)[1:])
            self.unit = self.readData.columns[1].split("(")[-1][:-1]
        else:
            self.title = self.readData.columns[1]
            self.group = self.separator.join(self.title.split(self.separator)[1:])

        # Remove the data footer & make list of region.
        self.readData = self.readData.iloc[:-5]
        self.listRegion = self.readData[self.region].dropna().values

        # Get the row that contain nan values & extract each year value
        nanRows = self.readData.loc[self.readData.isnull().any(axis=1) == True]
        self.year = nanRows.iloc[-1].dropna().astype("int").unique().tolist()

        # Extract each commodities.
        if nanRows.shape[0] == 1:
            self.comodity = [self.group]
        else:
            self.comodity = nanRows.dropna(axis=1).iloc[0].to_list()

        # Drop the region columns, drop rows that contain nan value & replace '-' with '0' value.
        self.oldColumns = self.readData.columns[1:]
        self.readData = self.readData.dropna(axis=0)[self.oldColumns].replace("-", "0")

    def pipeline(self):
        """
        Methode for cleaning, parsing and transforming data from wide-table format to long-table format.

        :return: DataFrame or Dictionary regarding the value of fullResult
        """
        listComodityData = []
        for numberItem, _ in enumerate(self.oldColumns):
            if (numberItem != len(self.oldColumns) - 1) & (
                numberItem % len(self.year) == 0
            ):
                numberCommodites = int(numberItem / len(self.year))

                # This column indices the value of each commodites each year.
                df = self.readData[
                    self.oldColumns[numberItem : numberItem + len(self.year)]
                ].astype("float")
                df.columns = self.year

                # Set the other columns
                df[self.region] = self.listRegion
                df["group"] = self.group
                df["type"] = (
                    self.group
                    if len(self.comodity) == 1
                    else self.comodity[numberCommodites - 1]
                )
                df["unit"] = self.unit

                # Add each dataframe to the list
                listComodityData.append(df)

        # Combine list of dataframe to one dataframe
        result = concat(listComodityData)

        # Check the value of unit if placed at commodites type
        if self.unit == "":
            expandedColumns = result["type"].str.split("(", expand=True)
            result["unit"] = expandedColumns[1].str.replace(")", "", regex=False)
            result["type"] = expandedColumns[0]

        # Change the title's case to Title Case
        result[self.region] = result[self.region].str.title()

        # Rearrange data columns
        oldCols = result.columns.to_list()
        newCols = oldCols[-4:] + oldCols[: len(oldCols) - 4]
        result = result[newCols].reset_index(drop=True)

        if self.fullResult == True:
            result = result.set_index([self.region, "group", "unit", "type"])
            return {
                "group": self.group,
                "data": result,
            }

        else:
            return result


@dataclass
class BulkParse:
    """
    Class that contain a list of BPS data that will be aggregated based on commodity groups.

    :param pathInput: Input path location for BPS data.
    :param pathOutput: Output path location for result. Default value is the current directory.
    :param separator: Character that which separates each word.
    :param export: Option for export dataframe to csv file.
    """

    pathInput: str
    pathOutput: str = "./"
    separator: str = " "
    export: bool = False

    def __post_init__(self):
        self.listFile = glob(self.pathInput)
        self.listData = list(
            BPSData(fileName, separator=self.separator, fullResult=True).pipeline()
            for fileName in self.listFile
        )

    def combineResult(self):
        """
        Methode for combine result of list dataframe

        :return: List that containing dataframe that has beeen aggregated by commodity group, if export value is False.
        :return: Return none or empty list, instead export aggregated data as csv files.
        """
        listCombinedDf = []
        listUniqueGroup = set([Df["group"] for Df in self.listData])
        for id, uniqueGroup in enumerate(listUniqueGroup):
            sameDf = [Df["data"] for Df in self.listData if Df["group"] == uniqueGroup]
            fileNames = f"{self.pathOutput}/Combine_Result_{id}.csv"
            combineDf = concat(sameDf, axis=1)
            combineDf = combineDf[sorted(combineDf.columns)]
            if self.export == True:
                combineDf.to_csv(fileNames)
                break
            else:
                listCombinedDf.append(combineDf)
        return listCombinedDf
