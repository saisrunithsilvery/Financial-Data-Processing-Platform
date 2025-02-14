#!/usr/bin/env python3

from zipfile import ZipFile
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from marshmallow import Schema, fields
from dateutil.relativedelta import relativedelta
import json
import os
from pprint import pprint

# Define Schema Classes
class FinancialElementImportDto:
    def __init__(self):
        self.label = ""          #Tag.doc
        self.concept = ""        #Num.tag
        self.info = ""           #Pre.plabel
        self.unit = ""           #Num.uom
        self.value = 0.0         #Num.value

class FinancialsDataDto:
    def __init__(self):
        self.bs = []  #FinancialElementImportDto[] mapping key from Pre.stmt
        self.cf = []  #FinancialElementImportDto[] mapping key from Pre.stmt
        self.ic = []  #FinancialElementImportDto[] mapping key from Pre.stmt

class SymbolFinancialsDto:
    def __init__(self):
        self.startDate = date.today()   #Sub.period 
        self.endDate = date.today()     #Sub.period + Sub.fp
        self.year = 0                   #Sub.fy
        self.quarter = ""               #Sub.fp
        self.symbol = ""                #Sub.cik -> Sym.cik -> Sym.symbol
        self.name = ""                  #Sub.name
        self.country = ""               #Sub.countryma
        self.city = ""                  #Sub.cityma
        self.data = FinancialsDataDto()

class FinancialElementImportSchema(Schema):
    label = fields.String()
    concept = fields.String()
    info = fields.String()
    unit = fields.String()
    value = fields.Int()

class FinancialsDataSchema(Schema):
    bs = fields.List(fields.Nested(FinancialElementImportSchema()))
    cf = fields.List(fields.Nested(FinancialElementImportSchema()))
    ic = fields.List(fields.Nested(FinancialElementImportSchema()))

class SymbolFinancialsSchema(Schema):
    startDate = fields.DateTime()
    endDate = fields.DateTime()
    year = fields.Int()
    quarter = fields.String()
    symbol = fields.String()
    name = fields.String()
    country = fields.String()
    city = fields.String()
    data = fields.Nested(FinancialsDataSchema)

# Helper functions
def npInt_to_str(var):
    return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]

def formatDateNpNum(var):
    dateStr = npInt_to_str(var)
    return dateStr[0:4]+"-"+dateStr[4:6]+"-"+dateStr[6:8]

def formatDateNpNum(var):
    """Improved date formatting function that handles different input types"""
    try:
        if isinstance(var, (int, np.integer)):
            dateStr = str(var)
        else:
            dateStr = str(int(var))  # Convert numpy types to int then string
        
        if len(dateStr) != 8:
            raise ValueError(f"Invalid date format: {dateStr}")
            
        return f"{dateStr[0:4]}-{dateStr[4:6]}-{dateStr[6:8]}"
    except Exception as e:
        raise ValueError(f"Date formatting error: {str(e)} for value {var}")

def main():
    # Read input files
    dirname = "2022q4"
    try:
        with ZipFile('importFiles/'+dirname+'.zip') as myzip:
            with myzip.open('num.txt') as myfile1:
                dfNum = pd.read_table(myfile1,delimiter="\t", low_memory=False)
            with myzip.open('pre.txt') as myfile2:
                dfPre = pd.read_table(myfile2,delimiter="\t")
            with myzip.open('sub.txt') as myfile3:
                dfSub = pd.read_table(myfile3,delimiter="\t", low_memory=False)
            with myzip.open('tag.txt') as myfile4:
                dfTag = pd.read_table(myfile4,delimiter="\t")
    except Exception as e:
        print(f"Error reading input files: {str(e)}")
        return

    try:
        dfSym = pd.read_table('importFiles/ticker.txt', delimiter="\t", header=None, names=['symbol','cik'])
    except Exception as e:
        print(f"Error reading ticker file: {str(e)}")
        return

    print(f"Data sizes: dfNum={dfNum.size}, dfPre={dfPre.size}, dfSub={dfSub.size}, dfTag={dfTag.size}, dfSym={dfSym.size}")

    # Process submissions
    fileStartTime = datetime.now()
    export_path = os.path.join(os.getcwd(), 'export_folders', dirname)
    os.makedirs(export_path, exist_ok=True)  # Create export directory at start
    
    successful_files = 0
    failed_files = 0
    
    for subId in range(len(dfSub)):
        startTime = datetime.now()
        submitted = dfSub.iloc[subId]
        sfDto = SymbolFinancialsDto()
        
        try:
            # Handle dates
            periodStartDate = date.fromisoformat(formatDateNpNum(submitted["period"]))
            sfDto.startDate = periodStartDate
            sfDto.endDate = date.today()

            # Handle year and quarter
            if pd.isna(submitted["fy"]):
                sfDto.year = 0
            else:
                sfDto.year = int(submitted["fy"])
            
            sfDto.quarter = str(submitted["fp"]).strip().upper()

            # Calculate end date based on quarter
            if sfDto.quarter in ["FY", "CY"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+12, days=-1)
            elif sfDto.quarter in ["H1", "H2"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+6, days=-1)
            elif sfDto.quarter in ["T1", "T2", "T3"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+4, days=-1)
            elif sfDto.quarter in ["Q1", "Q2", "Q3", "Q4"]:
                sfDto.endDate = periodStartDate + relativedelta(months=+3, days=-1)
            else:
                print(f"Skipping file due to invalid quarter: {sfDto.quarter}")
                failed_files += 1
                continue

            # Handle symbol
            val = dfSym[dfSym["cik"]==submitted["cik"]]
            if len(val) > 0:
                sfDto.symbol = val["symbol"].iloc[0].strip().upper()  # Use iloc instead of string operations
                if len(sfDto.symbol) > 19 or len(sfDto.symbol) < 1:
                    print(f"Invalid symbol length: {sfDto.symbol}")
                    failed_files += 1
                    continue
            else:
                print(f"No symbol found for CIK: {submitted['cik']}")
                failed_files += 1
                continue

            # Set basic info
            sfDto.name = str(submitted["name"])
            sfDto.country = str(submitted["countryma"])
            sfDto.city = str(submitted["cityma"])

            # Process numerical data
            dfNum['value'] = pd.to_numeric(dfNum['value'], errors='coerce')
            dfNum = dfNum.dropna(subset=['value'])
            dfNum['value'] = dfNum['value'].astype(int)
            filteredDfNum = dfNum[dfNum['adsh'] == submitted['adsh']].copy()
            filteredDfNum.reset_index(drop=True, inplace=True)

            # Process each numerical entry
            for myId in range(len(filteredDfNum)):
                myNum = filteredDfNum.iloc[myId]
                myDto = FinancialElementImportDto()
                
                myTag = dfTag[dfTag["tag"] == myNum['tag']]
                if not myTag.empty:
                    myDto.label = str(myTag["doc"].iloc[0])
                    myDto.concept = str(myNum["tag"])
                    
                    myPre = dfPre[(dfPre['adsh'] == submitted["adsh"]) & (dfPre['tag'] == myNum['tag'])]
                    if not myPre.empty:
                        myDto.info = str(myPre["plabel"].iloc[0]).replace('"',"'")
                        myDto.unit = str(myNum["uom"])
                        myDto.value = int(myNum["value"])

                        # Categorize by statement type
                        stmt_type = str(myPre['stmt'].iloc[0]).strip()
                        if stmt_type == 'BS':
                            sfDto.data.bs.append(myDto)
                        elif stmt_type == 'CF':
                            sfDto.data.cf.append(myDto)
                        elif stmt_type == 'IC':
                            sfDto.data.ic.append(myDto)

            # Serialize and save
            result = SymbolFinancialsSchema().dump(sfDto)
            result = json.dumps(result)
            result = result.replace('\\r', '').replace('\\n', ' ')

            # Save to file
            output_file = os.path.join(export_path, f"{submitted['adsh']}.json")
            with open(output_file, "w") as json_file:
                json_file.write(result)

            successful_files += 1
            
            if successful_files % 100 == 0:  # Progress update every 100 files
                print(f"Processed {successful_files} files successfully...")

        except Exception as e:
            print(f"Error processing file {submitted['adsh']}: {str(e)}")
            failed_files += 1
            continue

    fileEndTime = datetime.now()
    processing_time = (fileEndTime - fileStartTime) / timedelta(seconds=1)
    
    print(f"\nProcessing Complete:")
    print(f"Successfully processed: {successful_files} files")
    print(f"Failed to process: {failed_files} files")
    print(f"Total time: {processing_time:.2f} seconds")
    print(f"Average time per file: {processing_time/max(successful_files, 1):.3f} seconds")

if __name__ == "__main__":
    main()
