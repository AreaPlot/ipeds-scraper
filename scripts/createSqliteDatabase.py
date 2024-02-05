import os
import sys
import csv
import sqlite3
import argparse


def buildLookupTable(year: int):
    year = str(year)
    data = {}
    with open("./data/dictionary.csv") as csvf:
        reader = csv.DictReader(csvf)
        for row in reader:
            if row["year"] == year:
                if row["dictname"] not in data:
                    data[row["dictname"]] = []
                columndata = {
                    "varnumber": row["varnumber"],
                    "varname": row["varname"],
                    "datatype": row["datatype"],
                    "fieldwidth": row["fieldwidth"],
                    "format": row["format"],
                    "imputationvar": row["imputationvar"],
                    "vartitle": row["vartitle"],
                }
                if columndata['datatype'] == 'N':
                    if columndata["format"] == 'Disc':
                        columndata["type"] = 'INTEGER'
                    else:
                        columndata["type"] = 'REAL'
                else: # should only be "A"
                    columndata["type"] = 'TEXT'

                data[row["dictname"]].append(columndata)
    return data


def createSqliteDatabase(start, stop):
    for i in range(start, stop):
        yeardict = buildLookupTable(i)
        conn = sqlite3.connect(f"data/ipeds{i}.db")
        cur = conn.cursor()
        # define schema
        for table, columns in yeardict.items():
            tbl_statement = f"CREATE TABLE {table} ( "
            column_defs = []
            for col in columns:
                column_defs.append(f"{col['varname']} { col['type'] }")
            tbl_statement += ",\n".join(column_defs) + ");"
            cur.execute(tbl_statement)

            column_names = ",".join([x['varname'] for x in columns])
            column_count = ",".join("?"*len(columns))
            ### data from IPEDS is not clean; many trailing spaces
            ### process the raw/{year} folder of CSVs with the following:
            ### for FILE in *.csv; do iconv -f ISO-8859-1 -t UTF8 $FILE > $FILE.conv; sed -i '' -Ee 's/[[:space:]]*//' $FILE.conv; csvclean $FILE.conv ; rm -f $FILE.conv ; done
            csvfile = f'./raw/{i}/{table}.csv_out.csv' 
            print(csvfile)
            if(os.path.exists(csvfile)):
                sql_statement = f"INSERT INTO {table} ({column_names}) VALUES ({column_count});"
                print(sql_statement)
                # import data
                with open(csvfile) as csvf:
                    csvreader = csv.DictReader(csvf)
                    rows = []
                    try:
                        for row in csvreader:
                            current_row = []
                            for col in [x['varname'] for x in columns]:
                                current_row.append(row[col])
                            rows.append(current_row)
                    except Exception as e:
                        print(col)
                        print(row)
                        sys.exit(2)
                    cur.executemany(sql_statement, rows)
                conn.commit()
        conn.close()                



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start", help="start year", type=int)
    parser.add_argument("stop", help="stop year", type=int)
    args = parser.parse_args()

    createSqliteDatabase(args.start, args.stop)
    #print(buildLookupTable(args.start)['hd2022'])
