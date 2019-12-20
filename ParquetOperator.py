# -*- coding: UTF-8 -*-

import pyarrow.parquet as pq
import os
import time


class ParquetOperator:
    def __init__(self, parquet_path):
        self.parquet_path = parquet_path
        self.schema = None
        self._get_schema()

    def _get_schema(self):
        if not self.schema:
            self.schema = pq.read_schema(self.parquet_path)
            cols = list(self.schema.names)
            if cols[-1] == "__index_level_0__":
                cols.pop(-1)
            self.columns = cols

    def showColumns(self):
        print("Column count: %d" % len(self.schema.names))
        for n in self.schema.names:
            print(n)

    def summary(self):
        print("parquet path: %s" % self.parquet_path)
        self._get_schema()
        print(self.schema)

    def selectCol2csv(self, destfolder, cols):
        notin = False
        if not cols or len(cols) == 0:
            return
        for c in cols:
            if c not in self.schema.names:
                print("Column %s not found in the parquet!" % c)
                notin = True
        if notin:
            return
        splitname = '-'.join(cols)
        fullname = destfolder + os.path.sep + splitname + ".csv"
        table = pq.read_table(self.parquet_path, columns=cols)
        p = table.to_pandas()
        p.to_csv(path_or_buf=fullname, index=False)

    def splitParquet2csv(self, destfolder, colstep=3):
        for i in range(0, len(self.columns), colstep):
            current_cols = self.columns[i:i+colstep]
            self.selectCol2csv(destfolder, current_cols)

    def mergeCSV(self, folder, linelimit, colstep=3):
        cols = self.columns
        fname = time.strftime("merged_%Y%m%d%H%M%S.csv", time.localtime())
        mf = open(folder + os.path.sep + fname, "wb")
        fdict = {}
        for i in range(0, len(cols), colstep):
            current_cols = cols[i:i + colstep]
            splitname = '-'.join(current_cols)
            fname = folder + os.path.sep + splitname + ".csv"
            fh = open(fname, "r")
            fdict[splitname] = fh
        con = True
        j = 0
        while con:
            j += 1
            if j > linelimit:
                break
            mline = ""
            for i in range(0, len(cols), colstep):
                current_cols = cols[i:i + colstep]
                splitname = '-'.join(current_cols)
                line = fdict[splitname].readline()
                if line and line.strip() != '':
                    mline += str(line.strip()) + ","
                else:
                    con = False
            mline = mline[:-1] + "\n"
            mf.write(mline.encode(encoding="utf-8"))
        mf.close()
        for v in fdict.values():
            v.close()
