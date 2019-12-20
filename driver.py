# -*- coding: UTF-8 -*-

import ParquetOperator
p = ParquetOperator.ParquetOperator('C:\\Temp\\predict_for_zeppelin.parquet')
# p.showColumns()
p.mergeCSV("C:\\Temp\\split_predict", linelimit=2510200)