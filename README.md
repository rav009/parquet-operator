# parquet-operator
#### What is Parquet?
Parquet is high-ratio compressed file format wildely used in bigdata systems.

#### Features of this repo
This repo is a parquet handler library which features the function like convert big parquet to small ones so you can use pandas to convert the small parquets to csv files.
When the parquet is too big to directly transform into a CSV using pandas, you can use the compromise method above.

##### Functions
`splitParquet2csv：split a parquet by columns into several small CSV files.` 

`mergeCSV: Merge all the csv-format files into a CSV file.`


#### Requirements
python3.5

`pip install pandas pyarrow`
