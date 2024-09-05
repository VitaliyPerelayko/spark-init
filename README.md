## first task

* set WORKING_DIR where data is stored
* run the script (replace <part_date> with proper month "2019-07-31" for example)
```commandline
WORKING_FOLDER=""
spark-submit \
    --files "$WORKING_FOLDER*.csv" \
    --py-files job.py \
    first_task.py <part_date>
```
------------------------------------------------------------------
## word count
* run the script (data_dir path to text files)
```commandline
spark-submit word_count.py <data_dir>
```
------------------------------------------------------------------
## bigrams count
* run the script (data_dir path to text files)
```commandline
spark-submit bigram_count.py <data_dir>
```
-----------------------------------------------------------------
## data mart v2
* run the script 
```commandline
export DATA_FOLDER=<path to folder with data>
export RESULTS_FOLDER=<path to folder where script will sore data_marts>
spark-submit \
    --py-files data_mart_v2_handler.py \
    data_mart_v2_manager.py 2019-08-31 USD
```
-----------------------------------------------------------------
## data mart v3 (spark final task)
* run the script 
```commandline
export DATA_FOLDER=<path to folder with data>
export RESULTS_FOLDER=<path to folder where script will sore data_marts>
spark-submit \
    --py-files data_mart_v3_handler.py \
    data_mart_v3_manager.py 2019-08-31 BYN 4
```