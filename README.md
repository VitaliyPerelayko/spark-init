# spark-init
to run
* set WORKING_DIR where data is stored
* run the script (replace <part_date> with proper month "2019-07-31" for example)
```commandline
WORKING_FOLDER=""
spark-submit \
    --files "$WORKING_FOLDER*.csv" \
    --py-files job.py \
    first_task.py <part_date>
```