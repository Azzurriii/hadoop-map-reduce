# Upload file to HDFS
hdfs dfs -mkdir -p /user/vothanh/question7/input/test1
hdfs dfs -mkdir -p /user/vothanh/question7/input/test2
hdfs dfs -mkdir -p /user/vothanh/question7/input/test3
hdfs dfs -put test/test1/input/* /user/vothanh/question7/input/test1
hdfs dfs -put test/test2/input/* /user/vothanh/question7/input/test2
hdfs dfs -put test/test3/input/* /user/vothanh/question7/input/test3

# Remove output directory
hdfs dfs -rm -r /user/vothanh/question7/output
# Create output directory
hdfs dfs -mkdir -p /user/vothanh/question7/output


# Run MapReduce Job
hadoop jar question7.jar Question7 /user/vothanh/question7/input/test1 /user/vothanh/question7/output/test1
hadoop jar question7.jar Question7 /user/vothanh/question7/input/test2 /user/vothanh/question7/output/test2
hadoop jar question7.jar Question7 /user/vothanh/question7/input/test3 /user/vothanh/question7/output/test3

# View result
hdfs dfs -cat /user/vothanh/question7/output/test1/part-r-00000
hdfs dfs -cat /user/vothanh/question7/output/test2/part-r-00000
hdfs dfs -cat /user/vothanh/question7/output/test3/part-r-00000