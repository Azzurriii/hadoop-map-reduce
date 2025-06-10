# Upload file to HDFS
hdfs dfs -mkdir -p /user/vothanh/question6/input/test1
hdfs dfs -mkdir -p /user/vothanh/question6/input/test2
hdfs dfs -mkdir -p /user/vothanh/question6/input/test3
hdfs dfs -put test/test1/input/* /user/vothanh/question6/input/test1
hdfs dfs -put test/test2/input/* /user/vothanh/question6/input/test2
hdfs dfs -put test/test3/input/* /user/vothanh/question6/input/test3

# Remove output directory
hdfs dfs -rm -r /user/vothanh/question6/output
# Create output directory
hdfs dfs -mkdir -p /user/vothanh/question6/output


# Run MapReduce Job
hadoop jar question6.jar Question6 /user/vothanh/question6/input/test1 /user/vothanh/question6/output/test1 10
hadoop jar question6.jar Question6 /user/vothanh/question6/input/test2 /user/vothanh/question6/output/test2 15
hadoop jar question6.jar Question6 /user/vothanh/question6/input/test3 /user/vothanh/question6/output/test3 50

# View result
hdfs dfs -cat /user/vothanh/question6/output/test1/part-r-00000
hdfs dfs -cat /user/vothanh/question6/output/test2/part-r-00000
hdfs dfs -cat /user/vothanh/question6/output/test3/part-r-00000