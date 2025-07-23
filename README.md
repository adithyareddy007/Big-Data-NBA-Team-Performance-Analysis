Big Data NBA Team Performance Analysis
This project analyzes NBA team performance using big data technologies within the Hadoop ecosystem.
Key Features:
Data Storage: NBA datasets like seasons_stats.csv are stored in the Hadoop Distributed File System (HDFS) for scalable and fault-tolerant storage.
Data Processing: Hadoop MapReduce programs aggregate key performance metrics from the data. For example, 
PlayerAvgPPG.java calculates average points per game for each player, and TeamPointsAggregator.java sums total points for each team.
Performance Analysis: Apache Hive is used to run SQL-like queries (HiveQL) on the data stored in HDFS. This allows for detailed analysis of team and player performance.
