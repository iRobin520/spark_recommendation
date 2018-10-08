# spark_recommendation
基于机器学习(Spark2.0+MongoDB)实现的协同过滤推荐系统-定时更新推荐结果

此项目包含完整的代码, 只需通过命令行输入：mvn clean package 便可生成可执行的包

具体操作：
1. 打包完成后，将包上传至Spark2.0的环境中。
2. 可通过: standAlone或local或yarn方式来执行，本例以local的方式来提交JOB,如下：

离线推荐

./spark-submit --class com.sh.wcc.OfflineRecommendation --master local --driver-memory 16g --executor-memory 4g --executor-cores 1 --num-executors 6 /data/spark-jars/recommendation-1.0-SNAPSHOT.jar mongodb.ip mongodb.dbName mongodb.user mongodb.password

推荐:

./spark-submit --master local --driver-memory 16g --executor-memory 3g --executor-cores 1 --num-executors 5 --class com.sh.wcc.Recommendation /data/spark-jars/recommendation-1.0-SNAPSHOT.jar mongodb.ip mongodb.dbName mongodb.user mongodb.password

训练模型:

./spark-submit --class com.sh.wcc.TrainModel --master local --driver-memory 8g --executor-memory 4g --executor-cores 1 --num-executors 5  /data/spark-jars/recommendation-1.0-SNAPSHOT.jar mongodb://user:pwd@ip:27017/db hdfs://hadoop01:8020/trained-models/RecommendModel


导入离线样本数据: (Example)

./spark-submit --class com.sh.wcc.ImportRawData --master local --driver-memory 8g --executor-memory 4g --executor-cores 1 --num-executors 5  /data/spark-jars/recommendation-1.0-SNAPSHOT.jar hdfs://hadoop01:8020/spark-sample-data/wcc/2017_1.csv mongodb://user:pwd@ip:27017/db

通过Kafka处理日志消息

./spark-submit --master local --driver-memory 4g --executor-memory 2g --executor-cores 1 --class com.sh.wcc.ProcessLogData /data/spark/recommendation/target/recommendation-1.0-SNAPSHOT.jar localhost:9092 ProcessLogData mongodb://user:pwd@ip:27017/db

推存使用离线推荐功能, 通过创进Crontab(计划任务)来定时更新推荐结果，这其中汲及批量更新Mongodbo数据表的问题，但已完美解决，项目有一个操作MongoDB的工具类(Scala版本)，可以直接创建和修改表、数据和索引，非常好用，具体大家可以好好研究一下，有问题，可以和我来探讨，可加我QQ:35294983。
