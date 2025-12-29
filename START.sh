#!/bin/bash
v_JobName="STREAMING_SRCKAFKA_TO_INTERMEDIATEKAFKA"
v_QueueName="root.team_queue_name"
v_app_id=$(yarn application --list | grep -w "${v_JobName}.*${v_QueueName}"|awk '{print $1}')
if [ ! -z  "$v_app_id" ]; then
echo "WARNING:The ${v_JobName} ( ${v_app_id} ) is already running  state, so please stop it before restart."
exit;
else
sleep 4
v_app_id=$(yarn application --list | grep -w "${v_JobName}.*${v_QueueName}"|awk '{print $1}')
if [ ! -z  "$v_app_id" ]; then
echo "WARNING:The ${v_JobName} ( ${v_app_id} ) is already running  state, so please stop it before restart."
exit;
fi
fi
basePath=/edgenodepath/dataProctName
streamingLog=${basePath}/projectName/scripts/streaming_prjName/srcToSinikfk.log
jar_name=xxx-src-to-sink-1.0.0-shaded.jar
echo jar ${jar_name}
echo ---------- starting -------------------
echo
keytab=${basePath}/projectName/scripts/streaming_prjName/dev-env-genericid.keytab
keytab1=${basePath}/projectName/scripts/streaming_prjName/dev-env-genericid.keytab
[ -e ${keytab} ] && rm ${keytab} && rm ${keytab1}
hdfs dfs -copyToLocal hdfs:///user/genericid/projectName/dev-env-genericid.keytab ${basePath}/projectName/conf/.
mv ${basePath}/projectName/conf/dev-env-genericid.keytab ${basePath}/threatmatrix/r2b/conf/genericid.keytab
hdfs dfs -copyToLocal hdfs:///user/genericid/projectName/dev-env-genericid.keytab ${basePath}/projectName/conf/.

echo ----------- kerberos initialized ------------------
echo ----------- Deleting Streaming Log File -----------
echo
[ -e ${streamingLog} ] && rm ${streamingLog}
echo
echo
jarname=${basePath}/projectName/jars/${jar_name}
[ -e ${jarname} ] && rm ${jarname}
echo ----------- deleted jar file in local location ------------------
echo
echo started copying latest jar from the hdfs location
hdfs dfs -copyToLocal hdfs:////user/genericid/projectName/${jar_name} ${basePath}/projectName/jars/.
echo ---------- copied jar to local -------------------
echo
echo executing spark submit command
export HADOOP_CONF_DIR=/etc/hadoop/conf:/etc/hive/conf:/opt/cod--1f1862gu2fmxx/hbase-conf
export SPARK_CLASSPATH=/etc/hadoop/conf:/etc/hive/conf:/opt/cod--1f1862gu2fmxx/hbase-conf

nohup /opt/cloudera/parcels/CDH/bin/spark3-submit --master yarn --deploy-mode cluster --jars /projectName/jars/delta-core_2.12-2.0.2.jar,/projectName/jars/delta-storage-2.0.2.jar,/opt/cloudera/parcels/CDH/jars/kafka_2.12-2.8.1.7.2.15.100-9.jar --files /projectName/conf/jaas.conf,/projectName/conf/application.conf,/projectName/conf/log4j.properties,/projectName/conf/dev-env-genericid.keytab --conf 'spark.executor.memory=1G' --conf 'spark.driver.memory=1G' --conf 'spark.executor.cores=1' --conf 'spark.driver.cores=1' --conf 'spark.executor.instances=1' --conf spark.yarn.keytab=/genericid/conf/genericid.keytab --conf spark.yarn.principal=genericid@PRD/DEV/SIT-ENV.xxx-xxx.CLOUDERA.SITE --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Dlog4j.configuration=log4j.properties" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Dlog4j.configuration=log4j.properties" --conf "spark.eventLog.enabled=true" --conf "spark.speculation=true" --conf "spark.broadcast.compress=true" --conf "spark.checkpoint.compress=true" --conf "spark.io.compression.codec=snappy" --conf 'spark.executor.memoryOverhead=1G' --conf 'spark.shuffle.service.enabled=false' --conf "spark.app.name=STREAMING_SRCKAFKA_TO_INTERMEDIATEKAFKA" --conf 'spark.dynamicAllocation.enabled=false'  --conf "spark.checkpoint.compress=true" --conf 'spark.yarn.maxAppAttempts=5' --conf "spark.hadoop.fs.hdfs.impl.disable.cache=true" --conf "spark.io.compression.codec=snappy" --conf spark.kerberos.access.hadoopFileSystems=abfss://worksps@data001.dfs.core.windows.net --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" --class com.XXX.XXX.XXX.driver.PayloadDriver ${jarname} ${jarname} >> ${streamingLog} & 


echo ---------- spark job started in the background -------------------
echo
echo ---------- ending -------------------
