# spark-java-udf-demo
This project is based on Spark 2.0.2 API which uses Dataset approach instead of Traditional RDDs.

This project describes capibilities of Spark Aggegrate User Defined Functions which can be used to extend default available aggregate features.

Input Datasource is in below format:
--------------------------
swid|device_category|time_spent|video_start

A,mob,5,1<br>
A,desk,5,2<br>
A,desk,5,3<br>
A,mob,5,2<br>
A,mob,20,16<br>
B,desk,5,2<br>
B,mob,5,2<br>
B,mob,5,2<br>
B,desk,5,2<br>
B,desk,5,2<br>
C,desk,5,2<br>
C,OTT,5,2<br>

Which is available in below project location.
/src/main/resources/user_activities.txt

and my expected output is:
--------------------------
[B,{"mob":"40.00%","desk":"60.00%"},25,10]<br>
[C,{"desk":"50.00%","OTT":"50.00%"},10,4]<br>
[A,{"mob":"75.00%","desk":"25.00%"},40,24]<br>

Which is Aggregation on user(swid) and for each user its finding total_time_spent, total_video_starts and also device usage distribution in terms of percentage. This gives us more insight to understand user's device usage. This can be used to build user usage driven recommendation system.

You need to also provide S3 location or HDFS location of the input file.

#### How to run this:
--------------------------

spark-submit --class com.parmarh.driver.UserAggDriver spark-java-udf-demo-0.0.1-SNAPSHOT.jar

