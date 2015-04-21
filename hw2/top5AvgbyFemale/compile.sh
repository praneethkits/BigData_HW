hadoop fs -rm /tmp/userRatingsReduce_out1/*
hadoop fs -rmdir /tmp/userRatingsReduce_out1
hadoop fs -rm /tmp/userRatingsReduce_out/*
hadoop fs -rmdir /tmp/userRatingsReduce_out
hadoop fs -rm "/dataset/user_ratings_out/*"
hadoop fs -rmdir /dataset/user_ratings_out
rm -rf *.class
rm top5AvgbyFemale.jar
javac top5AvgbyFemale.java
jar -cvf top5AvgbyFemale.jar -C . ./
hadoop jar top5AvgbyFemale.jar top5AvgbyFemale /dataset/users.dat /dataset/ratings.dat /dataset/movies.dat /dataset/user_ratings_out
