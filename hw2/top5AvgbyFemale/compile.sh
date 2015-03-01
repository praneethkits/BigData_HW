hadoop fs -rm /tmp/userRatingsReduce_out1/*
hadoop fs -rmdir /tmp/userRatingsReduce_out1
hadoop fs -rm /tmp/userRatingsReduce_out/*
hadoop fs -rmdir /tmp/userRatingsReduce_out
hadoop fs -rm "/big_data/hw1/dataset/user_ratings_out/*"
hadoop fs -rmdir /big_data/hw1/dataset/user_ratings_out
rm -rf *.class
rm top5AvgbyFemale.jar
javac top5AvgbyFemale.java
jar -cvf top5AvgbyFemale.jar -C . ./
hadoop jar top5AvgbyFemale.jar top5AvgbyFemale  /big_data/hw1/dataset/users.dat /big_data/hw1/dataset/ratings.dat /big_data/hw1/dataset/movies.dat /big_data/hw1/dataset/user_ratings_out
