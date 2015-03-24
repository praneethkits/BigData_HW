ratings = LOAD '/big_data/dataset/ratings.dat' using PigStorage(':') as (userid:int, movieid:int, rating:int, timestamp:long);
movies = LOAD '/big_data/dataset/movies.dat' using PigStorage(':') as (movieid:int, title:chararray, genre:chararray);
cogrouped = COGROUP ratings by movieid, movies by movieid;
topResults = limit cogrouped 6;
store topResults into '/q2/topResults';
/*topResults = FOREACH cogrouped {
    result = TOP(10, 3, ratings); 
    GENERATE FLATTEN(result);
}*/
