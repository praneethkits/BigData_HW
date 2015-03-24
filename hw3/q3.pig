ratings = LOAD '/big_data/dataset/ratings.dat' using PigStorage(':') as (userid:int, movieid:int, rating:int, timestamp:long);
movies = LOAD '/big_data/dataset/movies.dat' using PigStorage(':') as (movieid:int, title:chararray, genre:chararray);
cogrouped = COGROUP ratings by movieid, movies by movieid;
join_data = foreach cogrouped {
    generate FLATTEN(movies), FLATTEN(ratings);
}
out_join_data = limit join_data 6;
store out_join_data into '/q3';
/*topResults = FOREACH cogrouped {
    result = TOP(10, 3, ratings); 
    GENERATE FLATTEN(result);
}*/
