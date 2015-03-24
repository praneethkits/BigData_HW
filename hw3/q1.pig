ratings = LOAD '/big_data/dataset/ratings.dat' using PigStorage(':') as (userid:int, movieid:int, rating:int, timestamp:long);
movies = LOAD '/big_data/dataset/movies.dat' using PigStorage(':') as (movieid:int, title:chararray, genre:chararray);
users = LOAD '/big_data/dataset/users.dat' using PigStorage(':') as (userid:int, gender:chararray, age:int, occupation:int, zipcode:int);
req_users1 = FILTER users by age>=20 and age <=40 and gender == 'M' and zipcode/10000 == 1;
req_users = FOREACH req_users1 GENERATE userid;
req_movies1 = FILTER movies by (LOWER(genre) matches '.*comedy.*') and (LOWER(genre) matches '.*drama.*');
req_movies = FOREACH req_movies1 GENERATE movieid;
req_ratings = FOREACH ratings GENERATE userid,  movieid, rating;
avg_req_ratings = JOIN req_movies by movieid, req_ratings by movieid;
avg_ratings_group = GROUP avg_req_ratings by req_ratings::movieid;
avg_ratings = FOREACH avg_ratings_group GENERATE group, AVG(avg_req_ratings.req_ratings::rating);
low_ratings_group = GROUP avg_ratings ALL;
low_rating = FOREACH low_ratings_group GENERATE  MIN(avg_ratings.$1);
/* store low_rating into '/q1pig_low_rating';*/
low_rated_movies = JOIN avg_ratings by $1, low_rating by $0;
/* dump low_rated_movies; */
low_movies = FOREACH low_rated_movies GENERATE $0;
rated_users1 = JOIN low_movies by $0, req_ratings by movieid;
rated_users = FOREACH rated_users1 GENERATE req_ratings::userid;
out_users1 = JOIN rated_users by $0, req_users by userid;
out_users = FOREACH out_users1 GENERATE $0;
STORE out_users INTO '/q1';
