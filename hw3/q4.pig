register /home/hduser/BigData_HW/hw3/FORMAT_GENRE/FORMAT_GENRE.jar
movies = LOAD '/big_data/dataset/movies.dat' using PigStorage(':') as (movieid:int, title:chararray, genre:chararray);
formatted_genre = foreach movies GENERATE FORMAT_GENRE(genre);
STORE formatted_genre into '/q4';
