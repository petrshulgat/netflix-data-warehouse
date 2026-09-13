create table netflix_data_raw(
show_id text primary key, 
type text, 
title text, 
director text, 
"cast" text, 
country text, 
date_added text, 
release_year text, 
rating text, 
duration text, 
listed_in text, 
description text);


create table netflix_data_clean(
show_id integer primary key, 
type varchar(10), 
title text, 
director varchar(50), 
"cast" text, 
country varchar(50), 
date_added date, 
release_year integer, 
rating text, 
duration text, 
listed_in text, 
description text);