# displays all values of enum popularity scale
"SELECT enum_range(NULL::popularityscale);"

# for psql stackexchange
"""
create table if not exists PostsCopy as (select * from Posts);
CREATE TYPE if not exists PopularityScale AS ENUM ('High', 'Medium', 'Low');
"""

# q1
"""
drop table if exists postscopy;
drop type if exists popularityscale;
create table PostsCopy as (select * from Posts);
create type popularityscale as enum('High', 'Medium','Low');
"""

# for usersummary q6
"""
drop view if exists userssummary;
drop view if exists userssummary;
"""