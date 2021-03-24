
--************************ODT cube construction from Twitter data************************--

--Fields (or columns) of the geotagged tweets table: tweetid, userid, postdate,latitude,longitude,geo,placetype,source

--List of human_sources:
('Echofon','Endomondo','Fenix for Android','Flamingo for Android','Foursquare','Gay Los Angeles','Gay Santa Monica','Gay West Hollywood',
'Hootsuite','Instagram','iOS','OS X','PlumeforAndroid','SoundHound','Squarespace','Talon (Plus)','Talon Android','Talon Plus','Tweet It! for Windows',
'Tweetbot for iS','Tweetbot for Mac','TweetCaster for Android','TweetCaster for iOS','Tweetings  for iPad','Tweetings for Android','Tweetings for Android Holo',
'Tweetings for Android Tablets','Tweetings for iPhone','Tweetings  for iPhone','Tweetlogix','twicca','Twidere for Android #4','Twidere for Android #5','Twidere for Android #7',
'Twishort Client','Twittelator','Twitter Dashboard for iPhone','Twitter Engage for iPhone','Twitter for  Android','Twitter for  iPhone','Twitter for Android',
'Twitter for Android Tablets','Twitter for Apple Watch','Twitter for BlackBerry','Twitter for Calendar','Twitter for iPad','Twitter for iPhone','Twitter for Mac',
'Twitter for Windows','Twitter for Windows Phone','Untappd','Tweetbot for iOS','Foursquare Swarm', 'UberSocial for Android','Twitter Web Client')

----------------------Compute the entity-level cross-day movement flows --> 4D cube (user, date, origin, destination)-------------

--The query below is for one cross-day flows at the entity level, need to repeat this for each cross-day to derive all days for the selected time period. This can be implemented in python or Java etc. After computing all the cross-day flows, a table named world_user_daily_movement_cd_2020 is generated (using 2020 data as an example). 

select a.userid, 2020,11,1,
round(cast(a.lat as decimal(20, 15)), 8) as o_lat,
round(cast(a.lng as decimal(20, 15)), 8) as o_lng, 
round(cast(b.lat as decimal(20, 15)), 8) as d_lat, 
round(cast(b.lng as decimal(20, 15)), 8) as d_lng, 
round(cast(2 * 3961 * asin(sqrt(power((sin(radians((b.lat - a.lat) / 2))),2) + cos(radians(a.lat)) * cos(radians(b.lat)) * power((sin(radians((b.lng - a.lng) / 2))),8))) as decimal(20, 15)),2) as distance 
from (select userid, avg(latitude) as lat, avg(longitude) as lng, count(*) as tweetcnt  from world_geo_tweets where (geo =  'latlon' or geo = 'place' and (placetype = 'city' or placetype = 'neighborhood' or placetype = 'poi'))
and (source in (select source from human_sources)) 
and (year = 2020 and month = 11 and day(postdate)= 1) group by userid) a  
join (select userid, avg(latitude) as lat, avg(longitude) as lng, count(*) as tweetcnt  from world_geo_tweets  where (geo =  'latlon' or geo = 'place' and (placetype = 'city' or placetype = 'neighborhood' or placetype = 'poi'))  and (year = 2020 and month = 11 and day(postdate)=2)) group by userid) b  
on a.userid = b.userid; 


----------------------Compute the entity-level single-day movement flows --> 4D cube (user, date, origin, destination)-------------

drop view if exists user_daily_firsttweettime;
create view user_daily_firsttweettime as
select userid, year, month, day(postdate) as day, min(postdate) as firsttweettime from world_geo_tweets where year = 2020
and (geo =  'latlon' or geo = 'place' and (placetype = 'city' or placetype = 'neighborhood' or placetype = 'poi'))  
and (source in (select source from human_sources)) 
group by userid, year, month, day(postdate);

drop view if exists user_daily_firsttweettime_latlng;
create view user_daily_firsttweettime_latlng as 
select a.userid, a.year,a.month,day(a.postdate) as day, b.firsttweettime,a.latitude,a.longitude from world_geo_tweets a
join user_daily_firsttweettime b 
on a.userid = b.userid and a.postdate = b.firsttweettime
where a.year = 2020;

drop table if exists user_tweet_distance;
create table user_tweet_distance as 
select distinct (a.tweetid), a.userid, a.year, a.month, day(a.postdate) as day,b.firsttweettime, a.postdate, 
round(cast(b.latitude  as decimal(25, 15)),3) as o_lat, 
round(cast(b.longitude  as decimal(25, 15)),3) as o_lng,
round(cast(a.latitude  as decimal(25, 15)),3) as d_lat,
round(cast(a.longitude  as decimal(25, 15)),3) as d_lng,
round(cast(2 * 3961 * asin(sqrt(power((sin(radians((b.latitude - a.latitude) / 2))),2) + cos(radians(a.latitude)) * cos(radians(b.latitude)) * power((sin(radians((b.longitude - a.longitude) / 2))),2))) as decimal(25, 15)),3) as distance
from world_geo_tweets a 
join user_daily_firsttweettime_latlng b 
on a.userid = b.userid and day(a.postdate) = b.day and a.month = b.month and a.year = b.year
where a.year = 2020;
from world_geo_tweets a 
join user_daily_firsttweettime_latlng b 
on a.userid = b.userid and day(a.postdate) = b.day and a.month = b.month and a.year = b.year
where a.year = 2020;

drop view if exists user_daily_distance;
create view user_daily_distance as 
select userid,year,month,day,max(distance) as distance, count(*) as tweetcnt from user_tweet_distance group by userid,year,month,day;

drop table if exists world_user_daily_movement_sd_2020;
create table world_user_daily_movement_sd_2020 as 
select distinct a.*, b.o_lat, b.o_lng, b.d_lat, b.d_lng  from user_daily_distance a
join user_tweet_distance b
on a.userid = b.userid and a.day= b.day and a.month = b.month and a.year = b.year and a.distance = b.distance
where a.tweetcnt > 1;

----------------------Compute the ODT cube(origin, destination, date) based on the 4D cubes. Using country-level as an example-------------
--Based on the 4D cube, ODT cubes are derived by aggregating the individual flows at specific geographic levels (e.g., county, state, world first-level subdivision, or country)
--First, combine the entity-level flows world_user_daily_movement_cd_2020 and world_user_daily_movement_sd_2020 into one table: 
--Next, the query below to assign the place id to the origin and destination of each individual flows for further aggregation. (run in Hive)
drop view if exists temp;
create view temp as 
select a.*,b.gmi_cntry as o_place from world_user_daily_movement_cd_sd_2020 a
join countries b
where ST_Contains(b.boundaryshape, ST_Point(cast(a.o_lng as double), cast(a.o_lat as double)));

drop table if exists world_user_daily_movement_cd_sd_country_2020;
create table world_user_daily_movement_cd_sd_country_2020 as 
select a.*,b.gmi_cntry as d_place from temp a
join countries b
where ST_Contains(b.boundaryshape, ST_Point(cast(a.d_lng as double), cast(a.d_lat as double)));



--************************ODT cube construction from SafeGraph data************************--

--HiveQL was used for extracting the census block group level OD flows (table: sg_od) from SafeGraph Social Distancing Metrics dataset (table: sg_social_distancing). The queries need to run in Hive on a Hadoop environment (https://hive.apache.org):

drop table if exists sg_social_distancing;
create external table sg_social_distancing(origin_census_block_group string,
                                           date_range_start string,
                                           date_range_end string,
                                           device_count int,
                                           distance_traveled_from_home int,
                                           bucketed_distance_traveled string,
                                           median_dwell_at_bucketed_distance_traveled string,
                                           completely_home_device_count smallint,
                                           median_home_dwell_time smallint,
                                           bucketed_home_dwell_time string,
                                           at_home_by_each_hour string,
                                           part_time_work_behavior_devices smallint,
                                           full_time_work_behavior_devices smallint,
                                           destination_cbgs string,
                                           delivery_behavior_devices smallint,
                                           median_non_home_dwell_time smallint,
                                           candidate_device_count int,
                                           bucketed_away_from_home_time string,
                                           median_percentage_time_home smallint,
                                           bucketed_percentage_time_home string,
                                           mean_home_dwell_time smallint,
                                           mean_non_home_dwell_time smallint,
                                           mean_distance_traveled_from_home int)
partitioned by (year smallint,month smallint,day smallint) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar'     = '"',
   'escapeChar'    = '\\'
)
location '/safegraph_social_distancing/'
tblproperties ("skip.header.line.count"="1");


drop view if exists destination_list;
create view destination_list as
select origin_census_block_group, year, month, day, 
split(translate(substr(destination_cbgs, 2, length(destination_cbgs) - 2),"\"",""), ",") as destinations 
from sg_social_distancing where year = 2020;

drop view if exists sg_od_view;
create view sg_od_view as
select origin_census_block_group as origin_bg, split(blck,":")[0] as destination_bg, cast(split(blck,":")[1] as int) as device_count, year, month, day
from destination_list 
LATERAL VIEW explode(destinations) dest_table as blck;

--Create entity-level 4D cube (blk,o,d,date)
drop table if exists sg_od_2020;
create table sg_od_2020 as
select c.origin_bg, 
c.lat as o_lat,
c.lon as o_lon, 
c.state_fips as o_state,
c.stcofips as o_county,
c.tract as o_tract,
c.destination_bg, 
d.lat as d_lat,
d.lon as d_lon,
d.state_fips as d_state,
d.stcofips as d_county,
d.tract as d_tract, 
c.device_count, c.year,c.month,c.day from
(select a.origin_bg, b.lat,b.lon, b.state_fips,b.stcofips,b.tract, a.destination_bg, a.device_count, a.year,a.month,a.day from sg_od_view a 
join block_group_centroid b on a.origin_bg = b.fips) c
join block_group_centroid d on c.destination_bg = d.fips;

--Create ODT cube (o,d,date), using tract-level as an example
drop table if exists sg_od_2020_tract;
create table sg_od_2020_tract as
select o_tract as o_fips, d_tract as d_fips, year, month, day, sum(device_count) as cnt, avg(o_lat) as o_lat,avg(o_lon) as o_lon,avg(d_lat) as d_lat,avg(d_lon) as d_lon 
from sg_od_2020 group by o_tract, d_tract, year, month, day
