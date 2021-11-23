SELECT
ETCF.*,
NFC.total_Bookings 
FROM
(select 
searchdate,
Airline as airlineIataCode,
pageTypeCode,
siteEdition,
airModules,
Device,
url,
route,
booked_route as book_route,
avg(CASE when search_price_usd >0 then search_price_usd END) as avg_searched_fare,
count (distinct case when event_action='viewable-impression' THEN search_timestamp END) as viewable_impression,
count(DISTINCT CASE WHEN event_action = 'pageview' THEN search_timestamp END) as pageviews,
count(DISTINCT CASE WHEN event_action = 'fsi' THEN search_timestamp END) as fsi,
count(DISTINCT CASE WHEN event_action = 'open-booking-popup' THEN search_timestamp END) as open_popup,
count(DISTINCT CASE WHEN event_action <> 'fsi' and event_action <> 'pageview' and event_action <> 'viewable-impression' THEN search_timestamp END) as other_event_actions_events,
sum(bookings) as bookings,
sum(passengercount) as passengercount,
sum(revenueusd) as revenueusd
from
(select 
searchdate,
search_timestamp,
Airline,
pageTypeCode,
siteEdition,
CASE --This CASE statement replaces the airModule names for the popup fsi to the original airModule where the popup window was called. 
WHEN airmodules = 'em-booking-popup-abstract' AND event_action='fsi' AND  lag(airmodules,1) over (order by search_timestamp) <> 'em-booking-popup-abstract'
THEN lag(airmodules,1) over (order by emcid,search_timestamp) 
WHEN airmodules = 'em-booking-popup-abstract' AND event_action='fsi' AND  lag(airmodules,1) over (order by search_timestamp) = 'em-booking-popup-abstract'
THEN lag(airmodules,2) over (order by emcid,search_timestamp)
WHEN airmodules = 'em-booking-popup' AND event_action='fsi' AND  lag(airmodules,1) over (order by search_timestamp) <> 'em-booking-popup'
THEN lag(airmodules,1) over (order by emcid,search_timestamp) 
WHEN airmodules = 'em-booking-popup' AND event_action='fsi' AND  lag(airmodules,1) over (order by search_timestamp) = 'em-booking-popup'
THEN lag(airmodules,2) over (order by emcid,search_timestamp)
ELSE airModules
END AS airModules,
event_action,
Device,
emcid,
url,
route,
search_price_usd,
case when row =1 and book_route <> '' then book_route else 'n/a' end as booked_route,
sum(case when row =1 then conversions else 0 end ) as bookings,
sum(case when row =1 then passengercount else 0 end ) as passengercount,
sum(case when row =1 then totalpriceusd else 0 end ) as revenueusd
from
(select 
et.searchdate,
et.search_timestamp,
et.Airline,
et.pageTypeCode,
et.siteEdition,
et.airModules,
et.event_action,
et.Device,
et.emcid,
et.url,
et.route,
et.search_price_usd,
cf.book_route,
cf.conversions,
cf.passengercount,
cf.totalpriceusd,
ROW_NUMBER () over
(Partition by et.emcid,cf.farenetconfirmationid order by datediff(second,et."search_timestamp",cf."book_timestamp") asc ) as row
from 
(select
__createdat::DATE as searchdate,
__createdat::TIMESTAMP as search_timestamp,
upper(airline_code) as Airline,
json_extract_path_text(variables, 'ptc') as pageTypeCode,
json_extract_path_text(variables, 'se') as siteEdition,
event_category as airModules,
event_action,
upper(json_extract_path_text(variables, 'dct')) as Device,
json_extract_path_text(variables,'emcid') as emcid,
replace(replace(replace(replace(replace(replace(replace(json_extract_path_text(variables,'url'),'\?',''),'\#',''),'\'',''),'\%3A',':'),'\%2F','/'),'\%3F',''),'\%23','') as url,
--replace(replace(replace(replace(replace(replace(replace(json_extract_path_text(variables,'url'),'\'',''),'\?',''),'\#',''),'\%3A',':'),'\%2F','/'),'\%3F',''),'\%23','') as url,
CASE
WHEN event_action='fsi' then json_extract_path_text(variables,'r')
WHEN event_action='open-booking-popup' then json_extract_path_text(variables,'r')
ELSE 'n/a'
END AS route,
cast((case 
when replace(replace(json_extract_path_text(variables,'tpu'),'n/a',0),'NaN',0)='' then '0'
else replace(replace(json_extract_path_text(variables,'tpu'),'n/a',0),'NaN',0)
end) as decimal(10,2)) as search_price_usd
from public.em_cmp_lib_tracking_001
where
__createdat >= '2020-05-01'::TIMESTAMP and  __createdat < CURRENT_DATE ::TIMESTAMP --Change timestamp range for events happened.
and "searchdate" >='2020-05-01' and "searchdate" <= CURRENT_DATE ::DATE -1 --Change date range for events happened.
and upper(airline_code)='XX' --Change airlineIataCode here.
and event_action not in ('pageview','viewable-impression')
and "emcid" <> '' and "emcid" <> 'n/a'
AND json_extract_path_text(variables, 'url') !~ '\:\/\/[a-z]+-[a-z]+\.'
AND json_extract_path_text(variables, 'url') !~ '\:\/\/[a-z]+_[a-z]+\.'
) et

left join --This joins the the conversion data to the user interactions (excludes viewable-impression and pageview) data from the event tracking table.

(select
upper(airlineIatacode) as book_Airline,
__createdat as book_timestamp,
__createdat::DATE as book_date,
emcid as book_emcid,
farenetconfirmationid,
upper(departureairportiatacode)+ '>' +upper(arrivalairportiatacode) as book_route,
count(distinct farenetconfirmationid) as conversions,
passengercount,
totalpriceusd
from public.normalized_farenet_confirmation_001
where 
__createdat >= '2020-05-01'::TIMESTAMP --Change timestamp range here for bookings according to the starting date of event date range above.
and upper(airlineiatacode)='XX' --Change airlineIataCode here.
and totalpriceusd is not null
group by __createdat,upper(airlineIatacode), emcid, farenetconfirmationid, passengercount,totalpriceusd,departureairportiatacode,arrivalairportiatacode) cf

on et.Airline = cf.book_Airline
and et.emcid = cf."book_emcid"
and et.searchdate +31 >= cf.book_date
and et.searchdate <= cf.book_date
and et.search_timestamp < cf.book_timestamp
)
group by searchdate,search_timestamp,Airline,pageTypeCode,siteEdition,airModules,event_action,Device,emcid,url,route,search_price_usd,booked_route

UNION --This unions the pageview and viewable impression data with the joined data above. 

select
__createdat::DATE as searchdate,
__createdat::TIMESTAMP as search_timestamp,
upper(airline_code) as Airline,
json_extract_path_text(variables, 'ptc') as pageTypeCode,
json_extract_path_text(variables, 'se') as siteEdition,
event_category as airModules,
event_action,
upper(json_extract_path_text(variables, 'dct')) as Device,
json_extract_path_text(variables,'emcid') as emcid,
replace(replace(replace(replace(replace(replace(replace(json_extract_path_text(variables,'url'),'\?',''),'\#',''),'\'',''),'\%3A',':'),'\%2F','/'),'\%3F',''),'\%23','') as url,
--replace(replace(replace(replace(replace(replace(replace(json_extract_path_text(variables,'url'),'\'',''),'\?',''),'\#',''),'\%3A',':'),'\%2F','/'),'\%3F',''),'\%23','') as url,
'n/a' AS route,
0.00 AS search_price_usd,
'n/a' AS booked_route,
0 AS bookings,
0 AS passengercount,
0 AS revenueusd
from public.em_cmp_lib_tracking_001
where
__createdat >= '2020-05-01'::TIMESTAMP and  __createdat < CURRENT_DATE ::TIMESTAMP --Change timestamp range for events happened.
and "searchdate" >='2020-05-01' and "searchdate" <= CURRENT_DATE ::DATE -1 --Change date range for events happened.
and upper(airline_code)='XX' --Change airlineIataCode here.
and event_category not like '%booking-popup%'
and event_action in ('pageview','viewable-impression')
AND json_extract_path_text(variables, 'url') !~ '\:\/\/[a-z]+-[a-z]+\.'
AND json_extract_path_text(variables, 'url') !~ '\:\/\/[a-z]+_[a-z]+\.'
)
group by 
searchdate,Airline,pageTypeCode,siteEdition,airModules,Device,url,route,booked_route) ETCF

LEFT JOIN --This step is to verify if the FN confirmation script has ever implemented or not for this customer.

(select upper(airlineIatacode) as total_Airline,
count(distinct farenetconfirmationid) as total_Bookings
from public.normalized_farenet_confirmation_001
where 
__createdat >='2020-05-01'::TIMESTAMP --Change timestamp here to the beginning date of the current year. 
and upper(airlineiatacode)='XX'  --Change airlineIataCode here.
GROUP BY "total_Airline") NFC

on ETCF.airlineIataCode=NFC.total_Airline
