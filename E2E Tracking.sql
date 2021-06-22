SELECT ETCF.*,NFC.total_Bookings 
FROM
(select distinct 
searchdate,
airlineIataCode,
airModules,
Device,
pageTypeCode,
siteEdition,
url,
case when sum(fsi) >= 1 or sum(bookings)>=1 then route
else 'n/a'
end as  route, 
book_route,
avg(avg_searched_fare) as avg_searched_fare,
sum(viewableImpression) AS viewableImpression,
sum(pageviews) AS pageviews,
sum(openPopup) AS openPopup,
sum(fsi) AS fsi,
sum(other_event_actions_events) AS other_event_actions_events,
sum(revenueusd) as revenueusd,
sum(passengercount) as passengercount,
sum(bookings) as bookings
from(
select distinct 
searchdate,
airlineIataCode,
airModules,
Device,
pageTypeCode,
siteEdition,
url, 
route,
book_route,
avg(totalpriceusd) as avg_searched_fare,
sum(viewableImpression) AS viewableImpression,
sum(pageviews) AS pageviews,
sum(openPopup) AS openPopup,
sum(fsi) AS fsi,
sum(other_event_actions_events) AS other_event_actions_events,
sum(case 
when event_action_group=1 and row =1 then revenueusd else 0 END) AS revenueusd,
sum(case 
when event_action_group=1 and row =1 then passengercount else 0 END) AS passengercount,
sum(case 
when event_action_group=1 and row =1 then bookings else 0 End) AS bookings
from 
(select  
et.searchdate,
et.search_timestamp,
et.Airline as airlineIataCode,
et.airModules,
et.event_action_group,
et.Device,
et.pageTypeCode, 
et.siteEdition,
et.emcid,
cast(et.totalpriceusd as decimal(10,2)) as totalpriceusd,
et.url,
et.route,
cf.book_route,
sum(et.viewable_impression) as viewableImpression,
sum(et.pageviews) as pageviews,
sum(et.open_popup) as openPopup,
sum(et.fsi) as fsi,
sum(et.other_event_actions_events) as other_event_actions_events,
sum(cf.conversions) as Bookings,
sum(cf.passengercount) as passengercount,
sum(cf.totalpriceusd ) as RevenueUSD,
cf.farenetconfirmationid,
ROW_NUMBER () over
(Partition by et.emcid,et.searchdate,et.Airline, et.Device, et.pageTypeCode, et.siteEdition,et.event_action_group,cf.book_route,cf.farenetconfirmationid
 order by datediff(second,et."search_timestamp",cf."book_timestamp") asc ) as row
from 
(select  
ga.searchdate,
ga.search_timestamp,
ga.Airline,
ga.airModules,
ga.event_action,
case
when ga.event_action IN ('pageview','viewable-impression') then 0
else 1
end as event_action_group,
ga.Device,
ga.pageTypeCode,
ga.siteEdition,
ga.emcid,
ga.url,
cast(ga.totalpriceusd as decimal(10,2)) as totalpriceusd,
case
when ga.open_popup= 1 and bp.popup_route <> '' then bp.popup_route
else ga.route
end as route,
sum(ga.viewable_impression) as viewable_impression,
sum(ga.pageviews) as pageviews,
sum(ga.open_popup) as open_popup,
sum(ga.other_event_actions_events) as other_event_actions_events,
sum(case
when ga.open_popup= 1 and bp.popup_fsi is null then 0
when ga.open_popup= 1 and bp.popup_fsi is not null then bp.popup_fsi
else ga.fsi 
end) as fsi
from 
(select
distinct
__createdat::DATE as searchdate,
__createdat::TIMESTAMP as search_timestamp,
upper(airline_code) as Airline,
event_action,
upper(json_extract_path_text(variables, 'dct')) as Device,
json_extract_path_text(variables, 'ptc') as pageTypeCode,
event_category as airModules,
json_extract_path_text(variables, 'se') as siteEdition,
json_extract_path_text(variables,'emcid') as emcid,
replace(replace(replace(replace(replace(replace(replace(json_extract_path_text(variables,'url'),'\?',''),'\#',''),'\'',''),'\%3A',':'),'\%2F','/'),'\%3F',''),'\%23','') as url,
--replace(replace(replace(replace(replace(replace(replace(json_extract_path_text(variables,'url'),'\'',''),'\?',''),'\#',''),'\%3A',':'),'\%2F','/'),'\%3F',''),'\%23','') as url,
case
when json_extract_path_text(variables,'r')='' then 'n/a'
ELSE json_extract_path_text(variables,'r')
end as route,
case 
when replace(replace(json_extract_path_text(variables,'tpu'),'n/a',0),'NaN',0)='' then '0'
else replace(replace(json_extract_path_text(variables,'tpu'),'n/a',0),'NaN',0)
end as totalpriceusd,
count(DISTINCT CASE WHEN event_action = 'viewable-impression' THEN __createdat::TIMESTAMP END) as viewable_impression,
count(DISTINCT CASE WHEN event_action = 'pageview' THEN __createdat::TIMESTAMP END) as pageviews,
count(DISTINCT CASE WHEN event_action = 'fsi' THEN __createdat::TIMESTAMP END) as fsi,
count(DISTINCT CASE WHEN event_action = 'open-booking-popup' THEN __createdat::TIMESTAMP END) as open_popup,
count(DISTINCT CASE WHEN event_action <> 'fsi' and event_action <> 'pageview' and event_action <> 'viewable-impression' THEN __createdat::TIMESTAMP END) as other_event_actions_events
from public.em_cmp_lib_tracking_001
where
upper(airline_code)='XX'  --change the airlineIataCode here.
and "searchdate" >='2020-05-01' and "searchdate" <= CURRENT_DATE -1 
and event_category not like '%booking-popup%' 
AND json_extract_path_text(variables, 'url') !~ '\:\/\/[a-z]+-[a-z]+\.'
AND json_extract_path_text(variables, 'url') !~ '\:\/\/[a-z]+_[a-z]+\.'
group by "searchdate","search_timestamp","Airline","event_action","Device","pageTypeCode","airModules","siteEdition","emcid","url","route","totalpriceusd"
) ga

left join 

(select
distinct
__createdat::DATE as popup_date,
__createdat::TIMESTAMP as popup_timestamp,
upper(airline_code) as popup_Airline,
json_extract_path_text(variables,'emcid') as popup_emcid,
case event_category 
     when 'em-booking-popup' then 'open-booking-popup'
     when 'em-booking-popup-abstract' then 'open-booking-popup'
     else event_category
END as event_category2,
replace(replace(replace(replace(replace(replace(replace(json_extract_path_text(variables,'url'),'\?',''),'\#',''),'\'',''),'\%3A',':'),'\%2F','/'),'\%3F',''),'\%23','') as popup_url,
json_extract_path_text(variables,'r') as popup_route,
count(distinct __createdat::TIMESTAMP) as popup_fsi
from public.em_cmp_lib_tracking_001
where 
event_category like '%booking-popup%' and event_action='fsi' 
and upper(airline_code)='XX'  --change the airlineIataCode here. 
 and "popup_date" >='2020-05-01' and "popup_date" <= CURRENT_DATE -1
group by "popup_date","popup_timestamp","popup_Airline","popup_emcid","event_category2","popup_url","popup_route") bp

on ga.Airline = bp.popup_Airline
and ga.emcid = bp.popup_emcid 
and ga.event_action = bp.event_category2
and ga.searchdate = bp.popup_date
and ga.search_timestamp < bp.popup_timestamp
and ga.url = bp.popup_url
and ga.route = bp.popup_route

GROUP BY ga.searchdate,ga.search_timestamp,ga.Airline,
ga.airModules,ga.event_action,ga.Device,ga.pageTypeCode,ga.siteEdition,ga.emcid,ga.url,ga.totalpriceusd,ga.route,ga.open_popup,bp.popup_route
) et

LEFT JOIN

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
upper(airlineiatacode)='XX'  --change the airlineIataCode here.
and __createdat::DATE >='2020-05-01'
and "book_emcid" <>  '' and "book_emcid" <>  'n/a'
and totalpriceusd is not null
group by __createdat,upper(airlineIatacode), emcid, farenetconfirmationid, passengercount,totalpriceusd,departureairportiatacode,arrivalairportiatacode) cf

on et.Airline = cf.book_Airline
and et.emcid = cf."book_emcid"
and et.searchdate +31 >= cf.book_date
and et.searchdate <= cf.book_date
and et.search_timestamp < cf.book_timestamp

group by et.searchdate, et.search_timestamp,et.Airline,et.airModules,et.event_action_group,et.emcid,et.totalpriceusd,et.url,et.route,et.Device, et.pageTypeCode, et.siteEdition,cf.book_route,cf.book_timestamp,cf.farenetconfirmationid
)
GROUP BY searchdate,airlineIataCode,airModules,Device,pageTypeCode,siteEdition,url,route,book_route)
GROUP BY searchdate,airlineIataCode,airModules,Device,pageTypeCode,siteEdition,url, route, book_route) ETCF

LEFT JOIN

(select upper(airlineIatacode) as total_Airline,
  count(distinct farenetconfirmationid) as total_Bookings
from public.normalized_farenet_confirmation_001
where upper(airlineiatacode)='XX'  --change the airlineIataCode here.
 and __createdat::DATE >='2020-05-01'
GROUP BY "total_Airline") NFC

on ETCF.airlineIataCode=NFC.total_Airline
