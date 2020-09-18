select distinct 
search_timestamp,
airlineIataCode,
eventCategory,
Device,
pageTypeCode,
siteEdition,
url,
route,
book_route,
viewableImpression,
pageviews,
openPopup,
fsi,
other_event_actions_events,
case 
when date_diff <=30 and row =1 and eventAction not in ('pageview','viewable-impression') then revenueusd else 0 END as revenueusd,
case 
when date_diff <=30 and row =1 and eventAction not in ('pageview','viewable-impression') then bookings else 0 End as bookings
from
(
select  
et.searchdate,
et.search_timestamp,
et.Airline as airlineIataCode,
et.event_category as eventCategory,
et.event_action as eventAction,
et.Device, 
et.pageTypeCode, 
et.siteEdition,
et.emcid,
et.url,
et.route,
cf.book_timestamp,
cf.book_emcid,
cf.book_route,
sum(et.viewable_impression) as viewableImpression,
sum(et.pageviews) as pageviews,
sum(et.open_popup) as openPopup,
sum(et.fsi) as fsi,
sum(et.other_event_actions_events) as other_event_actions_events,
sum(cf.conversions) as Bookings,
sum(cf.totalpriceusd) as RevenueUSD,
datediff(day,et."search_timestamp",cf."book_timestamp") as date_diff,
datediff(second,et."search_timestamp",cf."book_timestamp") as time_diff,
ROW_NUMBER () over
(Partition by et.emcid,et.searchdate,et.Airline, et.Device, et.pageTypeCode, et.siteEdition,cf.book_route
 order by datediff(second,et."search_timestamp",cf."book_timestamp") asc ) as row
from 
(select  
ga.searchdate,
ga.search_timestamp,
ga.Airline,
ga.event_category,
ga.event_action,
ga.Device,
ga.pageTypeCode,
ga.siteEdition,
ga.emcid,
ga.url,
ga.route,
ga.viewable_impression,
ga.pageviews,
ga.open_popup,
ga.other_event_actions_events,
case
when ga.open_popup= 1 and bp.popup_fsi is null then 0
when ga.open_popup= 1 and bp.popup_fsi is not null then bp.popup_fsi
else ga.fsi 
end as fsi
from 
(select
distinct
__createdat::DATE as searchdate,
__createdat::TIMESTAMP as search_timestamp,
upper(json_extract_path_text(variables, 'aic')) as Airline,
event_category,
event_action,
upper(json_extract_path_text(variables, 'dct')) as Device,
json_extract_path_text(variables, 'ptc') as pageTypeCode,
json_extract_path_text(variables, 'se') as siteEdition,
json_extract_path_text(variables,'emcid') as emcid,
json_extract_path_text(variables,'url') as url,
json_extract_path_text(variables,'r') as route,
count(DISTINCT CASE WHEN event_action = 'viewable-impression' THEN __createdat::TIMESTAMP END) as viewable_impression,
count(DISTINCT CASE WHEN event_action = 'pageview' THEN __createdat::TIMESTAMP END) as pageviews,
count(DISTINCT CASE WHEN event_action = 'fsi' THEN __createdat::TIMESTAMP END) as fsi,
count(DISTINCT CASE WHEN event_action = 'open-booking-popup' THEN __createdat::TIMESTAMP END) as open_popup,
count(DISTINCT CASE WHEN event_action <> 'fsi' and event_action <> 'pageview' and event_action <> 'viewable-impression' THEN __createdat::TIMESTAMP END) as other_event_actions_events
from public.em_cmp_lib_tracking_001
where
upper(airline_code)='Y4' 
and __createdat::DATE >='2020-05-01' and __createdat::DATE <= current_date::DATE-1
and event_category not like '%booking-popup%' 
and json_extract_path_text(variables,'url') not like '%-dev.%'
and json_extract_path_text(variables,'url') not like '%_dev.%'
and json_extract_path_text(variables,'url') not like '%-prepro.%'
and json_extract_path_text(variables,'url') not like '%_prepro.%'
and json_extract_path_text(variables,'url') not like '%-dev-latest.%'
and json_extract_path_text(variables,'url') not like '%.airtrfx.%'
group by event_category, event_action,__createdat,upper(json_extract_path_text(variables, 'aic')),
upper(json_extract_path_text(variables, 'dct')),json_extract_path_text(variables, 'ptc'),
json_extract_path_text(variables, 'se'),json_extract_path_text(variables,'emcid'),
json_extract_path_text(variables,'url'), json_extract_path_text(variables,'r')
) ga

left join 

(select
distinct
__createdat::DATE as popup_date,
__createdat::TIMESTAMP as popup_timestamp,
json_extract_path_text(variables,'emcid') as popup_emcid,
case event_category 
     when 'em-booking-popup' then 'open-booking-popup'
     when 'em-booking-popup-abstract' then 'open-booking-popup'
     else event_category
END as event_category2,
event_action as event_action2,
json_extract_path_text(variables,'url') as popup_url,
json_extract_path_text(variables,'r') as popup_route,
count(DISTINCT CASE WHEN event_action = 'fsi' THEN __createdat::TIMESTAMP END) as popup_fsi
from public.em_cmp_lib_tracking_001
where 
upper(airline_code)='Y4'
and event_category like '%booking-popup%' and event_action='fsi' 
and __createdat::DATE >='2020-05-01' and __createdat::DATE <= current_date::DATE-1 
and json_extract_path_text(variables,'url') not like '%-dev.%'
and json_extract_path_text(variables,'url') not like '%_dev.%'
and json_extract_path_text(variables,'url') not like '%-prepro.%'
and json_extract_path_text(variables,'url') not like '%_prepro.%'
and json_extract_path_text(variables,'url') not like '%-dev-latest.%'
and json_extract_path_text(variables,'url') not like '%.airtrfx.%'
group by __createdat,json_extract_path_text(variables,'emcid'),event_category, event_action,json_extract_path_text(variables,'url'),json_extract_path_text(variables,'r')) bp

on ga.emcid = bp.popup_emcid 
and ga.event_action = bp.event_category2
and ga.searchdate = bp.popup_date
and ga.search_timestamp < bp.popup_timestamp
and ga.url = bp.popup_url
and ga.route = bp.popup_route
) et

LEFT JOIN

(select
__createdat as book_timestamp,
emcid as book_emcid,
upper(departureairportiatacode)+ '-' +upper(arrivalairportiatacode) as book_route,
count(distinct farenetconfirmationid) as conversions,
totalpriceusd
from public.normalized_farenet_confirmation_001
where 
upper(airlineiatacode)='Y4' 
and __createdat::DATE >='2020-05-01'
and "book_emcid" <>  '' and "book_emcid" <>  'n/a'
and totalpriceusd is not null
group by __createdat,emcid, totalpriceusd,departureairportiatacode,arrivalairportiatacode) cf

on et.emcid = cf."book_emcid"
and et.search_timestamp < cf.book_timestamp

 
group by et.searchdate, et.search_timestamp, 
et.Airline,et.event_category,et.event_action, 
et.emcid,et.Device, et.pageTypeCode, et.siteEdition,et.url,
et.route,cf.book_timestamp,cf.book_emcid,cf.book_route
)
