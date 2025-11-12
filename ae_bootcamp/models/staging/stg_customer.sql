{#
SELECT
id,
company,
last_name,
first_name,
email_address,
job_title,
business_phone,
home_phone,
mobile_phone,
fax_number,
address,
city,
state_province,
zip_postal_code,
country_region,
web_page,
notes,
attachments,
insertion_timestamp
FROM `eighth-keyword-476220-f6.dl_northwind.customer`
#}


with source as (

    select * from {{ source('northwind', 'customer') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source
