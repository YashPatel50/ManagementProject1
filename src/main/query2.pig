customers = LOAD '/user/Project1/data/customers' USING PigStorage(',')
    as (id:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);

country_grouped = GROUP customers by countryCode;

country_count = FOREACH country_grouped generate group as countryCode, COUNT(customers) as numCustomers;

filtered_country_count = FILTER country_count BY numCustomers > 5000 or numCustomers < 2000;

country_codes = FOREACH filtered_country_count Generate countryCode;

STORE country_codes INTO 'query2.txt' USING PigStorage(',');