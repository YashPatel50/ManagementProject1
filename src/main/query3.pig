--Get the custom function in jar
REGISTER getAge.jar;

--Load in the datasets
customers = LOAD '/user/Project1/data/customers' USING PigStorage(',')
    as (id:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);

transactions = LOAD '/user/Project1/data/transactions' USING PigStorage(',')
    as (transID:int, custID:int, transTotal:float, transNumItems:int, transDesc:chararray);

--Create the age groups and get only what is needed from customers
customer_needed = foreach customers generate id, AGEGROUP(age) as age_group, gender;

--Get only what is needed from transactions
transactions_needed = foreach transactions generate custID, transTotal;

--Join the two groups
combined = join transactions_needed by custID, customer_needed by id using 'replicated';

--Group by the age group and gender
grouped = GROUP combined by (age_group, gender);

--Aggregate the fields
computed_groups = FOREACH grouped generate group, MIN(combined.transTotal) as min_transTotal,
                                                    MAX(combined.transTotal) as max_transTotal,
                                                     AVG(combined.transTotal) as avg_transTotal;

STORE computed_groups INTO 'query3' USING PigStorage(',');

