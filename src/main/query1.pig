customers = LOAD '/user/Project1/data/customers' USING PigStorage(',')
    as (id:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);

transactions = LOAD '/user/Project1/data/transactions' USING PigStorage(',')
    as (transID:int, custID:int, transTotal:float, transNumItems:int, transDesc:chararray);

combined = JOIN customers by id, transactions by custID;

grouped = GROUP combined by name;

grouped_count = FOREACH grouped Generate group as customerName, COUNT(combined) as transCount;

--Get the smallest transcount
min_count = LIMIT (ORDER grouped_count BY transCount ASC) 1;

minTransCount = FILTER grouped_count BY transCount == min_count.transCount;

STORE minTransCount INTO 'query1' USING PigStorage(',');