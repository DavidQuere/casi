-- ADD JAR json-serde-1.1.6-SNAPSHOT-jar-with-dependencies.jar;

-- loading raw data

CREATE EXTERNAL TABLE tweets_raw (
    text STRING,
    id BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
LOCATION 'upload/data/tweets_raw';

-- create sentiment dictionary
CREATE EXTERNAL TABLE dictionary (
    type string,
    length int,
    word string,
    pos string,
    stemmed string,
    polarity string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
LOCATION 'upload/data/dictionary';


-- Compute sentiment
create view l1 as select id, words from tweets_raw lateral view explode(sentences(lower(text))) dummy as words;
create view l2 as select id, word from l1 lateral view explode( words ) dummy as word ;

create view l3 as select 
    id, 
    l2.word, 
    case d.polarity
        when  'negative' then -1
        when 'positive' then 1 
        else 0 end as polarity 
from l2 left outer join dictionary d on l2.word = d.word;
 
create table tweets_sentiment stored as orc as select 
    id, 
    case 
        when sum( polarity ) > 0 then 1
        when sum( polarity ) < 0 then -1
        else 0 end as sentiment 
from l3 group by id;


-- Count numbers of positive/ngative tweets
CREATE TABLE sentiment_counter as select
    s.sentiment,
    count(1) count
from tweets_sentiment as s
group by s.sentiment order by count desc;
