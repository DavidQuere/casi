-- loading raw data

CREATE EXTERNAL TABLE tweets_raw (
    id BIGINT,
    text STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
LOCATION '/tweets';

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
LOCATION '/dictionary';


-- Compute sentiment
create view word_from_tweets as select id, words from tweets lateral view explode(split(lower(text), ' ')) w as words;

create view tweets_polarity as select 
    id, 
    word_from_tweets.words, 
    case d.polarity
        when  'negative' then -1
        when 'positive' then 1 
        else 0 end as polarity 
from word_from_tweets left outer join dictionary d on word_from_tweets.words = d.word;
 
create table tweets_sentiment stored as orc as select 
    id, 
    case 
        when sum( polarity ) > 0 then 1
        when sum( polarity ) < 0 then -1
        else 0 end as sentiment 
from tweets_polarity group by id;


-- Count numbers of positive/ngative tweets
CREATE TABLE sentiment_counter as select
    s.sentiment,
    count(1) count
from tweets_sentiment as s
group by s.sentiment order by count desc;
