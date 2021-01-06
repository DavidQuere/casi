-- création de la base de données et utilisation
CREATE DATABASE tweets;
USE tweets;

-- chargement des données des tweets à partir des fichiers stockés dans hdfs
CREATE EXTERNAL TABLE tweets (
    id BIGINT,
    text STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
LOCATION '/tweets';

-- chargement du dictionnaire à partir des fichiers stockés dans hdfs
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
LOCATION '/dict';


-- création d'une vue séparant les mots au sein des tweets
create view word_from_tweets as select id, words from tweets lateral view explode(split(REGEXP_REPLACE(lower(text), '[^0-9A-Za-z ]+', ''), ' ')) w as words;

-- création d'une associant une polarité à un mot
create view tweets_polarity as select 
    id, 
    word_from_tweets.words, 
    case d.polarity
        when  'negative' then -1
        when 'positive' then 1 
        else 0 end as polarity 
from word_from_tweets left outer join dictionary d on word_from_tweets.words = d.word;
 
-- création d'une table associant une polarité à un tweet
create table tweets_sentiment stored as orc as select 
    id, 
    case 
        when sum( polarity ) > 0 then 1
        when sum( polarity ) < 0 then -1
        else 0 end as sentiment 
from tweets_polarity group by id;


-- création d'une table comptant le nombre de tweets positifs, négatifs et neutres
CREATE TABLE sentiment_counter as select
    s.sentiment,
    count(1) count
from tweets_sentiment as s
group by s.sentiment order by count desc;
