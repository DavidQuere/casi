Utiliser notre projet

pré-requis (versions que nous avons utilisées, cela peut ne pas fonctionner avec d'autres) : 
- python 3.6.9
- hadoop 2.10.1
- hive 2.3.7
- avoir le dossier SentimentFiles

Etape 1 : transformer les fichiers Flume en un csv unique avec les colonnes id et text
dans le dossier parent de SentimentFiles, lancer :
python3 data_converter.py

Etape 2 : stocker les fichiers dans HDFS
lancer hadoop (avec l'utilisateur crée pour hadoop) en allant dans le dossier : /hadoop/sbin, mettre la commande ./start-all.sh
utiliser ensuite les commandes :
hdfs dfs -mkdir /dict
hdfs dfs -mkdir /tweets
hdfs dfs -put <votre-chemin>/SentimentFiles/upload/data/dictionary/dictionary.tsv /dict
hdfs dfs -put <votre-chemin>/tweets/1 /tweets

Etape 3 : créer les tables et vues dans Hive et faire les calculs
hive -f <votre-chemin>/hive-v2.sql

Etape 4 : voir les résultats
taper la commande : hive
puis : use tweets; 

Et voilà !

Les résultats obtenus sont les suivants : 
Neutre	52983
Positif	25944
Négatif	10915


