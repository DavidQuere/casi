"""
Transforme les fichiers archives Flume en un fichier de type csv
avec uniquement les colonnes id et contenu du tweet, séparés par un |
"""

import os
import gzip
import json
import re
import csv

ROOT = "./SentimentFiles/upload/data/tweets_raw"
ARCHIVES = [
    os.path.join(r, file)
    for r, _, f in os.walk(ROOT)
    for file in f
    if file.endswith(".gz")
]

RESULT_DIRECTORY_NAME = "tweets"

if not os.path.exists(RESULT_DIRECTORY_NAME):
    os.mkdir(RESULT_DIRECTORY_NAME)

CSV_COLUMNS = ["id", "text"]

NEW_DICT = []

for archive in ARCHIVES:
    print(archive)
    f = gzip.open(archive, "rb")
    file_content = f.read().decode("utf-8")
    f.close()
    file_content = re.sub("\n", ",", file_content)

    end = -1

    file_content = json.loads("[" + file_content[0:end] + "]")
    for value in file_content:
        NEW_DICT.append({"id": value["id"], "text": value["text"]})
with open(RESULT_DIRECTORY_NAME + "/1", "w") as csvfile:
    WRITER = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS, delimiter="|")
    WRITER.writeheader()
    for data in NEW_DICT:
        WRITER.writerow(data)
