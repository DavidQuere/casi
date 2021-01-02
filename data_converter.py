import os
import gzip
import json
import re
from nltk.tokenize import word_tokenize
import csv

root = "./SentimentFiles/upload/data/tweets_raw"
archives = names_IM = [os.path.join(r,file) for r,_,f in os.walk(root) for file in f if file.endswith('.gz')]

result_directory_name = "tweets"

if not os.path.exists(result_directory_name):
    os.mkdir(result_directory_name)

csv_columns = ['id','text']

new_dict = []

for archive in archives:
    print(archive)
    f = gzip.open(archive, 'rb')
    file_content = f.read().decode("utf-8")
    f.close()
    file_content = re.sub("\n", ",",file_content)

    # if "FlumeData.1367535703055.gz" in archive:
    #     file_content = re.sub("\\\\\"","'",file_content)
    #     file_content = re.sub("\\\\","",file_content)

    end = -1

    file_content = json.loads("["+file_content[0:end]+"]")
    for value in file_content:
        new_dict.append({"id": value["id"], "text": value["text"]})
with open(result_directory_name+"/1", 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter="|")
        writer.writeheader()
        for data in new_dict:
            writer.writerow(data)
