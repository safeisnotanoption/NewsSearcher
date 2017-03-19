#!/usr/bin/env python
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
import sqlite3
from tqdm import tqdm

es = Elasticsearch()

es.indices.create(index='fontanka', ignore=400)

es.index(index='fontanka', doc_type='news', id=1, body={'title': 'Hello', 'content': 'World'})

db = sqlite3.connect('Database.db')
rows = db.execute('SELECT title, content FROM fontanka').fetchall()

k = 2
for i in tqdm(range(len(rows) // 1000)):
    start = i * 1000
    end = (i + 1) * 1000
    titles = list(map(lambda z: z[0], rows))[start:end]
    contents = list(map(lambda z: z[1], rows))[start:end]
    for j in range(len(titles)):
        es.index(index='fontanka', doc_type='news', id=k, body={'title': titles[j], 'content': contents[j]})
        k += 1
    db.commit()

res = es.search(index='fontanka', doc_type='news', q='Продажи превысили')
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(title)s: %(content)s" % hit["_source"])
