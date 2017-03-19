#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import numpy
from flask import render_template, request, redirect, url_for, session, abort
from app import app
from elasticsearch import Elasticsearch

LOG = logging.getLogger('access')
es = Elasticsearch()


def ii(n):
    useless = " "
    for i in range(n):
        useless += "-"
    useless += " "
    return useless


@app.route('/', methods=['GET', 'POST'])
def index():
    LOG.info('Access: %s, %s, %s' % (request.remote_addr, request.args, request.form))

    anslist = []

    for k_get, v_get in request.args.items():
        results[k_get] = v_get

    for k_form, v_form in request.form.items():
        i = 1
        res = es.search(index='fontanka', doc_type='news', q=v_form)
        ans = "Найдено %d совпадений: " % res['hits']['total']
        anslist = [ans]
        for hit in res['hits']['hits']:
            #ans = ans + ii(10) + " Результат " + str(i) + ii(10) + (" %(title)s: %(content)s" % hit["_source"])
            ans = "Результат " + str(i) + (": %(title)s: %(content)s " % hit["_source"])
            anslist.append(ans)
            anslist.append(" ")
            i += 1

    return render_template('index.html', results=anslist)
