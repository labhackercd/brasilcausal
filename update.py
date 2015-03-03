# -*- coding: utf-8 -*-
import hashlib
import time
import datetime
import requests
import sqlite3
import sys
from config import config

con = sqlite3.connect(config.get('database'))


def request_seekr(method_name, payload):
    base_url = 'http://monitoramento.seekr.com.br/api/'

    ts = int(time.time())
    hash = hashlib.sha1('%s%s' % (config.get('api_secret'), str(ts), )).hexdigest()

    payload['ts'] = ts
    payload['key'] = config.get('api_key')
    payload['hash'] = hash

    return requests.get(base_url + method_name, params=payload)


def update_results():
    # con.execute("DROP TABLE IF EXISTS 'entry'")

    # con.execute("CREATE TABLE entry (id INTEGER PRIMARY KEY, title TEXT, text TEXT, url TEXT,
    # media_id TEXT, published_on TIMESTAMP, user TEXT, user_id INTEGER, user_image TEXT,
    # attached_image_url TEXT, social_media TEXT, search_term TEXT, polarization INT,
    # reach INT, favorite BOOLEAN, tags TEXT)")

    num_page = 1
    date_from = datetime.datetime.now() - datetime.timedelta(days=1)
    date_to = datetime.datetime.now() - datetime.timedelta(days=1)

    last_date = None

    while True:
        print 'Populating page ', num_page
        search_results = request_seekr('search_results.json', {
            'search_id': config.get('search_id'), 'page': num_page,
            'per_page': 100, 'date_from': date_from, 'date_to': date_to
        })

        search_results_list = search_results.json()['search_results']
        n = len(search_results_list)

        for r in search_results_list:
            d = datetime.datetime.strptime(r.get('published_on', '')[:10], '%Y-%m-%d')
            if d != last_date:
                print 'Capturing date', d
                last_date = d

            con.execute(
                ('INSERT OR IGNORE INTO entry (id, title, text, url, media_id, published_on,'
                 'user, user_id, user_image, attached_image_url, social_media, search_term,'
                 'polarization, reach, favorite, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,'
                 '?, ?, ?, ?, ?, ?, ?)'), (
                    r.get('id', None),
                    r.get('title', ''),
                    r.get('text', ''),
                    r.get('url', ''),
                    r.get('media_id', ''),
                    r.get('published_on', ''),
                    r.get('user', ''),
                    r.get('user_id', None),
                    r.get('user_image', ''),
                    r.get('attached_image_url', ''),
                    r.get('social_media', ''),
                    r.get('search_term', ''),
                    r.get('polarization', None),
                    r.get('reach', None),
                    r.get('favorite', None),
                    r.get('tags', ''),
                ))
            con.commit()

        if n < 100:
            break
        else:
            num_page += 1

    con.close()

    print 'Done'


def parse_results():
    cur = con.cursor()

    # Past few days
    dates = []
    for i in reversed(range(1, 2)):
        d = datetime.datetime.now().date() - datetime.timedelta(days=i)
        dates.append(d)

    # All entries
    cur.execute("SELECT * FROM entry")
    rows = cur.fetchall()
    # for row in rows:
    # print row

    # All tags on db
    # cur.execute("SELECT DISTINCT tags FROM entry")
    cur.execute(
        "SELECT tags, COUNT(*) AS count FROM entry WHERE published_on BETWEEN ? AND ? GROUP BY tags ORDER BY count DESC",
        (dates[0], dates[-1] + datetime.timedelta(days=1)))
    tags = []
    rows = cur.fetchall()
    for row in rows:
        tags.append(row[0])

    for tag in tags:
        print tag
        search_terms = set()

        for date in dates:
            cur.execute("SELECT COUNT(*) FROM entry WHERE tags=? AND published_on BETWEEN ? AND ?",
                        (tag, date, date + datetime.timedelta(days=1)))
            i = cur.fetchone()[0]

            print u'%s: %s ocorrências' % (date, i,)

        # Get list of search terms and respective number of occurrences
        cur.execute(
            "SELECT search_term, COUNT(*) AS count FROM entry WHERE tags=? AND published_on BETWEEN ? AND ? GROUP BY search_term ORDER BY count DESC",
            (tag, dates[0], dates[-1] + datetime.timedelta(days=1)))
        rows = cur.fetchall()
        s = 'Termos de busca: '
        for row in rows:
            s += '%s (%s), ' % (row[0], row[1])

        print s
        print

    con.close()

    print 'Pronto!'

if __name__ == '__main__':
    sys.exit(update_results() or 0)
