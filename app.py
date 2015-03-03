# -*- coding: utf-8 -*-
import datetime
import json
import sys

import bottle
import bottle_sqlite

from config import config

app = bottle.Bottle()
app.install(bottle_sqlite.SQLitePlugin(dbfile=config.get('database')))


@app.route('/')
def index(db):
    cur = db.cursor()

    ret = {'topics': {}}

    # Yesterday
    date = datetime.datetime.now().date() - datetime.timedelta(days=1)

    # All tags on db
    # cur.execute("SELECT DISTINCT tags FROM entry")
    cur.execute(
        ("SELECT tags, COUNT(*) AS count FROM entry WHERE "
         "published_on BETWEEN ? AND ? GROUP BY tags ORDER BY count DESC"),
        (date, date + datetime.timedelta(days=1)))

    rows = cur.fetchall()
    for row in rows:
        name = row[0]
        count = row[1]

        # Get list of search terms and respective number of occurrences
        cur.execute(
            ("SELECT search_term, COUNT(*) AS count FROM entry WHERE tags= "
             "AND published_on BETWEEN ? AND ? GROUP BY search_term ORDER BY count DESC"),
            (row[0], date, date + datetime.timedelta(days=1)))

        rows = cur.fetchall()
        search_terms = []

        for r in rows:
            search_terms.append(r[0])
        search_terms = ', '.join(search_terms)

        d = {'name': name, 'count': count, 'search_terms': search_terms}
        ret['topics'][name] = d

    return json.dumps(ret)


## MAIN

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8013)
    sys.exit(0)
