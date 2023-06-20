#!/home/bot/.venv/bin/python3

import pymysql
import time
import telepot

"""
This is free software released under MIT License

Copyright (c) 2015, 2019, 2022 Giorgio L. Rutigliano
(www.iltecnico.info, www.i8zse.eu, www.giorgiorutigliano.it)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

## Database configuration

# DB
DBHOST = 'localhost'
DBUSER = '<user>'
DBPASS = '<pass>'
DBNAME = '<db name>'
DBPORT = 3306

## Telegram configuration

TOKEN = '<token>'  ##  API token
cid = -1001     ##  Chat-id
lt = 0


# -------------------------------------------------------------------------------
def dbget(dbc, row):
    crl = dbc.cursor(pymysql.cursors.DictCursor)
    sql = 'select * from spot where rowid > %s order by rowid asc'
    crl.execute(sql, [row])
    rows = crl.fetchone()
    buf = ''
    while (rows != None):
        if row < int(rows['rowid']): row = int(rows['rowid'])
        tl = time.strftime(' | %d/%m %H:%M', time.localtime(rows['time']))
        buf += '{:<10}'.format(rows['spotcall']) + ' {:>9}'.format(rows['freq'])
        buf += ' dxcc:' + str(rows['spotdxcc'])
        buf += ' cqz:' + str(rows['spotcq'])
        buf += ' ituz:' + str(rows['spotitu'])
        if rows['comment'] is not None and rows['comment'] != '':
            buf += ' [' + str(rows['comment'])+']'
        buf += "\n"
        rows = crl.fetchone()

    dbc.commit()
    return buf, row


# ------------------------------------------------------------------------------

db = pymysql.connect(host=DBHOST, user=DBUSER, passwd=DBPASS, db=DBNAME, port=DBPORT)

cr = db.cursor(pymysql.cursors.DictCursor)
cr.execute('select max(rowid) as lst from spot')
data = cr.fetchone()
lst = int(data['lst'])
db.commit()
disc = "This is an experimental dxcluster telegram bot from I8ZSE [(@)grutig]"

bot = telepot.Bot(TOKEN)
bot.message_loop()
print('Listening ...')

# Keep the program running.
while 1:
    time.sleep(10)
    buf, row = dbget(db, lst)
    if buf != '':
        a = 0
        bot.sendMessage(cid, buf)
    ct = int(time.time())
    if ct - lt > 3600:
        bot.sendMessage(cid, disc)
        cr.execute('DELETE FROM spot WHERE spot.time < UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 45 DAY))')
        db.commit()
        lt = ct
