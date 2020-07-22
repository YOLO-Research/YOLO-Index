# my packages
import sqlite, index
from robin_stocks import stocks

# system packages
import time, os, json
from decimal import *
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

output_dir = os.environ.get("HOME") + "/public_html/reports"

def generate_template(db_file, tim=time.time()):
    date = datetime.fromtimestamp(tim).strftime("%m-%d-%Y")
    labels = []
    data = []
    stock_data = []
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('template.html')

    tim1 = tim - ((tim - 14400) % 86400)
    tim2 = min(tim, tim1 + 86400 - 1)

    conn = sqlite.create_connection(db_file)
    values = index.get_values(conn, tim1, tim2)
    for v in values:
        labels.append(1000 * v[0])
        data.append(round(v[1], 2))

    index_1 = index.get_composition(conn, tim1)
    index_2 = index.get_composition(conn, tim2)
    for i1 in index_1:
        i = 0
        while i < len(index_2):
            if index_2[i]['id'] == i1['id']:
                i1['price2'] = index_2[i]['price']
                index_2.pop(i)
            else:
                i += 1

    for i in index_1:
        if i.get("price2"):
            ticker = stocks.get_instrument_by_id(i.get('id'))['symbol']
            p_change = round(100 * (i['price2'] - i['price']) / i['price'], 2)
            v_change = (i['price2'] * i['weight']) - (i['price'] * i['weight'])
            p_html = ""
            v_html = ""
            if p_change > 0:
                p_html = "<td style='color: #84ff63;'>{:. 2f}%</td>".format(p_change)
                v_html = "<td style='color: #84ff63;'>{:. 2f}</td>".format(v_change)
            elif p_change < 0:
                p_html = "<td style='color: #ff6384;'>{:. 2f}%</td>".format(p_change)
                v_html = "<td style='color: #ff6384;'>{:. 2f}</td>".format(v_change)
            else:
                p_html = "<td style='color: #bbb;'>{:. 2f}%</td>".format(p_change)
                v_html = "<td style='color: #bbb;'>{:. 2f}</td>".format(v_change)

            stock_data.append((ticker, '{:. 2f}'.format(i['price']), '{:. 2f}'.format(i['price2']), p_html, v_html))

    output = template.render(date=date, data=json.dumps(data), labels=labels, stocks=stock_data)

    with open(output_dir + '/' + date + ".html", 'w') as f:
        f.write(output)
