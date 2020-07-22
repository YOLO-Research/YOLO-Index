# my packages
import sqlite, index
from robin_stocks import stocks

# system packages
import time, os, json
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

output_dir = os.environ.get("HOME") + "/public_html/reports"

def generate_template(db_file, tim=time.time()):
    date = datetime.fromtimestamp(tim - 14400).strftime("%m-%d-%Y")
    labels = []
    data = []
    stock_data = []
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('template.html')

    tim1 = tim - ((tim - 14400) % 86400)
    tim2 = (tim1 + 86400 - 1)
    if tim2 > tim: tim2 = tim

    conn = sqlite.create_connection(db_file)
    with conn:
        values = index.get_values(conn, tim1, tim2)
        for v in values:
            labels.append(1000 * v[0])
            data.append(round(v[1], 2))

        index_1 = index.get_composition(conn, tim1)
        index_2 = index.get_composition(conn, tim2)
        for i in range(len(index_1)):
            s = stocks.get_instrument_by_id(index_1[i]['id'])['symbol']
            p_change = round(100 * (index_2[i]['price'] - index_1[i]['price']) / index_1[i]['price'], 2)
            v_change = round((index_2[i]['price'] * index_2[i]['weight']) - (index_1[i]['price'] * index_1[i]['weight']), 2)
            p_html = ""
            v_html = ""
            if p_change > 0:
                p_html = "<td style='color: #84ff63;'>{}%</td>".format(p_change)
                v_html = "<td style='color: #84ff63;'>{}</td>".format(v_change)
            elif p_change < 0:
                p_html = "<td style='color: #ff6384;'>{}%</td>".format(p_change)
                v_html = "<td style='color: #ff6384;'>{}</td>".format(v_change)
            else:
                p_html = "<td style='color: #bbb;'>{}%</td>".format(p_change)
                v_html = "<td style='color: #bbb;'>{}</td>".format(v_change)

            stock_data.append((s, index_1[i]['price'], index_2[i]['price'], p_html, v_html))

    output = template.render(date=date, data=json.dumps(data), labels=labels, stocks=stock_data)

    with open(output_dir + '/' + date + ".html", 'w') as f:
        f.write(output)
