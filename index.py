import sqlite

def sort_by_popularity(conn, date):
	output = []
	with conn:
		cursor = conn.cursor()
		query = "SELECT * FROM stock_info WHERE tm LIKE '{}%' ORDER BY popularity".format(date)
		data = cursor.execute(query).fetchall()
		for d in data:
			output.append({
				"id": d[0],
				"tm": d[1],
				"popularity": d[2],
				"price": d[3]
				})
	return output

def sort_by_largest_change(conn, date1, date2):
	d1 = sort_by_popularity(conn, date1)
	d2 = sort_by_popularity(conn, date2)

	output = {}
	for d in d2:
		try:
			output[d["id"]] = d["popularity"] - next(item for item in d1 if item["id"] == d["id"])["popularity"]
		except StopIteration:
			print(d["id"])

	return sorted(output.items(), key=lambda x: x[1], reverse=True)