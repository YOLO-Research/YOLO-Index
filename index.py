import sqlite

def sort_by_popularity(conn, date):
		"""
	Calculate and sort stock popularity at date. 
	Returns a sorted list of dictionaries containing {id, time, popularity, price}
	:param conn: SQLite Connection
	:type conn: SQLite Connection
	:param date: Date
	:type date: String "YYYY-MM-DD"
	"""

	output = []
	with conn:
		cursor = conn.cursor()
		query = "SELECT * FROM stock_info WHERE tm LIKE '{}' ORDER BY popularity desc".format(date)
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
	"""
	Calculate and sort the largest changes in popularity between date1 and date2.
	Returns a dictionary such that "{id}" : {popularity}
	:param conn: SQLite Connection
	:type conn: SQLite Connection
	:param date1: Earlier date
	:type date21: String "YYYY-MM-DD"
	:param date2: Later date
	:type date2: String "YYYY-MM-DD"
	"""

	d1 = sort_by_popularity(conn, date1)
	d2 = sort_by_popularity(conn, date2)

	output = {}
	for d in d2:
		try:
			output[d["id"]] = d["popularity"] - next(item for item in d1 if item["id"] == d["id"])["popularity"]
		except StopIteration:
			output[d["id"]] = d["popularity"]

	return sorted(output.items(), key=lambda x: x[1], reverse=True)