import pandas as pd
from collections import Counter

from cassandra.cluster import Cluster 
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider

data3= pd.read_csv('Aspect_categories.csv',sep=',')


		#print keyword_str


def aspect_category(word,persona):
	#print word
	word2 =' ' +str(word) + ','
	for row in data3.itertuples(index=True, name='Pandas'):
		keywords =getattr(row, "keywords")
		aspect_name = getattr(row, "name")
		aspect_id = str(getattr(row, "sys_sql_id"))
		keyword_str = str(keywords).strip('[]')

		if word2 in keyword_str:
			print word
			print aspect_name+"\n\n"

			session.execute(
		    """
		    UPDATE aspect_freq SET aspect = %s, aspect_id = %s
			WHERE asp_kwrd = %s and prs =%s
		    """,(aspect_name,aspect_id,word,persona))
		






# username and password
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
# host and port
cluster = Cluster(contact_points = ['qa.kepsla.com',], port=3000,auth_provider=auth_provider)
# keyspace 
session = cluster.connect("kp_cs")

session.default_timeout= 300


query = "SELECT asp_kwrd, prs from aspect_freq"
statement = SimpleStatement(query, fetch_size=10)
results = session.execute(statement)



for rows in results:
	aspect_category (rows.asp_kwrd.encode('UTF-8'),rows.prs)