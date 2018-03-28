# PART 1 - IMPORTS
from cassandra.cluster import Cluster 
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider

import nltk 
import pandas as pd
from collections import Counter
from more_itertools import flatten



# PART 2 - SETUP
# username and password
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
# host and port
cluster = Cluster(contact_points = ['qa.kepsla.com',], port=3000,auth_provider=auth_provider)
# keyspace 
session = cluster.connect("kp_cs")

session.default_timeout= 300

# PART 3 - DECLATATIONS
is_noun = lambda pos: pos[:2] == 'NN' # check during pos tagging to xhexk if given word is a noun
txt_review_list =[]
all_nouns = []


# PART 4 - FUNCTION THAT DOES POS TAGIING FOR GIVNE REVIEW TXT USING NLTK. 
def Process(rev_txt):
	# break the review txt into words and apply the tagger.
	token =nltk.pos_tag(nltk.word_tokenize(rev_txt.decode("utf8")))
	# if the given word is found to be a noun append it to ALL_NOUNS list.
	nouns = [word for (word,pos) in token if is_noun(pos)]
	all_nouns.append(nouns) 





# PART 3 - QUERING 
query = "SELECT txt from experience WHERE  prs='SOLOT' allow filtering;"
statement = SimpleStatement(query, fetch_size=10)
results = session.execute(statement)


for i,row in enumerate(results):
	if row.txt!='' and row.txt is not None:
    		print i, str(row.txt.encode('UTF-8'))
    		txt_review_list.append(row.txt.encode('UTF-8')) # storing all valid reviews in the list..



# PART 4 - PROCESSING EACH REVIEW IN THE LIST.
no_of_reviews = len(txt_review_list)
for i in range(no_of_reviews):
	print i
	Process(txt_review_list[i]) #passing each review to the process function .
	print "\n\n"



# PART 5 - NOUN PROCESSING
noun_list =list(flatten(all_nouns)) #putting all review nouns in a single list
df = pd.DataFrame({'NOUNS': noun_list}) # convering list to pandas dataframe.
df['NOUNS']=df['NOUNS'].str.lower() # lowercasing all nouns.
obj_bus = Counter(df['NOUNS']) # keeping only single occurance of non in the list and its corresponding frequency
obj_bus = obj_bus.most_common() #
my_df = pd.DataFrame(obj_bus) # new dataframe containing noun and its frequency.

my_df.columns = ['NOUN','FREQ'] # giving header to my dataframe.





# PART 7 - STORING FINAL RESULTS BACK TO DB
no_of_nouns = len(my_df)
for i in range(0, no_of_nouns):
    noun = my_df.iloc[i]['NOUN']
    session.execute(
    """
    INSERT INTO aspect_freq (prs, asp_kwrd, freq)
    VALUES (%s, %s, %s)
    """,
    ('SOLOT',noun, my_df.iloc[i]['FREQ'])
)