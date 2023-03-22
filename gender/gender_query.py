from pymongo import MongoClient
from bson.json_util import dumps, loads

# Connect to the MongoDB, change the connection string per your MongoDB environment
client = MongoClient("localhost", 27010)             
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

personalities = {'M': 'male',
                 'F': 'female'
                    }

subreddits = ['r/']

regex_dic = {'E': '(^|\s)[Ee][SsNn][TtFf][PpJj](\s|$)',
             'I': '(^|\s)[Ii][SsNn][TtFf][PpJj](\s|$)',
             'S': '(^|\s)[EeIi][Ss][TtFf][PpJj](\s|$)',
             'N': '(^|\s)[EeIi][Nn][TtFf][PpJj](\s|$)',
             'T': '(^|\s)[EeIi][SsNn][Tt][PpJj](\s|$)',
             'F': '(^|\s)[EeIi][SsNn][Ff][PpJj](\s|$)',
             'J': '(^|\s)[EeIi][SsNn][TtFf][Jj](\s|$)',
             'P': '(^|\s)[EeIi][SsNn][TtFf][Pp](\s|$)'
             }

for personality in personalities.keys():

    pipeline = [
        {'$match': {'subreddit_name_prefixed': {'$in': subreddits}}},
        {'$match': {'author_flair_text': {'$regex': regex_dic[personality]}}},
        {'$project': {'author': 1, 'author_flair_text': 1, 'subreddit_name_prefixed': 1, 'post_id': '$_id'}}, 
        {'$group': {'_id': '$author', 'subreddits': {'$addToSet': '$subreddit_name_prefixed'}, 'flairs': {'$addToSet': '$author_flair_text'}}},
        
    ]

    results = list(db.july2021_all.aggregate(pipeline))

    with open(f'personality/query_results/authors_{personalities[personality]}.json', 'w') as f:
        f.write(dumps(results, indent=2))