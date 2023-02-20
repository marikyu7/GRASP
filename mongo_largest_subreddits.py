from pymongo import MongoClient                                                 
                                                                             
client = MongoClient("localhost", 27010)             
db = client.data
db.authenticate("guest", "supersecretguestpasswordformongo")

pipeline = [
        {'$group': {'_id': '$subreddit', 'posts_number': {'$sum': 1}}},
        {'$project': {'_id': 0, 'subreddit': '$_id', 'post_number': '$posts_number'}},
        {'$sort' : {'post_number': -1 }}
]


results = list(db.reap_main.aggregate(pipeline))

with open("query_result.txt", "w") as f:
    for item in results:
        f.write(str(item) + "\n")
