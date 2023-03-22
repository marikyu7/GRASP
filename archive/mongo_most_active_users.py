from pymongo import MongoClient                                                 
                                                                             
client = MongoClient("localhost", 27010)             
db = client.data
db.authenticate("guest", "supersecretguestpasswordformongo")

pipeline = [
        #{'$limit': 100000},
        {'$group': {'_id': {'subreddit': '$subreddit', 'user_id': '$author_fullname'}, 'count': {'$sum': 1}}},
        {'$group': {'_id': '$_id.subreddit',
                    'number_active_users':
                        {'$sum':
                            {'$cond': [ {'$gte':['$count', 10]},
                                        {'$sum': 1},
                                        {'$sum': 0}
                                    ]   
                            }
                         }
                    }
        },
        {'$sort': {'number_active_users': -1}}

]


results = list(db.reap_main.aggregate(pipeline, allowDiskUse=True))

with open("active_users_query.txt", "w") as f:
    for item in results:
        f.write(str(item) + "\n")