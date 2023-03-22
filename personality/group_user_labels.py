from pymongo import MongoClient
import json
import os
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
from bson.json_util import dumps, loads

# Connect to the MongoDB, change the connection string pmongodb accumulate from several documenter your MongoDB environment
client = MongoClient("localhost", 27010)             
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

group_personalities_pipeline = [
    {'$unwind': {'path': '$labels.personality.extrovert', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.introvert', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.sensing', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.intuitive', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.thinking', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.feeling', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.judging', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.perceiving', 'preserveNullAndEmptyArrays': True}},
    {'$group': {'_id': '$author_id',
                'extrovert': {'$push': '$labels.personality.extrovert'},
                'introvert': {'$push': '$labels.personality.introvert'},
                'sensing': {'$push': '$labels.personality.sensing'},
                'intuitive': {'$push': '$labels.personality.intuitive'},
                'thinking': {'$push': '$labels.personality.thinking'},
                'feeling': {'$push': '$labels.personality.feeling'},
                'judging': {'$push': '$labels.personality.judging'},
                'perceiving': {'$push': '$labels.personality.perceiving'},
                }},
    {'$addFields': {'personality': {'extrovert': '$extrovert',
                                    'introvert': '$introvert',
                                    'sensing': '$sensing',
                                    'intuitive': '$intuitive',
                                    'thinking': '$thinking',
                                    'feeling': '$feeling',
                                    'judging': '$judging',
                                    'perceiving': '$perceiving',
                                    }}},
    {'$addFields': {'labels': {'personality': '$personality'}}},
    {'$project':{'author_id': '$_id', 'labels': 1, '_id': 0}}
]

results = list(db.labelled_authors.aggregate(group_personalities_pipeline))

with open('personality/query_results/group.json', 'w') as f:
    f.write(dumps(results, indent=2))


