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
                ## DO SOMETHING WITH THE LABELS HERE
    {'$addFields': {'labels': {'personality': '$personality'}}},
    {'$project':{'author_id': '$_id', 'labels': 1, '_id': 0}},
    {'$out': 'labelled_authors'}
]

db.labelled_authors.aggregate(group_personalities_pipeline)

