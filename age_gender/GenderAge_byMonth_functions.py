from pymongo import MongoClient
import pandas as pd
from bson.json_util import dumps, loads
import re
from tqdm import tqdm
from time import time

client = MongoClient("localhost", 27010)
db = client.research
db.authenticate("marilu", "topsecretpasswordformarilusmongo")

def query_mogoDB(patterns, regex_dic, posts_dic, path):
    '''
    A function to query a mongoDB collection selecting only those documents which text attribute contains the text of interest
    patterns: a list with the names of the different patterns (this names are the same of the dictionaries keys)
    regex_dic: a dictionary that link every pattern name to the actual pattern to be searched with regex
    db_file: mongoDB collection
    posts_dic: dictionary that will contain the objects queried (keys are the patterns)
    path: where the function is going to save the queriesd posts
    database_month: label to indicate the mongoBB collection (when saving)
    text_attribute: mongoDB attribute to query
    '''
    for p in tqdm(patterns, total=len(patterns)):
        main_db_pipeline = [
            {'$match': {'body': {'$regex': regex_dic[p]}}},
        ]

        results = list(db.july2021_all.aggregate(main_db_pipeline, allowDiskUse=True))
        posts_dic[p] = results

        with open(path + 'FULL_authors_{p}.json', 'w') as f:
            f.write(dumps(results, indent=2))

        return posts_dic


def remove_bots(patterns, posts_dic, bots_names, author_attribute='author'):
    '''
    A function to remove those authors that we know for sure are bots.
    patterns: a list with the names of the different patterns (this names are the same of the dictionaries keys)
    posts_dic: dictionary that will contain the objects queried (keys are the patterns)
    bots_names: a list with the names (string) of the bots
    author_attribute: mongoDB attribute that contain the bot name
    '''
    for p in patterns:
        for name in bots_names:
            posts_dic[p] = [author for author in posts_dic[p] if author[author_attribute] != name]

    return posts_dic

def assign_GenderAge(patterns, posts_dic, regex_dic, attribute_dic, db_year, text_attribute= 'body'):
    posts = []
    for p in patterns:
        for post in posts_dic[p]:
            text = post[text_attribute]
            match = re.search(regex_dic[p], text)
            post['regex_match'] = match.group(0)
            post['match_index'] = list(match.span())

            if match.group(0)[attribute_dic[p]['g']].lower() == 'f':
                post['gender'] = 'female'
            elif match.group(0)[attribute_dic[p]['g']].lower() == 'm':
                post['gender'] = 'male'
            post['age'] = match.group(0)[attribute_dic[p]['y1':'y2']]
            post['birth_year'] = db_year - int(post['age'])
        posts += posts_dic[p]
    return posts

def group_by_author(posts_list, database_month):
    authors_dic =  {}
    for post in posts_list:
        post_info = {"post_id": post['post_id'],
                     "regex_match": post['regex_match'],
                     "match_index": post['match_index'],
                     "subreddit_with_prefix": post['subreddit_with_prefix'],
                     "database_month": database_month}
        if post['_id'] not in authors_dic:
            authors_dic[post['_id']] = {"author_id": post['_id'],
                                        "labels": {}}
            authors_dic[post['_id']]['labels']['age'][post['age']] = [post_info]
            authors_dic[post['_id']]['labels']['gender'][post['gender']] = [post_info]
        else:
            if post['age'] in authors_dic[post['_id']]['labels']['age'].keys():
                authors_dic[post['_id']]['labels']['age'][post['age']].append(post_info)
            if post['age'] in authors_dic[post['_id']]['labels']['age'].keys():
                authors_dic[post['_id']]['labels']['age'][post['age']].append(post_info)
            if post['age'] not in authors_dic[post['_id']]['labels']['age'].keys():
                authors_dic[post['_id']]['labels']['age'][post['age']] = [post_info]
            if post['age'] not in authors_dic[post['_id']]['labels']['age'].keys():
                authors_dic[post['_id']]['labels']['age'][post['age']] = [post_info]
    return authors_dic
