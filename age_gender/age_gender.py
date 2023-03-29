from pymongo import MongoClient
from bson.json_util import dumps, loads
import GenderAge_byMonth_functions as gender_age 

# Connect to the MongoDB, change the connection string per your MongoDB environment
client = MongoClient("localhost", 27010)
db = client.research
db.authenticate("marilu", "topsecretpasswordformarilusmongo")

database_month = '07-2021'
db_year = 2021
path = 'age&gender/raw_dfs/'
bots_names = ['[deleted]']

patterns = ['(YYG)', '(GYY)', 'YYG', 'GYY']

regex_dic = {'(YYG)': "(My|my|I|I am|I'm)\s\([0-9][0-9][MmfF]\)",
             '(GYY)': "(My|my|I|I am|I'm)\s\([MmfF][0-9][0-9]\)",
             'YYG': "(My|my|I|I am|I'm)(,|, | )[0-9][0-9][MmfF]",
             'GYY': "(My|my|I|I am|I'm)(,|, | )[MmfF][0-9][0-9]"
            }

posts_dic_null = {'(YYG)': None, '(GYY)': None, 'YYG':None, 'GYY': None}

attribute_dic = {'(YYG)': {'g': -2, 'y1': -4, 'y2':-2},
                 '(GYY)': {'g': -4, 'y1': -3, 'y2':-1},
                 'YYG': {'g': -3, 'y1':-2, 'y2': None},
                 'GYY': {'g': -1, 'y1':-3, 'y2': -1}
                }

posts_dic = gender_age.query_mogoDB(patterns, regex_dic, posts_dic_null, path)

posts_dic = gender_age.remove_bots(patterns, posts_dic, bots_names)

posts = gender_age.assign_GenderAge(patterns, posts_dic, regex_dic, attribute_dic, db_year)
