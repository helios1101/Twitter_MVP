# import necessary modules 
from datetime import datetime
from tweepy.streaming import StreamListener
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from tweepy import OAuthHandler , Stream , API
import json
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient

app = Flask(__name__)
api_flask = Api(app)

# Authentication Details
consumer_key = " "
consumer_secret = " "
access_token = " "
access_token_secret = " "
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = API(auth)

client = MongoClient('mongodb://localhost:27017')
db = client.local

tweets = {}

# A class to stream tweets and its metadata
class StdOutListener(StreamListener):
    def __init__(self,api,count):
        self.api = api
        self.counter = count + 1
        
    def on_status(self,data):
        self.counter -= 1
        if self.counter > 0 :
            tweet = {'Date':data.created_at , 'TweetId':str(data.id) ,'Tweet':data.text,
                    'AuthorID':str(data.user.id),'ScreenName':data.user.screen_name, 
                    'Source': data.source,'RetweetCount':data.retweet_count,
                    'FavoriteCount':data.favorite_count,'FollowerCount':data.user.followers_count,
                    'UserURL':data.user.url , 'Language' : data.user.lang} 
            db.Twitter_data.insert_one(tweet)
            
        else :
            return False

# A function to display query results  
def show_result(result):
    result = [{'Date': str(i['Date']) , 'TweetId':i['TweetId'] ,'Tweet':i['Tweet'],
                        'AuthorID':i['AuthorID'],'ScreenName':i['ScreenName'], 
                        'Source': i['Source'],'RetweetCount':i['RetweetCount'],
                        'FavoriteCount':i['FavoriteCount'],'FollowerCount':i['FollowerCount'],
                        'UserURL':i['UserURL'] ,'Language' : i['Language']} for i in result]
    return result

# Show table name  
class table_name(Resource):
    def get(self):
        return {'The name of table containing twitter data is' : 'Twitter_data'}

# Display column names    
class column_names(Resource):
    def get(self):
        column_names = "Date, TweetId, Tweet, AuthorID, ScreenName, Source, RetweetCount, FavoriteCount, FollowerCount, UserURL, Language"
        return {'Columns' : column_names}

# A query to filter integer columns		
class filter_int_columns(Resource):
    def get(self,column,operator,num):
        
        if column in ['FavoriteCount','RetweetCount','FollowerCount']:
            if operator == '=':
                result = db.Twitter_data.find({column: num})
            elif operator == '>':
                result = db.Twitter_data.find({column:{"$gt": num}})
            elif operator == '<':
                result = db.Twitter_data.find({column:{"$lt": num}})
            else:
                return 'Operator should be either one of the following : < , > , ='

            result = show_result(result)
            if len(result) > 0:
                return jsonify(result)
            else:
                return {'status' : 'No results found for the given filter.'}
        else:
            return {'status':'The input column should be an integer type column, i.e. one of : FavoriteCount, RetweetCount, FollowerCount'}

    
if __name__ == '__main__':
     app.run()