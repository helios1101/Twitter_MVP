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

tweet = {}

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

       
# A class to stream and store tweets in a NoSQL database	   
class store_tweets(Resource):
    def get(self,query,count):
        streaming = StdOutListener(api,count)
        stream = Stream(auth,streaming)
        track = query.split('or')
        stream.filter(track = track)
        return {'Status' : 'Data inserted in database.' }
  
# Show table name  
class table_name(Resource):
    def get(self):
        return {'The name of table containing twitter data is' : 'Twitter_data'}
  
# Delete twitter table from database  
class delete_table(Resource):
    def get(self):
        db.Twitter_data.drop()
        return {'Status' : 'The table of tweets was deleted.'}


# Check table size
class table_size(Resource):
    def get(self):
        try:
            result = db.Twitter_data.count()
            return {'Size of Twitter data table': result}
        except:
            return {'Size of Twitter data table': 0}
        
# Display column names    
class column_names(Resource):
    def get(self):
        column_names = "Date, TweetId, Tweet, AuthorID, ScreenName, Source, RetweetCount, FavoriteCount, FollowerCount, UserURL, Language"
        return {'Columns' : column_names}

# Display all the data inside the table
class show_table_data(Resource):
    def get(self):
        result = db.Twitter_data.find()
        result = show_result(result)
        return jsonify(result)

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

# A query to filter string columns
class filter_string_columns(Resource):
    def get(self,column,operator,text):
        
        if column in ['TweetdId','Tweet','AuthorId','ScreenName','Source','UserURL','Language']:
            if operator == 'starts':
                pattern = '^'+text
                result = db.Twitter_data.find( { column: { '$regex': pattern , '$options': 'i' } } )
            elif operator == 'ends':
                pattern = text+'$'
                result = db.Twitter_data.find( { column: { '$regex': pattern , '$options': 'i' } } )
            elif operator == '=':
                result = db.Twitter_data.find( { column: text } )
            elif operator == 'contains':
                pattern = '.*'+text+'.*'
                result = db.Twitter_data.find( { column: { '$regex': pattern , '$options': 'i' } } )
            else:
                return "Operator should be either one of the following : 'starts' , 'ends' , '=' , 'contains'"
    
            result = show_result(result)
            if len(result) > 0:
                return jsonify(result)
            else:
                return {'status' : 'No results found for the given filter.'}
                    
        else:
            return {'status':'The input column should be a string type column, i.e. one of : TweetdId, Tweet, AuthorId, ScreenName, Source, UserURL'}

# Sorting columns			
class sort(Resource):
    def get(self,column,order):
        
        if order == 'asc':
            result = db.Twitter_data.find().sort([(column, pymongo.ASCENDING)])
        elif order == 'desc':
            result = db.Twitter_data.find().sort([(column, pymongo.DESCENDING)])
        else:
            return {'Status' : "Order should be one of 'asc' or 'desc' "}
        
        result = show_result(result)
        if len(result) > 0:
            return jsonify(result)
        else:
            return {'Status' : 'No results found.'}

# Display tweets in specified range of dates
class date_range(Resource):
    def get(self,from_date,to_date):
        column = 'Date'
        from_d = str.split(from_date , '-')
        to_d = str.split(to_date , '-')
        from_d = datetime(int(from_d[0]) , int(from_d[1]) , int(from_d[2]),0,0,0)
        to_d = datetime(int(to_d[0]) , int(to_d[1]) , int(to_d[2]) , 23,59,59)
        result = db.Twitter_data.find( {"$and":[ {column:{"$gte": from_d}} ,{column:{"$lte": to_d}}] })            
        result = show_result(result)
        if len(result) > 0:
            return jsonify(result)
        else:
            return {'status' : 'No results found for the given range.'}
        
        

# Export the twitter table data into a csv file   
class export_to_csv(Resource):
    def get(self,path):
        result = db.Twitter_data.find()
        result = [{'Date': str(i['Date']) , 'TweetId':i['TweetId'] ,'Tweet':i['Tweet'].encode('utf-8'),
                        'AuthorID':i['AuthorID'],'ScreenName':i['ScreenName'], 
                        'Source': i['Source'].encode('utf-8'),'RetweetCount':i['RetweetCount'],
                        'FavoriteCount':i['FavoriteCount'],'FollowerCount':i['FollowerCount'],
                        'UserURL':i['UserURL'] ,'Language' : i['Language']} for i in result]
        if len(result) > 0:
            df = pd.DataFrame(np.zeros((len(result),10)))
            df.columns = ['Date', 'TweetId', 'Tweet', 'AuthorID', 'ScreenName', 'Source', 'RetweetCount', 'FavoriteCount', 'FollowerCount', 'UserURL']
            for i in range(len(result)):
                for j in df.columns:
                    df[j][i] = result[i][j]  
                    
            path = path.split('-')
            file = path[0] + ':/'
            l = len(path)
            for i in range(1,l-1):
                file = file + path[i] + '/'
            file = file + path[-1]
            
            df.to_csv(file , header = True , index = False)
            return {'Status' : 'The dataframe was successfully exported to a csv file.'}
        else:
            return {'Status' : 'No data to export'}
    

# adding resources for all the classes in the REST API  
api_flask.add_resource(delete_table , '/delete_table')
api_flask.add_resource(table_size , '/table_size')
api_flask.add_resource(table_name , '/table_name')
api_flask.add_resource(column_names , '/column_names')
api_flask.add_resource(store_tweets, '/store_tweets/<string:query>/<int:count>')
api_flask.add_resource(show_table_data, '/show_data')
api_flask.add_resource(filter_int_columns, '/filter_int/<string:column>/<string:operator>/<int:num>')
api_flask.add_resource(filter_string_columns, '/filter_string/<string:column>/<string:operator>/<string:text>') 
api_flask.add_resource(sort, '/sort/<string:column>/<string:order>')
api_flask.add_resource(date_range, '/date_range/<string:from_date>/<string:to_date>') 
api_flask.add_resource(export_to_csv,'/export_csv/<string:path>')

if __name__ == '__main__':
     app.run()
        
