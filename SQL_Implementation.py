# import necessary modules 
from tweepy.streaming import StreamListener
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from tweepy import OAuthHandler , Stream , API
from sqlalchemy import Table,select,insert,create_engine,MetaData,cast,desc
from sqlalchemy import Column,String,Integer,Date
import json
import pyodbc
import urllib
import pandas as pd
import numpy as np

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
 
quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};Server=<server name>;Database=<database>;UID=<username>;PWD=<password>;Port=1433;Trusted_connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))  
conn = engine.connect()
metadata = MetaData()
twitter = Table('Twitter_Data' , metadata ,
                Column('Date',Date()),
                Column('TweetId',String(50)),
                Column('Tweet',String(5000)),
                Column('AuthorId',String(50)),
                Column('ScreenName',String(100)),
                Column('Source',String(500)),
                Column('RetweetCount',Integer()),
                Column('FavoriteCount',Integer()),
                Column('FollowerCount',Integer()),
                Column('UserURL',String(500)),
                Column('Language',String(50)),
                extend_existing=True
                )
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
            metadata.create_all(engine)
            stmt = insert(twitter)
            conn.execute(stmt , tweet)
            
        else :
            return False
        
  
# A function to display query results  
def show_result(result):
    result = [{'Date': str(i[0]) , 'TweetId':i[1] ,'Tweet':i[2],
                        'AuthorID':i[3],'ScreenName':i[4], 
                        'Source': i[5],'RetweetCount':i[6],
                        'FavoriteCount':i[7],'FollowerCount':i[8],
                        'UserURL':i[9] ,'Language' : i[10]} for i in result]
    return jsonify(result)

       
# A class to stream and store tweets in a SQL database	   
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
        return {'The name of table containing twitter data is' : 'Twitter_Data'}
  
# Delete twitter table from database  
class delete_table(Resource):
    def get(self):
        twitter = Table('Twitter_Data',metadata,autoload=True,autoload_with = engine)
        twitter.drop(engine)
        return {'Status' : 'The table of tweets was deleted.'}

# Check table size
class table_size(Resource):
    def get(self):
        try:
            stmt = 'SELECT COUNT(*) FROM Twitter_Data'
            result = conn.execute(stmt).scalar()
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
        stmt = 'SELECT * FROM Twitter_Data'
        result = conn.execute(stmt).fetchall()
        result = show_result(result)
        return result

# A query to filter integer columns		
class filter_int_columns(Resource):
    def get(self,column,operator,num):
        twitter = Table('Twitter_Data',metadata,autoload=True,autoload_with = engine)
        
        if column in ['FavoriteCount','RetweetCount','FollowerCount']:
            stmt = select([twitter])
            if operator == '=':
                stmt = stmt.where(twitter.columns[column] == num)
            elif operator == '<':
                stmt = stmt.where(twitter.columns[column] < num)
            elif operator == '>':
                stmt = stmt.where(twitter.columns[column] > num)
            else:
                return 'Operator should be either one of the following : < , > , ='

            result = conn.execute(stmt).fetchall()
            if len(result) > 0:
                result = show_result(result)
                return result
            else:
                return {'status' : 'No results found for the given filter.'}
        else:
            return {'status':'The input column should be an integer type column, i.e. one of : FavoriteCount, RetweetCount, FollowerCount'}

# A query to filter string columns
class filter_string_columns(Resource):
    def get(self,column,operator,text):
        twitter = Table('Twitter_Data',metadata,autoload=True,autoload_with = engine)
        
        if column in ['TweetdId','Tweet','AuthorId','ScreenName','Source','UserURL','Language']:
            stmt = select([twitter])
            if operator == 'starts':
                stmt = stmt.where(twitter.columns[column].startswith(text))
            elif operator == 'ends':
                stmt = stmt.where(twitter.columns[column].endswith(text))
            elif operator == '=':
                text = text.encode('utf-8')
                stmt = stmt.where(twitter.columns[column] == text)
            elif operator == 'contains':
                pattern = '%'+text+'%'
                stmt = stmt.where(twitter.columns[column].like(pattern))
            else:
                return "Operator should be either one of the following : 'starts' , 'ends' , '=' , 'contains'"
    
            result = conn.execute(stmt).fetchall()
            if len(result) > 0:
                result = show_result(result)
                return result
            else:
                return {'status' : 'No results found for the given filter.'}
                    
        else:
            return {'status':'The input column should be a string type column, i.e. one of : TweetdId, Tweet, AuthorId, ScreenName, Source, UserURL'}

# Sorting columns			
class sort(Resource):
    def get(self,column,order):
        twitter = Table('Twitter_Data',metadata,autoload=True,autoload_with = engine)
        stmt = select([twitter])
        if order == 'asc':
            stmt = stmt.order_by(twitter.columns[column])
        elif order == 'desc':
            stmt = stmt.order_by(desc(twitter.columns[column]))
        else:
            return {'Status' : "Order should be one of 'asc' or 'desc' "}
        result = conn.execute(stmt).fetchall()
        if len(result) > 0:
            result = show_result(result)
            return result
        else:
            return {'Status' : 'No results found.'}

# Display tweets in specified range of dates
class date_range(Resource):
    def get(self,from_date,to_date):
        twitter = Table('Twitter_Data',metadata,autoload=True,autoload_with = engine)
        stmt = select([twitter])
        stmt = stmt.where(cast(twitter.columns.Date,String).between(from_date,to_date))
        result = conn.execute(stmt).fetchall()
        if len(result) > 0:
            result = show_result(result)
            return result
        else:
            return {'status' : 'No results found for the given range.'}
        
# Implement your own custom query
class custom_query(Resource):
    def get(self,query):
        result = conn.execute(query).fetchall()
        if len(result) > 0:
            result = show_result(result)
            return result
        else:
            return {'status' : 'No results found for the given query.'}
   
# Export the twitter table data into a csv file   
class export_to_csv(Resource):
    def get(self,path):
        stmt = 'SELECT * FROM Twitter_Data'
        result = conn.execute(stmt).fetchall()
        if len(result) > 0:
            result = [{'Date':i[0] , 'TweetId':i[1] ,'Tweet':i[2].encode('utf-8'),
                        'AuthorID':i[3],'ScreenName':i[4].encode('utf-8'), 
                        'Source': i[5].encode('utf-8'),'RetweetCount':i[6],
                        'FavoriteCount':i[7],'FollowerCount':i[8],
                        'UserURL':i[9] ,'Language' : i[10]} for i in result ]
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
api_flask.add_resource(custom_query,'/custom/<string:query>')
api_flask.add_resource(export_to_csv,'/export_csv/<string:path>')

if __name__ == '__main__':
     app.run(host='0.0.0.0')
        
