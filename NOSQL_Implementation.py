# import necessary modules 
from datetime import datetime
from tweepy.streaming import StreamListener
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from tweepy import OAuthHandler , Stream , API

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

    
if __name__ == '__main__':
     app.run()