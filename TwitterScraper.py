import tweepy
from tweepy import AppAuthHandler
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
from os import path
import uploadtoGDrive

def authenticate(api_key, api_key_secret, access_token, access_token_secret,
                 wait_on_rate_limit=True, 
                 wait_on_rate_limit_notify=True):
    
    try:
        _auth = AppAuthHandler(api_key, api_key_secret)
    except:
        print("Faild to authenticate.")
        return
    try:
        _api = tweepy.API(auth_handler=_auth,
                 wait_on_rate_limit=True, 
                 wait_on_rate_limit_notify=True)
        return _api
    except:
        print('Failed to access Twitter API')
        return
    
def getDate(format_ = '%m/%d/%Y'):
    return datetime.today().strftime(format_)

def getCreateDate(result):
    return result.created_at.strftime("%m/%d/%Y")

def getTweetId(result):
    return result.id

def getCustomDate(delta, format_):
    return datetime.strftime(datetime.now() - timedelta(delta), format_)

def checkMaxDate(current_date, max_date):
    current_date = datetime.strptime(current_date,'%m/%d/%Y')
    max_date = datetime.strptime(max_date,'%m/%d/%Y')
    
    if (current_date - max_date).days <= 9:
        return True
    else:
        return print('Max date exceeds the 7 days limit')

def saveToCsv(filename, search_results):

    if path.exists(filename+'.csv'):
        print('file exist')
        search_results.to_csv(filename+'.csv', mode='a', header=False, index=False, encoding='utf-8-sig')
        return print('File has been save in existing csv file')
    else:
        print('create new file')
        search_results.to_csv(filename+'.csv', index=False, encoding='utf-8-sig')
        return print('File has been save in csv file')
            

                
                 
def uploadToGDrive(filename):
    return uploadtoGDrive.upload(filename+'.csv')

def initializeTweetAttributes():
    tweet_attributes = {
        'id' : [],
        'date' : [],
        'time' : [],
        'user_id' : [],
        'name' : [],
        'username' : [],
        'tweet' : [],
        'retweet_count' : [],
        'favorite_count' : [],
        'link_tweet' : [],
        'url_in_tweet' : [],
        'hashtags' : [],
        'lang' : [],
        'in_reply_to_status' : [],
        'is_quote_status' : [],
        'is_retweet' : [],
        'retweeted_status' : [],
        'retweeted_status_name' : [],
        }
    return tweet_attributes
        
def appendSearchResult(result, tweet_attributes):
    tweet_attributes['id'].append(result.id)
    tweet_attributes['date'].append(result.created_at.strftime("%m/%d/%Y"))
    tweet_attributes['time'].append(result.created_at.strftime("%H:%M:%S"))
    tweet_attributes['user_id'].append(result.user.id_str)
    tweet_attributes['name'].append(result.user.name)
    tweet_attributes['username'].append(result.user.screen_name)
    tweet_attributes['tweet'].append(result.full_text)
    tweet_attributes['retweet_count'].append(result.retweet_count)
    tweet_attributes['favorite_count'].append(result.favorite_count)
    url = 'https://twitter.com/{username}/status/{id_tweet}'.format(username=result.user.screen_name, id_tweet=result.id)
    tweet_attributes['link_tweet'].append(url)
    
    try:
        tweet_attributes['url_in_tweet'].append(result.entities['urls'][0]['url'])
    except:
        tweet_attributes['url_in_tweet'].append('-')

    try:
        tweet_attributes['in_reply_to_status'].append(result.in_reply_to_screen_name)
    except:
        tweet_attributes['in_reply_to_status'].append('-')
        
    tweet_attributes['is_quote_status'].append(result.is_quote_status)
    try:
        if result.retweeted_status:
            tweet_attributes['is_retweet'].append('True')
            tweet_attributes['retweeted_status'].append(result.retweeted_status.full_text)
            tweet_attributes['retweeted_status_name'].append(result.retweeted_status.user.screen_name)
    except:
        tweet_attributes['is_retweet'].append('False')
        tweet_attributes['retweeted_status'].append('-')
        tweet_attributes['retweeted_status_name'].append('-')
        
    tweet_attributes['hashtags'].append(result.entities['hashtags'])
    
    try:
        tweet_attributes['lang'].append(result.lang)
    except:
        tweet_attributes['lang'].append('-')

def searchTweets(api, query, current_date, max_date, max_id,
                tweet_attributes,lang=None, locale=None, result_type='recent', print_dates=False):
    if checkMaxDate(current_date, max_date):
        if max_id==None:
            search_results = api.search(q=query, lang=None, locale=None, result_type='recent',
                                        count=5, tweet_mode='extended', until=getCustomDate(0,'%Y-%m-%d'))
            appendSearchResult(search_results[0], tweet_attributes)
            last_date = getCreateDate(search_results[0])
            if print_dates==True:
                print(last_date)
            last_id = getTweetId(search_results[0])    
    
            while last_date != max_date:
                
                search_results = api.search(q=query,max_id=last_id-1,lang=None, locale=None,
                                            count=100, tweet_mode="extended")
                if len(search_results) == 0:
                    print('search result is empty')
                    break
                for result in search_results:
                    appendSearchResult(result, tweet_attributes)
    
                last_date = getCreateDate(result)
                last_id = getTweetId(result) 
                print(last_id)
                if print_dates==True:
                    print(last_date)
                if last_date == max_date:
                    break
        else:
            print('run with max id')
            last_date = current_date
            while last_date != max_date:
                
                search_results = api.search(q=query,max_id=max_id-1, lang=None, locale=None, 
                                            count=100, tweet_mode="extended")
                if len(search_results) == 0:
                    print('search result is empty')
                    break
                for result in search_results:
                    appendSearchResult(result, tweet_attributes)
        
                last_date = getCreateDate(result)
                max_id = getTweetId(result)  
                print(max_id)
                if print_dates==True:
                    print(last_date)
                if last_date == max_date:
                    break

    
    tweet_attributes_df = pd.DataFrame(tweet_attributes)
    return tweet_attributes_df

def startScraping(api, queries, filename, tweet_attributes,max_date,
                  print_dates=False,lang=None, locale=None, result_type='recent'):     
    for i,query in enumerate(queries):
        tweet_attributes = initializeTweetAttributes()
        current_date = getDate()
        #max_date = getCustomDate(8,"%m/%d/%Y")
        max_id = None
        print(max_id==None)
        print(query)
        while True:
            try:
                search_results = searchTweets(api, query,current_date,max_date,max_id,tweet_attributes,
                                              lang, locale, result_type,print_dates)
                break
            except tweepy.TweepError as e:
                print(e.reason)
                time.sleep(30)
                max_id = tweet_attributes['id'][-1]
                current_date = tweet_attributes['date'][-1]
                continue
        try:
            saveToCsv(filename[i], search_results)
        except:
            print('failed to save file to csv')
            return search_results
        try :
            uploadToGDrive(filename[i])
        except :
            print('failed to upload file to google drive')
            return search_results
            continue
    return tweet_attributes

def __init__():
    print('Twitter Scraper')

if __name__ == "__main__":
    __init__()