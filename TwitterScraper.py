import tweepy
from tweepy import AppAuthHandler
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
from os import path
import csv

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
    
def getCurrentDate(format_ = '%Y-%m-%d'):
    return datetime.today().strftime(format_)

def getCreateDate(result):
    return result.created_at.strftime('%Y-%m-%d')

def getTweetId(result):
    return result.id

def getCustomDate(delta, format_='%Y-%m-%d'):
    return datetime.strftime(datetime.now() - timedelta(delta), format_)

def checkMaxDate(current_date, max_date):
    current_date = datetime.strptime(current_date,'%Y-%m-%d')
    max_date = datetime.strptime(max_date,'%Y-%m-%d')
    
    if (current_date - max_date).days <= 9:
        return True
    else:
        return print('Max date exceeds the 7 days limit')
    
def n_day(since_date):
    current_date = getCurrentDate()
    current_date = datetime.strptime(current_date,'%Y-%m-%d')
    since_date = datetime.strptime(since_date,'%Y-%m-%d')
    
    return (current_date - since_date).days

def saveToCsv(filename, search_results):

    if path.exists(filename+'.csv'):
        print('file exist')
        search_results.to_csv(filename+'.csv', mode='a', header=False, index=False, encoding='utf-8-sig')
        return print('File has been save in existing csv file')
    else:
        print('create new file')
        search_results.to_csv(filename+'.csv', index=False, encoding='utf-8-sig')
        return print('File has been save in csv file')
            
        
def appendSearchResult(result, writer):
    url = 'https://twitter.com/{username}/status/{id_tweet}'.format(username=result.user.screen_name, id_tweet=result.id)
    try:
        url_in_tweet =result.entities['urls'][0]['url']
    except:
        url_in_tweet ='-'
    
    try:
        in_reply_to_status =result.in_reply_to_screen_name
    except:
        in_reply_to_status ='-'
        
    try:
        if result.retweeted_status:
            is_retweet='True'
            retweeted_status=result.retweeted_status.full_text
            retweeted_status_name=result.retweeted_status.user.screen_name
    except:
        is_retweet='False'
        retweeted_status='-'
        retweeted_status_name='-'
    try:
        lang=result.lang
    except:
        lang='-'
        
    writer.writerow({
    'id' : result.id,
    'date':result.created_at.strftime("%m/%d/%Y"),
    'time':result.created_at.strftime("%H:%M:%S"),
    'user_id':result.user.id_str,
    'name':result.user.name,
    'username':result.user.screen_name,
    'tweet':result.full_text,
    'retweet_count':result.retweet_count,
    'favorite_count':result.favorite_count,
    'link_tweet':url,
    'url_in_tweet':url_in_tweet,
    'in_reply_to_status': in_reply_to_status ,
    'is_quote_status':result.is_quote_status,
    'is_retweet':is_retweet,
    'retweeted_status':retweeted_status,
    'retweeted_status_name':retweeted_status_name,
    'hashtags':result.entities['hashtags'],
    'lang': lang})

def searchTweets(api, query, since_date, max_date, max_id,last_result, filename,
                lang=None, locale=None, result_type='recent', print_dates=False):
    
    if checkMaxDate(since_date, max_date):
        
        fieldnames = ['id','date','time','user_id','name','username',
        'tweet', 'retweet_count','favorite_count','link_tweet',
        'url_in_tweet', 'hashtags', 'lang','in_reply_to_status',
        'is_quote_status','is_retweet','retweeted_status',
        'retweeted_status_name' ]
        
        if path.exists(filename+'.csv'):
            csvfile = open("{}.csv".format(filename), 'a', encoding='utf-8-sig')
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        else:
            csvfile = open("{}.csv".format(filename), 'a', encoding='utf-8-sig')
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        if max_id==None:
            search_results = api.search(q=query, lang=None, locale=None, result_type=result_type,
                                        count=5, tweet_mode='extended', until=getCustomDate(n_day(since_date)-1,'%Y-%m-%d'))
            appendSearchResult(search_results[0], writer)
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
                    appendSearchResult(result, writer)
                    last_result['id'] = result.id
                    last_result['date'] = result.created_at.strftime("%Y-%m-%d")
    
                last_date = getCreateDate(result)
                last_id = getTweetId(result) 
                print(last_id)
                if print_dates==True:
                    print(last_date)
                if last_date == max_date:
                    break
        else:
            print('run with max id')
            last_date = since_date
            while last_date != max_date:
                
                search_results = api.search(q=query,max_id=max_id-1, lang=None, locale=None, 
                                            count=100, tweet_mode="extended")
                if len(search_results) == 0:
                    print('search result is empty')
                    break
                for result in search_results:
                    appendSearchResult(result, writer)
                    last_result['id'] = result.id
                    last_result['date'] = result.created_at.strftime("%Y-%m-%d")
        
                last_date = getCreateDate(result)
                max_id = getTweetId(result)  
                print(max_id)
                if print_dates==True:
                    print(last_date)
                if last_date == max_date:
                    break

    return result

def startScraping(api, queries, filename,since_date=getCurrentDate(), max_date=getCustomDate(8),
                  max_id=None, print_dates=True,lang=None, locale=None, result_type='recent'):     
    print(since_date)
    for i,query in enumerate(queries):
        print(query)
        last_result = {'id': 0, 'date':''}
        while True:
            try:
                searchTweets(api, query,since_date,max_date,max_id,last_result,filename[i],
                                              lang, locale, result_type,print_dates)
                break
            except tweepy.TweepError as e:
                print(e.reason)
                time.sleep(30)
                max_id = last_result['id']
                since_date = last_result['date']
                
                continue
        
        
    return 

def __init__():
    print('Twitter Scraper')

if __name__ == "__main__":
    __init__()
