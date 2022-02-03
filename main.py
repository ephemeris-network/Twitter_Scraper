import pandas as pd
import tweepy
import credentials
import functions
import scraper_input_data

#while True:
auth = tweepy.AppAuthHandler(credentials.consumer_key, credentials.consumer_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)    # set wait_on_rate_limit =True; as twitter may block you from querying if it finds you exceeding some limits

item_num = 50 #how many tweets to grab, starting from now and heading back

columns = ["Collection", "Tweets Scraped", "Polarity","Words Used"]
df_all_collection = pd.DataFrame(columns = columns)

main_list = functions.main_list(scraper_input_data.name_dict)

for i in range(len(main_list)): 
    
    Topics = main_list[i]
    tweets = []

    for i, topic in enumerate(Topics): 
        tweets.append(tweepy.Cursor(api.search_tweets, (topic),lang="en").items(item_num))

    count = 0
    for i, topic in enumerate(Topics):

        tweet_analyzer = functions.TweetAnalyzer(pd.DataFrame(columns=['Polarity - {}'.format(Topics[i])]))
        df = tweet_analyzer.tweets_to_data_frame(tweets[i], topic)
        if count == 0: 
            df_main = df
        else: 
            df_main = pd.concat([df_main, df], axis=1)
        count += 1

    Volume = []
    Polarity = []
    for i, topic in enumerate(Topics): 
        Volume.append(df_main['Polarity - {}'.format(topic)].shape[0])
        Polarity.append(df_main['Polarity - {}'.format(topic)].mean())

    zippedList =list(zip(Topics, Volume, Polarity))

    df_11 = pd.DataFrame(zippedList,columns=('Topics','Volume','Polarity'))
    df_sort = df_11.sort_values(by="Volume", ascending=False ,kind="mergesort")

    df_sort.to_csv('./Individual Collection Data/Sentiment - {}.csv'.format(Topics[0]),index=False)
    
    Collection_name = Topics[0]
    collection_polarity = df_sort["Polarity"].mean()
    new_row = {'Collection':Collection_name, 'Tweets Scraped':item_num, 'Polarity': collection_polarity, "Words Used":Topics}
    df_all_collection = df_all_collection.append(new_row, ignore_index=True)

    df_all_collection.to_csv('./All Collections Data/Sentiment - All Collections.csv',index=False)
