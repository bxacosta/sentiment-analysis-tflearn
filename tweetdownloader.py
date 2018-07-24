import tweepy
import appconfig
import dataloader
import simplifier
import database
import re

# Configura las credenciales OAuth de tweepy
__auth = tweepy.OAuthHandler(appconfig.consumer_key, appconfig.consumer_secret)
__auth.set_access_token(appconfig.access_token, appconfig.access_token_secret)
__api = tweepy.API(__auth, wait_on_rate_limit=True)

__users_id = appconfig.accounts_id

# Descarga todos los tweets que contega el string especificado
#
# query: String a buscar en los tweets
def getTweetsByQuery(query, number=None):
    alltweets = []
    # Inicia una peticion para obtener los ultimos tweets
    new_tweets = __api.search(q=query + " -filter:retweets", lang="es", count=200, tweet_mode='extended')
    if len(new_tweets) > 0:
        # Guarda el id del ultimo tweet obtenido
        oldest = new_tweets[-1].id - 1
        # Agrega los nuevos tweets filtrados
        filter_tweet = __filterData(new_tweets)
        if len(filter_tweet) > 0:
            alltweets.extend(filter_tweet)    
        print("{} tweets descargados".format(len(alltweets)))

    # Obtiene todos los tweets posibles
    while len(new_tweets) > 0:
        # Detiene la descarga si se descarga la cantidad especificada
        if (number is not None) and len(alltweets) >= number:
            break

        try:
            new_tweets = __api.search(q=query  + " -filter:retweets", lang="es", count=200, max_id=oldest, tweet_mode='extended')
            oldest = new_tweets[-1].id - 1
            filter_tweet = __filterData(new_tweets)
            if len(filter_tweet) > 0:
                alltweets.extend(filter_tweet)
            print("{} tweets descargados".format(len(alltweets)))
        except:
            print("Limite de peticiones excedido")
            print("{} tweets descargados".format(len(alltweets)))
            break

    return alltweets

def findUser(screen_name):
    return __api.get_user(screen_name)

def __filterData(tweets):
    filter_tweets = []

    for tweet in tweets:
        reply_user_id = tweet.in_reply_to_user_id_str
        location = tweet.user.location 
        if (reply_user_id in __users_id) and (location is not None):
            if (len(location) > 0) and (not re.search("[$&+:=?@#|'<>^*()%!]", location)):
                simple_location = location.lower()
                simple_location = simplifier.removeAccents(simple_location)
                simple_location = simplifier.removeUnicode(simple_location)
                for [province, city] in dataloader.locations():
                    if (simple_location.find(province) >= 0) or (simple_location.find(city) >= 0):
                        tweet.user.location = city
                    elif simple_location.find("ecuador") >= 0:
                        tweet.user.location = "ecuador"
                    else:
                        tweet.user.location = simple_location

                filter_tweets.append(tweet)

    return filter_tweets

if __name__ == '__main__':
    tweets = getTweetsByQuery("Lenin")

    num_saved = 0
    for tweet in tweets:
        if database.saveTweet(tweet): 
            num_saved += 1
        if num_saved%10 == 0:
            print("{} twetts guardados en la base".format(num_saved))

    database.close()