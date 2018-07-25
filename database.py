import pymysql
import appconfig
import numpy as np
import pandas as pd

__db = pymysql.connect(appconfig.db_host, appconfig.db_user, appconfig.db_password, appconfig.db_name)
__cursor = __db.cursor()

__user_table = (
    "id_user VARCHAR(25) PRIMARY KEY, " 
    "screen_name VARCHAR(100), "
    "location VARCHAR(100), "
    "followers_count INT, "
    "friends_count INT, "
    "listed_count INT, "
    "favourites_count INT, "
    "statuses_count INT, "
    "relevance DECIMAL(10,2)"
)

__tweet_table = (
    "id_tweet VARCHAR(25) PRIMARY KEY, "
    "created_at DATETIME, "    
    "in_reply_to_user_id_str VARCHAR(25), "
    "in_reply_to_screen_name VARCHAR(100), "
    "in_reply_to_status_id_str VARCHAR(25), "
    "retweet_count INT, "
    "favorite_count INT, "
    "lang VARCHAR(10), "
    "text TEXT, "
    "processed BOOLEAN, "
    "id_user VARCHAR(25), "
    "FOREIGN KEY (id_user) REFERENCES user(id_user)"
)

__training_table =(
    "id_training INT PRIMARY KEY AUTO_INCREMENT, "
    "id_tweet VARCHAR(25) UNIQUE, "
    "polarity VARCHAR(10), "
    "text TEXT "
)

__result_table = (
    "id_result INT PRIMARY KEY AUTO_INCREMENT, "
    "id_tweet VARCHAR(25) UNIQUE, "
    "id_user VARCHAR(25), "
    "user_screen_name VARCHAR(100), "
    "in_reply_to_id_user VARCHAR(25), "
    "in_reply_to_screen_name VARCHAR(100), "
    "polarity_model VARCHAR(10), "
    "polarity_dictionary VARCHAR(10), "
    "location VARCHAR(40), "
    "relevance DECIMAL(10,2), "
    "simple_text VARCHAR(255)"
)

def createTables():
    try:
        sql = "CREATE TABLE IF NOT EXISTS user("+ __user_table +")"
        __cursor.execute(sql)
        print("Tabla user creada")

        sql = "CREATE TABLE IF NOT EXISTS tweet("+ __tweet_table +")"
        __cursor.execute(sql)
        print("Tabla tweet creada")

        sql = "CREATE TABLE IF NOT EXISTS training("+ __training_table +")"
        __cursor.execute(sql)
        print("Tabla training creada")

        sql = "CREATE TABLE IF NOT EXISTS result("+ __result_table +")"
        __cursor.execute(sql)
        print("Tabla result creada")
    except:
        print("Ocurrio un error al crear una de las tablas")

# [id_tweet, polarity_dictionary, polarity_model, text]
def saveResult(result):
    skip = False

    exist = existTweet(result[0], "result")
    if exist is None:
        skip = True
    else:
        if exist:
            print("El tweet {} ya ha sido procesado".format(result[0]))
            skip = True

    if skip:
        return False
    else:
        sql = ("SELECT u.id_user, u.screen_name, u.location, u.followers_count, "
            "t.retweet_count, t.favorite_count, t.in_reply_to_user_id_str, t.in_reply_to_screen_name "
            "FROM tweet t, user u "
            "WHERE t.id_user = u.id_user AND t.id_tweet='{}'".format(result[0])
        )

        try:
            __cursor.execute(sql)
            [id_user, name, location, followers, retweet, favorite, reply_id, reply_name] = np.asarray(__cursor.fetchone())
            
            engagement = 0
            followers = int(followers)
            retweet = int(retweet)
            favorite = int(favorite)

            if followers > 0:
                engagement = ((retweet + favorite)/followers)*100
            
            standar_location = location

            sql = ("INSERT INTO result(id_tweet, id_user, user_screen_name, in_reply_to_id_user, in_reply_to_screen_name, polarity_dictionary, polarity_model, location, relevance, simple_text) " 
                "VALUES('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}')"
            ).format(result[0], id_user, name, reply_id, reply_name, result[1], result[2], standar_location, engagement, result[3])

            __cursor.execute(sql)
            __db.commit()

            #updateTweetProcessed(result[0], True)
            return True

        except:
            __db.rollback()
            print("Error al guardar el resultado:", result[0])
            return False

def saveTweet(tweet):
    skip = False

    exist = existUser(tweet.user.id_str)
    if exist is None:
        skip = True
    else:
        if not exist:
            if not saveUser(tweet.user):
                skip = True

    if skip:
        return False
    else:
        if 'retweeted_status' in dir(tweet):
            text = tweet.retweeted_status.full_text
        else:
            text = tweet.full_text
        # Remove line breaks
        text = text.replace("\n", "")
        # Remove unicode character
        text = (text.encode('ascii', 'ignore')).decode("utf-8")

        sql = "INSERT INTO tweet VALUES ('{}', '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', {}, '{}')".format(
            tweet.id_str,
            tweet.created_at,
            tweet.in_reply_to_user_id_str,
            tweet.in_reply_to_screen_name,
            tweet.in_reply_to_status_id_str,
            tweet.retweet_count,
            tweet.favorite_count,
            tweet.lang,
            text,
            False,
            tweet.user.id_str
        )
        try:
            __cursor.execute(sql)
            __db.commit()
            return True
        except:
            __db.rollback()
            print("Error al guardar el tweet:", tweet.id_str)
            return False

def saveUser(user):
    sql = "INSERT INTO user VALUES('{}', '{}', '{}', {}, {}, {}, {}, {}, {})".format(
        user.id_str,
        user.screen_name,
        user.location,
        user.followers_count,
        user.friends_count,
        user.listed_count,
        user.favourites_count,
        user.statuses_count,
        0.0
    )
    try:
        __cursor.execute(sql)
        __db.commit()
        return True
    except:
        __db.rollback()
        print("Error al guardar el usuario:", user.screen_name)
        return False

def existUser(id):
    sql = "SELECT COUNT(*) FROM user WHERE id_user='{}'".format(id)

    try:
        __cursor.execute(sql)
        (number_of_rows,) = __cursor.fetchone()
        return number_of_rows > 0
    except:
        print("Error al consultar si existe el usuario con id:", id)
        return None
    
def updateTweetProcessed(idTweet, state):
    sql = "UPDATE tweet SET processed={} WHERE id_tweet='{}'".format(state, idTweet)
    try:
        __cursor.execute(sql)
        __db.commit()
    except:
        __db.rollback()
        print("Error al actualizar el estado processed del tweet con id:", idTweet)

def getRandomTweets(amount):
    sql = "SELECT id_tweet, text FROM tweet ORDER BY RAND() LIMIT {}".format(amount)
    try:
        __cursor.execute(sql)
        results = np.asarray(__cursor.fetchall())

        df = pd.DataFrame(results)
        df.columns = ["id_tweet", "text"]
        return df
    except:
        print("Error no se puede obtener los {} tweets".format(amount))
        return None

def getUnprocessedTweets(start, end):
    sql = "SELECT id_tweet, text FROM tweet WHERE processed=false ORDER BY id_tweet LIMIT {}, {}".format(start, end)
    try:
        __cursor.execute(sql)
        results = np.asarray(__cursor.fetchall())
        df = pd.DataFrame(results)
        df.columns = ["id_tweet", "text"]
        return df
    except:
        print("Error no se puede obtener los tweets")
        return None

def getAllUnprocessedTweets():
    sql = "SELECT id_tweet, text FROM tweet WHERE processed=false ORDER BY id_tweet"
    try:
        __cursor.execute(sql)
        results = np.asarray(__cursor.fetchall())

        df = pd.DataFrame(results)
        df.columns = ["id_tweet", "text"]
        return df
    except:
        print("Error no se puede obtener los tweets")
        return None

def saveTrainingData(data):
    skip = False

    exist = existTweet(data[0], "training")
    if exist is None:
        skip = True
    else:
        if exist:
            skip = True

    if skip:
        return False
    else:
        sql = "INSERT INTO training (id_tweet, polarity, text) VALUES('{}', '{}', '{}')".format(
            data[0], data[1], data[2]
        )
        try:
            __cursor.execute(sql)
            __db.commit()
            return True
        except:
            __db.rollback()
            print("Error al guardar el tweet:", data[0])
            return False

def existTweet(id, table):
    sql = "SELECT COUNT(*) FROM {} WHERE id_tweet='{}'".format(table, id)

    try:
        __cursor.execute(sql)
        (number_of_rows,) = __cursor.fetchone()
        return number_of_rows > 0
    except:
        print("Error al consultar si existe el tweet con id:", id)
        return None

def getTrainingDataSet(num_positive, num_negative, num_neutral):
    sql1 = "SELECT polarity, text FROM training WHERE polarity='{}' ORDER BY RAND() LIMIT {}".format("positive", num_positive)
    sql2 = "SELECT polarity, text FROM training WHERE polarity='{}' ORDER BY RAND() LIMIT {}".format("negative", num_negative)
    sql3 = "SELECT polarity, text FROM training WHERE polarity='{}' ORDER BY RAND() LIMIT {}".format("neutral", num_neutral)
    
    try:
        data = []

        if num_positive > 0:
            __cursor.execute(sql1)
            data.extend(__cursor.fetchall()) 

        if num_negative > 0:
            __cursor.execute(sql2)
            data.extend(__cursor.fetchall()) 

        if num_neutral > 0:
            __cursor.execute(sql3)
            data.extend(__cursor.fetchall()) 

        training = np.asarray(data)
        df = pd.DataFrame(training)
        df.columns = ["polarity", "text"]
        df.sort_values(by=["text"])
        return df
    except:
        print("Error al obtener los tweets de entrenamiento")
        return None

def close():
    __cursor.close()
    __db.close()

if __name__ == '__main__':
    createTables()
