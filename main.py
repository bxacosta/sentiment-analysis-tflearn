import database as db 
import sentiment as sm 
import simplifier as sp

if __name__ == '__main__':
    model, vocabulary, classes = sm.initNeuralNet()

    tweets = db.getUnprocessedTweets(0, 100).values

    num = 0
    for [id_tweet, text] in tweets:
        text = sp.minimize(text)
        polarityDict  = sm.dictionary(text, True) 
        polarityModel = sm.neuralNet(text, model, vocabulary, classes, True)
        db.saveResult([id_tweet, polarityDict, polarityModel, text])
        num += 1
        if num%10 == 0:
            print("{} tweets procesados".format(num))
    
    print("{} tweets procesados en total".format(num))