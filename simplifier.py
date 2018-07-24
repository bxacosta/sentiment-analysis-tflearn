import re
import dataloader
import numpy as np
from nltk.stem.snowball import SnowballStemmer

def isStopword(word):
    stopwords = dataloader.stopwords()["stopword"].values
    return word in stopwords

def getSteamm(word):
    stemmer = SnowballStemmer("spanish")
    return stemmer.stem(word)

def getLemma(word):
    lemmas = dataloader.lemmas()
    result = lemmas.loc[lemmas["variation"] == word]
    if result.empty:
        return word
    else:
        return result["main"].values[0]

def removeReferences(text):
    words = text.split(" ")
    # Quitar hastag, usuarios, enlaces
    for word in words:
        if (word.find("@") >= 0) or (word.find("#") >= 0) or (word.find("http") >= 0) or (word.find("https") >= 0):
            text = text.replace(word, "")
    return text

def removeAccents(text):
    text = text.replace("á", "a")
    text = text.replace("é", "e")
    text = text.replace("í", "i")
    text = text.replace("ó", "o")
    text = text.replace("ú", "u")
    return text

def removeSpecialCharacters(text):
    return re.sub("[^a-zñ ]", "", text)

def removeExtraSpaces(text):
    return re.sub("[ \t]+", " ", text)

def removeUnicode(text):
    return (text.encode('ascii', 'ignore')).decode("utf-8")

def clear(text): 
    text = text.lower()
    text = removeReferences(text)   
    text = removeAccents(text)
    text = removeSpecialCharacters(text)
    text = removeExtraSpaces(text)
    return text

def simplify(text):
    text_clear = clear(text)
    words = text_clear.split(" ")

    simple_text = []
    for word in words:
        if not isStopword(word):
            lemma = getLemma(word)
            simple_text.append(lemma)
      
    return " ".join(simple_text)[1:]

def minimize(text):
    text_clear = clear(text)
    words = text_clear.split(" ")

    simple_text = []
    for word in words:
        if not isStopword(word):
            #lemma = getLemma(word)
            steam = getSteamm(word)
            simple_text.append(steam)
        
    return " ".join(simple_text)[1:]


if __name__ == '__main__':
    text = "@lenin El Ecuador debe incentivar el uso de energas alternativas como parte de su compromiso con los ODS de @ONU_es  y el Acuerdo de Paris. La medida oportuna es el estmulo fiscal de los productos que las demanden y desestmulo a las energas fsiles. https:"
    simplified = simplify(text)
    minimized = minimize(text)
    print(simplified)
    print(minimized)