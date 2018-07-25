import re
import dataloader
import numpy as np
from nltk.stem.snowball import SnowballStemmer

# Consulta si la la palabra ingresada como parametro esta en la lista de stopwords
def isStopword(word):
    stopwords = dataloader.stopwords()["stopword"].values
    return word in stopwords

# Obtiene la palabra raiz de la parabra ingresada como parametro
def getStem(word):
    stemmer = SnowballStemmer("spanish")
    return stemmer.stem(word)

# Obtiene el lemma de la parabra ingresada como parametro
def getLemma(word):
    lemmas = dataloader.lemmas()
    result = lemmas.loc[lemmas["variation"] == word]
    if result.empty:
        return word
    else:
        return result["main"].values[0]

# Elimina las referencias a usuarios, hastag y enlaces en el texto ingresado como parametro
def removeReferences(text):
    words = text.split(" ")
    new_text = []
    # Quitar hastag, usuarios, enlaces
    for word in words:
        if (word.find("@") < 0) and (word.find("#") < 0) and (word.find("http") < 0) and (word.find("https") < 0):
            new_text.append(word)
    return " ".join(new_text)

# Elimina los acentos en el texto ingresado como parametro
def removeAccents(text):
    text = text.replace("á", "a")
    text = text.replace("é", "e")
    text = text.replace("í", "i")
    text = text.replace("ó", "o")
    text = text.replace("ú", "u")
    return text

# Elimina los caracteres especiales en el texto ingresado como parametro
def removeSpecialCharacters(text):
    return re.sub("[^a-zñ ]", "", text)

# Elimina los espacios extra y las tabulaciones en el texto ingresado como parametro
def removeExtraSpaces(text):
    return re.sub("[ \t]+", " ", text)

# Elimina los caracteres unicode presentes en el texto ingresado como parametro
def removeUnicode(text):
    return (text.encode('ascii', 'ignore')).decode("utf-8")

# Limpia un texto con los metodos anteriores
def clear(text): 
    text = text.lower()    
    text = removeReferences(text)   
    text = removeAccents(text)
    text = removeSpecialCharacters(text)
    text = removeExtraSpaces(text)
    return text

# Simplifica un texto eliminando las stopwords y lematizando
def simplify(text):
    text_clear = clear(text)
    words = text_clear.split(" ")

    simple_text = []
    for word in words:
        if not isStopword(word):
            lemma = getLemma(word)
            simple_text.append(lemma)
      
    return " ".join(simple_text)

# Simplifica un texto eliminando las stopwords y onteniendo sus palabra raiz
def minimize(text):
    text_clear = clear(text)
    words = text_clear.split(" ")

    simple_text = []
    for word in words:
        if not isStopword(word):
            #lemma = getLemma(word)
            stem = getStem(word)
            simple_text.append(stem)
        
    return " ".join(simple_text)


if __name__ == '__main__':
    text = "@lenin El Ecuador debe incentivar el uso de energas alternativas como parte de su compromiso con los ODS de @ONU_es  y el Acuerdo de Paris. La medida oportuna es el estmulo fiscal de los productos que las demanden y desestmulo a las energas fsiles. https:"
    simplified = simplify(text)
    minimized = minimize(text)

    print("original:", text)
    print("simplificado:", simplified)
    print("minimizado:", minimized)