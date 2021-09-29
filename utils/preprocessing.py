from tqdm import tqdm
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
from utils.crawler import is_words, remove_punct
import os
import pickle
from datetime import datetime
from collections import Counter

stop_words = set(stopwords.words('english'))
stop_words |= {'10-k', 'form', 'table', 'contents', 'united', 'states', 'securities', 'exchange', 'commission'}

def preprocess(texts):
    """ 
    Tokenize texts, remove stopwords and numbers, and keep only the relevant words,
    then lemmatize the tokens
    """
    lemmatizer = WordNetLemmatizer()
    # ps = PorterStemmer()
#     for w in words:
#         rootWord=ps.stem(w)
    
    tokens = [lemmatizer.lemmatize(token) for token in nltk.word_tokenize(texts) if token not in stop_words and is_words(token)]
    # tokens = [ps.stem(token) for token in nltk.word_tokenize(texts) if token not in stop_words and is_words(token)]
    
    return ' '.join(tokens)

def aggregate_cik_texts(cik, filetype):
    """
    Collect all the texts related to given `cik` with given filetype and 
    return a single string which concatenate all docs
    """
    cik_dir = os.path.join("data", filetype, cik)
    pkl_path = os.path.join(cik_dir, "pickle")

    if not os.path.isdir(pkl_path):
        os.mkdir(pkl_path)
    else:
        # If already processed before, directly read the cache and return
        with open(os.path.join(pkl_path, 'agg_texts.pkl'), 'rb') as f:
            texts = pickle.load(f)
        with open(os.path.join(pkl_path, 'token_counter.pkl'), 'rb') as f:
            counter = pickle.load(f)
        return {"texts": texts, "counter": counter}

    rawtext_dir = os.path.join(cik_dir, "rawtext")
    # goes into the directory to find the path for txtfiles
    try:
        all_files = os.listdir(rawtext_dir)
    except:
        print("No such dir")
    
    texts = ""
    for file in all_files:
        with open(os.path.join("data", filetype, cik, "rawtext", file), encoding = "utf8") as f:
            string_temp = f.read().lower()
            texts += preprocess(string_temp)
    
    texts = remove_punct(texts)
    counter = texts2counter(texts)

    with open(os.path.join(pkl_path, 'agg_texts.pkl'), 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        # print(os.path.join(pkl_path, 'agg_texts.pkl'))
        pickle.dump(texts, f, pickle.HIGHEST_PROTOCOL)
    
    with open(os.path.join(pkl_path, 'token_counter.pkl'), 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        # print(os.path.join(pkl_path, 'token_counter.pkl'))
        pickle.dump(counter, f, pickle.HIGHEST_PROTOCOL)

    return {"texts": texts, "counter": counter}

def texts2counter(texts):
    tokens = texts.split(' ')
    counter = Counter(tokens)
    
    return counter

def get_texts(cik_list, ticker_list):
    # './data/10k/[cik]/rawtext/[cik]_[date]'
    docs = []
    tickers = []
    counters = dict()   # {ticker: counter}

    for cik, ticker in tqdm(zip(cik_list, ticker_list)):
        tickers.append(ticker)
        texts = ""
        for filetype in ["10k", "10q"]:
            dict_ret = aggregate_cik_texts(cik, filetype)
            texts += dict_ret["texts"]
        
        counter = texts2counter(texts)
        counters[ticker] = counter

        docs.append(texts)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%m-%d-%H_%M_%S")
    cache_path = os.path.join("data", date_time)

    if not os.path.exists(cache_path):
        os.mkdir(cache_path)
    
    with open(os.path.join(cache_path, 'agg_counters.pkl'), 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(counters, f, pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(cache_path, 'agg_texts.pkl'), 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(docs, f, pickle.HIGHEST_PROTOCOL)
    
    return {"docs": docs, "tickers": tickers, "counters": counters}
