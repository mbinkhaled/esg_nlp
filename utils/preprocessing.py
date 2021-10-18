from tqdm import tqdm
from time import gmtime, strftime
import time
import unicodedata
import re
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
from utils.crawler import is_words, remove_punct
import os
import pickle
import requests
import bs4 as bs
from datetime import datetime
from collections import Counter

stop_words = set(stopwords.words('english'))
stop_words |= {'10-k', 'form', 'table', 'contents', 'united', 'states', 'securities', 'exchange', 'commission'}

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15 Version/13.0.4",
          "referer": "http://localhost:8888/",
          "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"}

headers_2 = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
          "referer": "http://localhost:8888/",
          "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"}

def preprocess(texts):
    """ 
    Tokenize texts, remove stopwords and numbers, and keep only the relevant words,
    then lemmatize the tokens
    """
    lemmatizer = WordNetLemmatizer()
    # ps = PorterStemmer()
#     for w in words:
#         rootWord=ps.stem(w)
    
    # tokens = [lemmatizer.lemmatize(token) for token in nltk.word_tokenize(texts) if token not in stop_words and len(token) < 18 and is_words(token)]
    tokens = [lemmatizer.lemmatize(token) for token in texts.split() if token not in stop_words and len(token) < 18 and is_words(token)]
    # tokens = [ps.stem(token) for token in nltk.word_tokenize(texts) if token not in stop_words and is_words(token)]
    
    return ' '.join(tokens)

def aggregate_cik_texts(cik, filetype):
    """
    Collect all the texts related to given `cik` with given filetype and 
    return a single string which concatenate all docs
    """
    cik_dir = os.path.join("data", filetype, cik)

    if not os.path.isdir(cik_dir):
        scrape_10k([cik])
        ConvertHTML(cik, os.path.join("data", "10k"))
        scrape_10q([cik])
        ConvertHTML(cik, os.path.join("data", "10q"))
    
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
        # import shutil
        # shutil.rmtree(pkl_path)
        # os.mkdir(pkl_path)

    rawtext_dir = os.path.join(cik_dir, "rawtext")
    # goes into the directory to find the path for txtfiles
    try:
        all_files = os.listdir(rawtext_dir)
    except:
        print("No such dir")
    
    texts = ""
    for file in all_files:
        if file[0] == '.' or int(file.split('_')[1].split('-')[0]) < 2020:
            continue
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

def scrape_10k(ciks):
    # Run the function to scrape 10-Ks

    # Define parameters
    browse_url_base_10k = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=10-K'
    filing_url_base_10k = 'http://www.sec.gov/Archives/edgar/data/%s/%s-index.html'
    doc_url_base_10k = 'http://www.sec.gov/Archives/edgar/data/%s/%s/%s'

    # Initialize log file
    # (log file name = the time we initiate scraping session)
    t = strftime("%Y_%m_%d_%H_%M_%S", gmtime())
    log_file_name = t + ".txt"
    log_file_path = os.path.join("log", log_file_name)

    with open(log_file_path, 'a') as log_file:
        log_file.close()

    # Iterate over CIKs and scrape 10-Ks
    for cik in tqdm(ciks):
        time.sleep(5)
        Scrape10K(browse_url_base=browse_url_base_10k, 
            filing_url_base=filing_url_base_10k, 
            doc_url_base=doc_url_base_10k, 
            cik=cik,
            log_file_name=log_file_path)
        

    #return to the main menu

# Run the function to scrape 10-Qs
def scrape_10q(ciks):
    # Define parameters
    browse_url_base_10q = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=10-Q&count=1000'
    filing_url_base_10q = 'http://www.sec.gov/Archives/edgar/data/%s/%s-index.html'
    doc_url_base_10q = 'http://www.sec.gov/Archives/edgar/data/%s/%s/%s'

    # Initialize log file
    # (log file name = the time we initiate scraping session)
    t = strftime("%Y_%m_%d_%H_%M_%S", gmtime())
    log_file_name = t + ".txt"
    log_file_path = os.path.join("log", log_file_name)

    with open(log_file_path, 'a') as log_file:
        log_file.close()

    # Iterate over CIKs and scrape 10-Qs
    for cik in tqdm(ciks):
        time.sleep(5)
        Scrape10Q(browse_url_base = browse_url_base_10q, 
            filing_url_base = filing_url_base_10q, 
            doc_url_base = doc_url_base_10q, 
            cik = cik,
            log_file_name = log_file_path)

def Scrape10K(browse_url_base, filing_url_base, doc_url_base, cik, log_file_name):
    
    '''
    Scrapes all 10-Ks and 10-K405s for a particular 
    CIK from EDGAR.
    
    Parameters
    ----------
    browse_url_base : str
        Base URL for browsing EDGAR.
    filing_url_base : str
        Base URL for filings listings on EDGAR.
    doc_url_base : str
        Base URL for one filing's document tables
        page on EDGAR.
    cik : str
        Central Index Key.
    log_file_name : str
        Name of the log file (should be a .txt file).
        
    Returns
    -------
    None.
    
    '''
    data_dir = os.path.join("data", "10k", cik)
    # Check if we've already scraped this CIK
    try:
        os.mkdir(data_dir)
    except OSError:
        print("Already scraped CIK", cik)
        return
    
    print('Scraping CIK', cik)
    
    # Request list of 10-K filings
    res = requests.get(browse_url_base % cik, headers=headers)
    # res = requests.get(browse_url_base % cik)
    
    # If the request failed, log the failure and exit
    if res.status_code != 200:
        os.rmdir(data_dir) # remove empty dir
        text = "Request failed with error code " + str(res.status_code) + \
               "\nFailed URL: " + (browse_url_base % cik) + '\n'
        print("main", text)
        WriteLogFile(log_file_name, text)
        return
    else:
        WriteLogFile(log_file_name, "Success URL: {}\n".format(browse_url_base % cik))

    # If the request doesn't fail, continue...
    
    # Parse the response HTML using BeautifulSoup
    soup = bs.BeautifulSoup(res.text, "lxml")

    # Extract all tables from the response
    html_tables = soup.find_all('table')
    
    # Check that the table we're looking for exists
    # If it doesn't, exit
    if len(html_tables) < 3:
        return
    
    # Parse the Filings table
    filings_table = pd.read_html(str(html_tables[2]), header=0)[0]
    filings_table['Filings'] = [str(x) for x in filings_table['Filings']]

    # Get only 10-K and 10-K405 document filings
    filings_table = filings_table[(filings_table['Filings'] == '10-K') | (filings_table['Filings'] == '10-K405')]

    # If filings table doesn't have any
    # 10-Ks or 10-K405s, exit
    if len(filings_table) == 0:
        return
    
    # Get accession number for each 10-K and 10-K405 filing
    filings_table['Acc_No'] = [x.replace('\xa0',' ')
                               .split('Acc-no: ')[1]
                               .split(' ')[0] for x in filings_table['Description']]

    # Iterate through each filing and 
    # scrape the corresponding document...
    for index, row in filings_table.iterrows():
        if row["Filing Date"] < "2020-01-01":
            continue
        
        # Get the accession number for the filing
        acc_no = str(row['Acc_No'])
        
        # Navigate to the page for the filing
        docs_page = requests.get(filing_url_base % (cik, acc_no), headers=headers_2)
        # docs_page = requests.get(filing_url_base % (cik, acc_no))

        # If request fails, log the failure
        # and skip to the next filing
        if docs_page.status_code != 200:
            text = "Request failed with error code " + str(docs_page.status_code) + \
                   "\nFailed URL: " + (filing_url_base % (cik, acc_no)) + '\n'
            WriteLogFile(log_file_name, text)
            continue
        else:
            WriteLogFile(log_file_name, "Success URL: {}\n".format(filing_url_base % (cik, acc_no)))

        # If request succeeds, keep going...
        
        # Parse the table of documents for the filing
        docs_page_soup = bs.BeautifulSoup(docs_page.text, 'lxml')
        docs_html_tables = docs_page_soup.find_all('table')
        if len(docs_html_tables)==0:
            continue
        docs_table = pd.read_html(str(docs_html_tables[0]), header=0)[0]
        docs_table['Type'] = [str(x) for x in docs_table['Type']]
        
        # Get the 10-K and 10-K405 entries for the filing
        docs_table = docs_table[(docs_table['Type'] == '10-K') | (docs_table['Type'] == '10-K405')]
        
        # If there aren't any 10-K or 10-K405 entries,
        # skip to the next filing
        if len(docs_table)==0:
            continue
        # If there are 10-K or 10-K405 entries,
        # grab the first document
        elif len(docs_table)>0:
            docs_table = docs_table.iloc[0]
        
        docname = docs_table['Document']
        
        # If that first entry is unavailable,
        # log the failure and exit
        if str(docname) == 'nan':
            text = 'File with CIK: %s and Acc_No: %s is unavailable' % (cik, acc_no) + '\n'
            WriteLogFile(log_file_name, text)
            continue       
        
        # If it is available, continue...
        docname = docname.split(' ')[0]
        
        # Request the file
        file = requests.get(doc_url_base % (cik, acc_no.replace('-', ''), docname), headers=headers)
        # file = requests.get(doc_url_base % (cik, acc_no.replace('-', ''), docname))
        
        # If the request fails, log the failure and exit
        if file.status_code != 200:
            text = "Request failed with error code " + str(file.status_code) + \
                   "\nFailed URL: " + (doc_url_base % (cik, acc_no.replace('-', ''), docname)) + '\n'
            WriteLogFile(log_file_name, text)
            continue
        else:
            WriteLogFile(log_file_name, "Success URL: {}\n".format(doc_url_base % (cik, acc_no.replace('-', ''), docname)))
        
        # If it succeeds, keep going...
        
        # Save the file in appropriate format
        """
        date = str(row['Filing Date'])
        filename = cik + '_' + date + '.txt'
        html_file = open(filename, 'a')
        html_file.write(file.text)
        html_file.close()
        """
        if '.txt' in docname:
            # Save text as TXT
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.txt'
            file_path = os.path.join("data", "10k", cik, filename)
            html_file = open(file_path, 'a')
            html_file.write(file.text)
            html_file.close()
        else:
            # Save text as HTML
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.html'
            file_path = os.path.join("data", "10k", cik, filename)
            html_file = open(file_path, 'a')
            html_file.write(file.text)
            html_file.close()
        
#     # Move back to the main 10-K directory
#     os.chdir('..')
        
    return

def Scrape10Q(browse_url_base, filing_url_base, doc_url_base, cik, log_file_name):
    
    '''
    Scrapes all 10-Qs for a particular CIK from EDGAR.
    
    Parameters
    ----------
    browse_url_base : str
        Base URL for browsing EDGAR.
    filing_url_base : str
        Base URL for filings listings on EDGAR.
    doc_url_base : str
        Base URL for one filing's document tables
        page on EDGAR.
    cik : str
        Central Index Key.
    log_file_name : str
        Name of the log file (should be a .txt file).
        
    Returns
    -------
    None.
    
    '''
    
    # Check if we've already scraped this CIK
    data_dir = os.path.join("data", "10q", cik)
    try:
        os.mkdir(data_dir)
    except OSError:
        print("Already scraped CIK", cik)
        return
    
    # If we haven't, go into the directory for that CIK
    
    #I avoid print here
    
    print('Scraping CIK', cik)
    
    # Request list of 10-Q filings
    res = requests.get(browse_url_base % cik, headers=headers)
    # res = requests.get(browse_url_base % cik)
    
    # If the request failed, log the failure and exit
    if res.status_code != 200:
        os.rmdir(data_dir) # remove empty dir
        text = "Request failed with error code " + str(res.status_code) + \
               "\nFailed URL: " + (browse_url_base % cik) + '\n'
        print("main", text)
        WriteLogFile(log_file_name, text)
        return
    
    # If the request doesn't fail, continue...

    # Parse the response HTML using BeautifulSoup
    soup = bs.BeautifulSoup(res.text, "lxml")

    # Extract all tables from the response
    html_tables = soup.find_all('table')
    
    # Check that the table we're looking for exists
    # If it doesn't, exit
    if len(html_tables)<3:
        print("table too short")
        return
    
    # Parse the Filings table
    filings_table = pd.read_html(str(html_tables[2]), header=0)[0]
    filings_table['Filings'] = [str(x) for x in filings_table['Filings']]

    # Get only 10-Q document filings
    filings_table = filings_table[filings_table['Filings'] == '10-Q']

    # If filings table doesn't have any
    # 10-Ks or 10-K405s, exit
    if len(filings_table)==0:
        return
    
    # Get accession number for each 10-K and 10-K405 filing
    filings_table['Acc_No'] = [x.replace('\xa0',' ')
                               .split('Acc-no: ')[1]
                               .split(' ')[0] for x in filings_table['Description']]

    # Iterate through each filing and 
    # scrape the corresponding document...
    for index, row in filings_table.iterrows():
        if row["Filing Date"] < "2020-01-01":
            continue
        
        # Get the accession number for the filing
        acc_no = str(row['Acc_No'])
        
        # Navigate to the page for the filing
        docs_page = requests.get(filing_url_base % (cik, acc_no), headers=headers_2)
        # docs_page = requests.get(filing_url_base % (cik, acc_no))
        
        # If request fails, log the failure
        # and skip to the next filing    
        if docs_page.status_code != 200:
            text = "Request failed with error code " + str(docs_page.status_code) + \
                   "\nFailed URL: " + (filing_url_base % (cik, acc_no)) + '\n'
            WriteLogFile(log_file_name, text)
            continue
        
        # If request succeeds, keep going...
        
        # Parse the table of documents for the filing
        docs_page_soup = bs.BeautifulSoup(docs_page.text, 'lxml')
        docs_html_tables = docs_page_soup.find_all('table')
        if len(docs_html_tables)==0:
            continue
        docs_table = pd.read_html(str(docs_html_tables[0]), header=0)[0]
        docs_table['Type'] = [str(x) for x in docs_table['Type']]
        
        # Get the 10-K and 10-K405 entries for the filing
        docs_table = docs_table[docs_table['Type'] == '10-Q']
        
        # If there aren't any 10-K or 10-K405 entries,
        # skip to the next filing
        if len(docs_table)==0:
            continue
        # If there are 10-K or 10-K405 entries,
        # grab the first document
        elif len(docs_table)>0:
            docs_table = docs_table.iloc[0]
        
        docname = docs_table['Document']
        
        # If that first entry is unavailable,
        # log the failure and exit
        if str(docname) == 'nan':
            text = 'File with CIK: %s and Acc_No: %s is unavailable' % (cik, acc_no) + '\n'
            WriteLogFile(log_file_name, text)
            continue       
        
        # If it is available, continue...
        docname = docname.split(' ')[0]
        # Request the file
        
        file = requests.get(doc_url_base % (cik, acc_no.replace('-', ''), docname), headers=headers)
        # file = requests.get(doc_url_base % (cik, acc_no.replace('-', ''), docname))
        
        # If the request fails, log the failure and exit
        if file.status_code != 200:
            text = "Request failed with error code " + str(file.status_code) + \
                   "\nFailed URL: " + (doc_url_base % (cik, acc_no.replace('-', ''), docname)) + '\n'
            WriteLogFile(log_file_name, text)
            continue
        
        # If it succeeds, keep going...
        
        # Save the file in appropriate format
        """
        date = str(row['Filing Date'])
        filename = cik + '_' + date + '.txt'
        html_file = open(filename, 'a')
        html_file.write(file.text)
        html_file.close()
        """
        if '.txt' in docname:
            # Save text as TXT
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.txt'
            file_path = os.path.join("data", "10q", cik, filename)
            html_file = open(file_path, 'a')
            html_file.write(file.text)
            html_file.close()
        else:
            # Save text as HTML
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.html'
            file_path = os.path.join("data", "10q", cik, filename)
            html_file = open(file_path, 'a')
            html_file.write(file.text)
            html_file.close()
        
    # Move back to the main 10-Q directory
        
    return

def WriteLogFile(log_file_name, text):
    '''
    Helper function.
    Writes a log file with all notes and
    error messages from a scraping "session".
    
    Parameters
    ----------
    log_file_name : str
        Name of the log file (should be a .txt file).
    text : str
        Text to write to the log file.
        
    Returns
    -------
    None.
    
    '''
    with open(log_file_name, "a+") as log_file:
        log_file.write(text)

    return

def RemoveNumericalTables(soup):
    
    '''
    Removes tables with >15% numerical characters.
    
    Parameters
    ----------
    soup : BeautifulSoup object
        Parsed result from BeautifulSoup.
        
    Returns
    -------
    soup : BeautifulSoup object
        Parsed result from BeautifulSoup
        with numerical tables removed.
        
    '''
    
    # Determines percentage of numerical characters
    # in a table
    def GetDigitPercentage(tablestring):
        if len(tablestring) > 0.0:
            numbers = sum([char.isdigit() for char in tablestring])
            length = len(tablestring)
            return numbers/length
        else:
            return 1
    
    # Evaluates numerical character % for each table
    # and removes the table if the percentage is > 15%
    [x.extract() for x in soup.find_all('table') if GetDigitPercentage(x.get_text())>0.15]
    
    return soup

def RemoveTags(soup):
    
    '''
    Drops HTML tags, newlines and unicode text from
    filing text.
    
    Parameters
    ----------
    soup : BeautifulSoup object
        Parsed result from BeautifulSoup.
        
    Returns
    -------
    text : str
        Filing text.
        
    '''
    
    # Remove HTML tags with get_text
    text = soup.get_text()
    
    # Remove newline characters
    text = text.replace('\n', ' ')
    
    '''
    Adding a replace to convert all excecute space to one space 
    '''
    text = re.sub(r'\s', ' ', text)
    
    # Replace unicode characters with their
    # "normal" representations
    text = unicodedata.normalize('NFKD', text)
    
    return text

def ConvertHTML(cik, dirpath):
    
    '''
    Removes numerical tables, HTML tags,
    newlines, unicode text, and XBRL tables.
    
    Parameters
    ----------
    cik : str
        Central Index Key used to scrape files.
    
    Returns
    -------
    None.
    
    '''
    dir_path = os.path.join(dirpath, cik)
    
    # Look for files scraped for that CIK
    try: 
        os.path.exists(dir_path)
    # ...if we didn't scrape any files for that CIK, exit
    except FileNotFoundError:
        print("Could not find directory for CIK", cik)
        return
    """ avoid print"""    
    # print("Parsing CIK %s..." % cik)
    
    parsed = False # flag to tell if we've parsed anything
    
    # Try to make a new directory within the CIK directory
    # to store the text representations of the filings
    raw_txt_dir = os.path.join(dir_path, "rawtext")
    try:
        os.mkdir(raw_txt_dir)
    # If it already exists, continue
    # We can't exit at this point because we might be
    # partially through parsing text files, so we need to continue
    except OSError:
        pass
    
    # Get list of scraped files
    # excluding hidden files and directories
    file_list = [fname for fname in os.listdir(dir_path) if not (fname.startswith('.') | os.path.isdir(os.path.join(dir_path, fname)))]
    
    # Iterate over scraped files and clean
    for filename in file_list:
            
        # Check if file has already been cleaned
        new_filename = filename.replace('.html', '.txt')
        text_file_list = os.listdir(raw_txt_dir)
        if new_filename in text_file_list:
            continue
        
        # If it hasn't been cleaned already, keep going...
        
        # Clean file
        with open(os.path.join(dir_path, filename), 'r', encoding='utf-8') as file:
            parsed = True
            soup = bs.BeautifulSoup(file.read(), "lxml")
            soup = RemoveNumericalTables(soup)
            
            #add my delete reference function here
            #soup = delete_reference(soup)
            
            text = RemoveTags(soup)
            with open(os.path.join(raw_txt_dir, new_filename), 'w',encoding='utf-8') as newfile:
                newfile.write(text)
    
    # If all files in the CIK directory have been parsed
    # then log that
    if parsed==False:
        print("Already parsed CIK", cik)
    
    return

'''
In the documents, there are some parts not in the Management Discussion but contain reference to this part,like:
'For a discussion of our contractual obligations, see “Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations,”'' 

That is why I need a function to delete all the reference parts of MD&A.

'''

#compose a function to delete the references
def delete_reference(soup):
    
    #define string reference to the management's discussion parts
    management_str = "Financial Condition and Results of Operations"
                    
    #define string reference to the financial statement parts, which is what we need for parsing
    financial_str = "and Supplementary Data"
    
    #given the none reference length for comparision
    length_1 = len('Management’s Discussion and Analysis of Financial Condition and Results of Operations')
    
    #given the none reference length for comparision
    length_2 = len('Financial Statements and Supplementary Data')
    
    #find all the parts contains this reference
    for part in soup.find_all(text = re.compile(management_str)):
        
        #if the lenth is much greater than the original string length, then it's a reference, and we should delete it
        if len(part) > length_1:
            
            #we should extract these text
            part.extract()
            
    #the same should be operated to the financial statements
    for part in soup.find_all(text = re.compile(financial_str)):
        if len(part) > length_2:
            part.extract()
    
    #return soup for futher parsing
    return soup
