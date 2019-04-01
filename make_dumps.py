
# coding: utf-8

# In[7]:


from bs4 import BeautifulSoup
import requests
import time
from random import randint
from lxml import html
import pandas as pd
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# In[2]:


from news.utils import *


# In[9]:



# In[10]:


def make_headline_dict(query):
    ### Getting Custom Date Range ###
    y1,m1,d1 = ('2014','01','01')
    y2,m2,d2 = str(date.today()).split('-')
    
    ### Setting up API ###
    headline_list = []
    headline_dict = {}

    options = Options()
    options.binary_location = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    options.add_argument('--headless')
    options.add_argument('--window-size=1200x600')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')

    chromedriver = 'C:\\Users\\Bhavya\\Desktop\\Vaibhav\\chromedriver.exe'
    r = webdriver.Chrome(executable_path=chromedriver, options=options)

    ### Scraping From Google ###
    final_date = date(int(y2),int(m2),int(d2))
    while True:
        dd = int(d1)
        mm = int(m1) 
        yy = int(y1) + mm//12
        mm = mm % 12 + 1    

        temp_date = date(yy,mm,dd)   
        if temp_date > final_date:
            break  

        r.get("https://www.google.com/search?q={}&hl=en-US&gl=US&source=lnt&tbs=cdr%3A1%2Ccd_min%3A{}%2F{}%2F{}%2Ccd_max%3A{}%2F{}%2F{}&tbm=nws".format(                            query,m1,d1,y1,mm,dd,yy))
        soup = BeautifulSoup(r.page_source, 'lxml')
        all_headlines = soup.findAll("h3")
        for headline in all_headlines:
            headline_list.append(headline.text)
            headline_dict[headline.text] = headline.a['href']        
        d1,m1,y1 = dd,mm,yy
    
    ### Saving the file ###
    save_headline_dict(headline_dict)

def save_headline_dict(headline_dict):
    path = 'data/dumps/{}_headline_dict.pkl'.format(query)
    with open(path, 'wb') as fp:
        pickle.dump(headline_dict, fp, protocol=pickle.HIGHEST_PROTOCOL)
        print("headline_dict saved!")
        
def load_headline_dict(query):
    path = 'data/dumps/{}_headline_dict.pkl'.format(query)
    headline_dict = {}
    try:
        with open(path, 'rb') as fp:
            headline_dict = pickle.load(fp)
    except:
        print ("Creating a new Dictionary")
        headline_dict = {}
    return headline_dict

def save_useful100(useful100):
    path = 'data/dumps/{}_useful100.pkl'.format(query)
    with open(path, 'wb') as fp:
        pickle.dump(useful100, fp, protocol=pickle.HIGHEST_PROTOCOL)
        print("100 useful headlines for query {} is saved!".format(query))
        
def load_useful100(query):
    path = 'data/dumps/{}_useful100.pkl'.format(query)
    try:
        with open(path, 'rb') as fp:
            useful100 = pickle.load(fp)
    except:
        print ("No file exists. Creating a new one for {}".format(query))
        useful100 = {}
    return useful100

PERSON_RELATIONS = ['Educated at', 'Citizen of', 'Person Employee or Member of', 'Organization top employees^-1',                    'Person Current and Past Location of Residence', 'Person Parents', 'Person Parents^-1',                    'Person Place of Birth', 'Person Siblings', 'Person Spouse']
ORG_RELATION =     ['Organization Founded By', 'Organization Collaboration', 'Organization Collaboration^-1',                    'Organization Headquarters', 'Organization Subsidiary Of', 'Organization Subsidiary Of^-1',                    'Organization top employees', 'Person Employee or Member of^-1', 'Organization Acquired By^-1',                    'Organization Acquired By', 'Organization Provider To', 'Organization Provider To^-1']
COMMON_RELATION =  ['Organization Founded By^-1']

def add_dummy(df, person=False):
    global isPerson
    rel = set(df['Relationship'])
    if person:
        isPerson = person
    else:
        isPerson = True if any(s in PERSON_RELATIONS for s in rel) else False
    if isPerson:
        dummy_rels = COMMON_RELATION + PERSON_RELATIONS
    else:
        dummy_rels = COMMON_RELATION + ORG_RELATION
    for r in rel:
        dummy_rels.remove(r)
    for r in dummy_rels:
        df = df.append({'Subject': query, 'Relationship':r, 'Object':''}, ignore_index=True)
    return df

def count_confidence(main_df):
    if (not main_df.empty):
        main_df = main_df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x))
        main_df['Object'] = main_df['Object'].agg(lambda x: x if x != [''] else [])
        main_df['Count'] = main_df['Object'].apply(lambda x: len(x))
        #main_df = main_df[[c for c in main_df if c not in ['Confidence']] + ['Confidence']]
    return main_df


# In[ ]:

if __name__ == '__main__':

    N = 100
    u = Utils()
    queries = ['Angela Merkel', 'Donald Trump', 'Ivanka', 'Bill Gates']#, 'Brad Pitt', 'Christopher Nolan', 'Christian Bale', 'Megan Fox', 'Steve Jobs', 'Hillary Clinton', 'Gerhard Weikum']

    for query in queries:
        
        # Scraping News Links & Headlines
        make_headline_dict(query)
        headline_dict = load_headline_dict(query)
        
        # Shuffling and taking atleast 100 articles with some extractions
        shuffled_list = random.sample(headline_dict.items(), len(headline_dict.items()))
        useful100 = load_useful100(query)
        lock = Lock()
        DFs = []
        start = 0

        ### Creating threads for each link
        while len(useful100.items()) < N :
            pass100 = dict(shuffled_list[start:start+N])
            threads = []
            for headline,link in pass100.items():
                threads.append( News(link=link, name=query, lock=lock, headline=headline) )

            ### Waiting for each thread to complete
            for t in threads:
                try:
                    t.join()
                except Exception as e:
                    pass

            ### Collecting the dfs
            for i,t in enumerate(threads):
                headline, link, df = t.get_main_df()
                if not df.empty:
                    DFs.append(df)
                    useful100[headline] = link

            start = min(start+N, len(headline_dict.items()))

        save_useful100(useful100)
        MERGED_DF = pd.concat(DFs, ignore_index=True)
        MERGED_DF.loc[MERGED_DF['Confidence'] == '?','Confidence'] = MERGED_DF['Confidence'].apply(lambda x: round(np.random.uniform(0.85,1),2))
        
        # Finding Web and Reality
        isPerson = False
        main_df = MERGED_DF.drop('Confidence',axis=1)
        main_df = main_df.groupby(['Subject','Relationship','Object']).size().to_frame('c').reset_index()
        main_df = pd.merge(main_df, MERGED_DF)
        main_df.Confidence = main_df.Confidence.astype(float).fillna(0.0)
        main_df['Object'] = main_df.apply(lambda main_df: main_df['Object']+':'+str(main_df['c']), axis=1) 
        main_df = main_df.drop('c',axis=1)
        main_df = add_dummy(main_df)
        main_df = count_confidence(main_df)

        ###### ADDING GROUND TRUTH ######
        main_df = u.add_ground_truth(main_df)

        ###### ADDING RECALL SCORE ######
        main_df = u.add_recall_score(main_df)

        main_df.to_pickle("data/dumps/web_reality-{}-{}.pkl".format(N,query))
        
        print("Done for {}".format(query))

