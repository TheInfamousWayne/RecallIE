QUERY_DICT = {'Organization Founded By^-1':["""SELECT ?item ?itemLabel WHERE {
                                          ?item wdt:P112 wd:%s.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }"""
                                           ],
              'Organization Founded By':["""SELECT ?item ?itemLabel WHERE {
                                          wd:%s wdt:P112 ?item.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }"""
                                        ],
              'Organization Headquarters':["""SELECT ?item ?itemLabel WHERE {
                                          wd:%s wdt:P159 ?item.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }"""
                                          ],
              'Organization Subsidiary Of^-1':["""SELECT ?item ?itemLabel WHERE {
                                          wd:%s wdt:P355 ?item.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }"""
                                              ],
              'Organization Subsidiary Of':["""SELECT ?item ?itemLabel WHERE {
                                          ?item wdt:P355 wd:%s.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }"""
                                           ],
              'Organization top employees':["""SELECT ?item ?itemLabel WHERE {
                                          wd:%s wdt:P169 ?item.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }""", # CEO
                                            """SELECT ?item ?itemLabel WHERE {
                                          wd:%s wdt:P488 ?item.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }""" # Chairperson
                                            ],
              'Person Employee or Member of^-1':["""SELECT ?item ?itemLabel WHERE {
                                          ?item wdt:P108 wd:%s.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }""",
                                            """SELECT ?item ?itemLabel WHERE {
                                          wd:%s wdt:P527 ?item.
                                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                        }""" 
                                                ],
              'Person Employee or Member of':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P108 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }""",
                                              """SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P463 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""## member of ---> Band Members
                                            ],
              'Person Place of Birth':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P19 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
                                      ],
              'Person Current and Past Location of Residence':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P551 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
                                                              ],
              'Person Parents':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P22 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }""", #Father
                                """SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P25 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }""", #Mother
                                """SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P1038 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }""" #Relative (Adopted Parents?)
                                # Shall we include stepparents??
                               ],
              'Person Parents^-1':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P40 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
                                  ],
              'Person Siblings':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P3373 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
                                ],
              'Person Spouse':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P26 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
                              ],
              'Citizen of':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P27 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
                           ],
              'Educated at':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P69 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
                            ]
             }



PERSON_RELATIONS = ['Educated at', 'Citizen of', 'Person Employee or Member of', 'Organization top employees^-1',\
                    'Person Current and Past Location of Residence', 'Person Parents', 'Person Parents^-1',\
                    'Person Place of Birth', 'Person Siblings', 'Person Spouse']
ORG_RELATION =     ['Organization Founded By', 'Organization Collaboration', 'Organization Collaboration^-1',\
                    'Organization Headquarters', 'Organization Subsidiary Of', 'Organization Subsidiary Of^-1',\
                    'Organization top employees', 'Person Employee or Member of^-1', 'Organization Acquired By^-1',\
                    'Organization Acquired By', 'Organization Provider To', 'Organization Provider To^-1']
COMMON_RELATION =  ['Organization Founded By^-1']




from SPARQLWrapper import SPARQLWrapper, JSON   
from rosette.api import API, DocumentParameters, RosetteException
import pandas as pd
import wikipedia
import requests
import numpy as np
import pickle
import random
from threading import Lock
import os, sys
import threading
from threading import Thread
import queue
from bs4 import BeautifulSoup
from random import randint
from lxml import html
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def count_confidence(main_df):
	    if (not main_df.empty):
	        main_df = main_df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x))
	        main_df['Object'] = main_df['Object'].agg(lambda x: x if x != [''] else [])
	        main_df['Count'] = main_df['Object'].apply(lambda x: len(x))
	        #main_df = main_df[[c for c in main_df if c not in ['Confidence']] + ['Confidence']]
	    return main_df

def load_useful100(query):
    path = 'data/dumps/{}_useful100.pkl'.format(query)
    try:
        with open(path, 'rb') as fp:
            useful100 = pickle.load(fp)
    except:
        print ("No file exists. Creating a new one.")
        useful100 = {}
    return useful100


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

def get_news(link):
    ### Setting up API ###
    options = Options()
    options.binary_location = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    options.add_argument('--headless')
    options.add_argument('--window-size=1200x600')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')

    chromedriver = 'C:\\Users\\Bhavya\\Desktop\\Vaibhav\\chromedriver.exe'
    r = webdriver.Chrome(executable_path=chromedriver, options=options)

    ### Scraping From URL ###
    r.get(link)
    doc_summary = []
    soup = BeautifulSoup(r.page_source, 'lxml')
    all_paragraphs = soup.findAll("p")
    for paragraph in all_paragraphs:
        doc_summary.append(paragraph.text)
    doc_summary = " ".join(doc_summary)
    doc_summary = " ".join(doc_summary.split())
    return doc_summary

class Utils:
    
    def __init__(self):
        self.id_dict = {}
        self.lock = Lock()
        self.load_dict()
        
    
    def __del__(self):
        self.save_dict()

    def get_id(self, message, dict_to_use=None):
#         if dict_to_use:
#             dict_to_use = dict_to_use
#         else:
#             global id_dict
#             dict_to_use = id_dict
    
        if message in self.id_dict:
            return self.id_dict[message]
        else:
            API_ENDPOINT = "https://www.wikidata.org/w/api.php"
            query = message
            params = {
                'action': 'wbsearchentities',
                'format': 'json',
                'language': 'en',
                'search': query
            }
            r = requests.get(API_ENDPOINT, params = params)
            try:
                with self.lock:
                    self.id_dict[message] = r.json()['search'][0]['id']
                return self.id_dict[message]
            except Exception:
                return -1 #The id doesn't exist


    def id_to_name(self, eid):
#         if dict_to_use:
#             dict_to_use = dict_to_use
#         else:
#             global id_dict
#             dict_to_use = id_dict

        if eid in self.id_dict.values():
            return [key for key, value in self.id_dict.items() if value == eid][0]
        else:
            API_ENDPOINT = "https://www.wikidata.org/w/api.php"
            query = eid
            params = {
                'action': 'wbsearchentities',
                'format': 'json',
                'language': 'en',
                'search': query
            }
            r = requests.get(API_ENDPOINT, params = params)
            try:
                with self.lock:
                    self.id_dict[ r.json()['search'][0]['label'] ] = r.json()['search'][0]['id']
                return r.json()['search'][0]['label']
            except Exception:
                return -1 #The id doesn't exist


    def get_results(self, query, value, endpoint_url="https://query.wikidata.org/sparql"):
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setQuery(query%value)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()


    def ground_truth(self, relation, subject, debug=False):
        global QUERY_DICT
        results = []
        gt = []
        try:
            results = [self.get_results(query, self.get_id(subject)) for query in QUERY_DICT[relation]]
            for result in results:
                for r in result["results"]["bindings"]:
                    gt.append(r['itemLabel']['value'])
        except:
            if debug:
                print (relation, subject)
        return gt

    def add_ground_truth(self, df, debug=False):
        if df.empty:
            return df
        if debug:
            print (df)
        df = df.reset_index()
        df['Pseudo Ground Truth'] = df.apply(lambda row: self.ground_truth(row['Relationship'], row['Subject']), axis=1)
        df['Count_PGT'] = df['Pseudo Ground Truth'].apply(lambda x: len(x))
        df = df.set_index(['Subject','Relationship'])
        return df

    def add_recall_score(self, df):
        df['Recall Prediction'] = np.random.randint(0, 100, df.shape[0])/100
        return df


    def load_dict(self):
        try:
            with open('data/dumps/id_dict.pkl', 'rb') as fp:
                self.id_dict = pickle.load(fp)
        except:
            print ("Creating a new Dictionary")
            self.id_dict = {}


    def save_dict(self):
        with self.lock:
            old_dict = self.get_dict()
            self.id_dict = {**self.id_dict, **old_dict}
            with open('data/dumps/id_dict.pkl', 'wb') as fp:
                pickle.dump(self.id_dict, fp, protocol=pickle.HIGHEST_PROTOCOL)
                print("Saved")


    def get_dict(self):
        di = {}
        with open('data/dumps/id_dict.pkl', 'rb') as fp:
            di = pickle.load(fp)
        return di


    def Analyse(self, message, doc=None, lock=None, alt_url='https://api.rosette.com/rest/v1/'):
        """ Run the example """
        # Create an API instance
        api = API(user_key="969b3593686184bb42803d8da453f119", service_url=alt_url)

        # Set selected API options.
        # For more information on the functionality of these
        # and other available options, see Rosette Features & Functions
        # https://developer.rosette.com/features-and-functions#morphological-analysis-introduction

        # api.set_option('modelType','perceptron') #Valid for Chinese and Japanese only

        # Opening the ID Dictionary
#         load_dict()
        ### Will Close after Analysis of the document is completed
    
        if lock == None:
            lock = Lock()

        params = DocumentParameters()
        if doc:
            relationships_text_data = doc[:20000]
        else:
            relationships_text_data = wikipedia.page(message).content[:20000]
        params["content"] = relationships_text_data
        rel = []
        message_id = self.get_id(message)
        message_split = message.split(" ")
        try:
            with lock:
                RESULT = api.relationships(params)
            
            for r in RESULT['relationships']:
                arg2_split = r['arg2'].split(" ")
                confidence = '?'
                if "confidence" in r:
                    confidence = str(round(r["confidence"],2))
                if any(s in arg2_split for s in message_split):
                    if self.get_id(r['arg2']) == message_id:
                        rel.append({'Relationship':r['predicate']+'^-1', 'Subject':r['arg2'], 'Object':r['arg1'], 'Confidence': confidence})
                rel.append({'Relationship':r['predicate'],'Subject':r['arg1'],'Object':r['arg2'], 'Confidence': confidence})

            ## Closing the ID Dict
            self.save_dict()
            ##
            return rel, message_id
        except RosetteException as exception:
            print(exception)



class News(Thread):
    def __init__(self, lock=None, link=None, name=None, shared_df=None, headline=None):
        Thread.__init__(self)
        self.df = pd.DataFrame()
        self.main_df = pd.DataFrame()
        self.u = Utils()
        self.headline = headline
        self.lock = lock
        self.link = link
        self.eid = self.u.get_id(name)
        self.doc = ""
        self.message = name
        self.start()
        
        
    def run(self):
        a = Thread(target = self.get_link_text, args = ())
        a.start()
        a.join()

        
    def get_link_text(self):
        r = requests.get(self.link)
        content = r.text
        doc_summary = []
        soup = BeautifulSoup(content, "html.parser")
        paras = soup.findAll("p")
        for p in paras:
            doc_summary.append(p.text)
        s = " ".join(doc_summary)
        s = " ".join(s.split())
        self.doc = s
        ## Analysing the link text
        res,_ = self.u.Analyse(message=self.message, doc=self.doc, lock=self.lock)
        self.df = pd.DataFrame(res, columns=['Subject','Relationship','Object','Confidence'])
        self.main_df = self.df[self.df['Subject'].apply(lambda row: self.u.get_id(row)) == self.eid]
        self.main_df['Subject'] = self.message
    
    def get_df(self):
        return self.df
    
    def get_main_df(self):
        return self.headline, self.link, self.main_df