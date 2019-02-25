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
                                        }"""
                                                ],
              'Person Employee or Member of':["""SELECT ?item ?itemLabel WHERE {
                                              wd:%s wdt:P108 ?item.
                                              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                                            }"""
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




from SPARQLWrapper import SPARQLWrapper, JSON   
from rosette.api import API, DocumentParameters, RosetteException
import pandas as pd
import wikipedia
import requests
import numpy as np
import pickle
import random

id_dict = {}

def get_id(message, dict_to_use=None):
	if dict_to_use:
		dict_to_use = dict_to_use
	else:
		global id_dict
		dict_to_use = id_dict

	if message in dict_to_use:
		return dict_to_use[message]
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
	        dict_to_use[message] = r.json()['search'][0]['id']
	        return dict_to_use[message]
	    except Exception:
	        return -1 #The id doesn't exist



def get_results(query, value, endpoint_url="https://query.wikidata.org/sparql"):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(query%value)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


def ground_truth(relation, subject, debug=False):
    results = []
    gt = []
    try:
        results = [get_results(query, get_id(subject)) for query in QUERY_DICT[relation]]
        for result in results:
            for r in result["results"]["bindings"]:
                gt.append(r['itemLabel']['value'])
    except:
    	if debug:
        	print (relation, subject)
    return gt

def add_ground_truth(df, debug=False):
	if debug:
		print (df)
	load_dict()
	df = df.reset_index()
	df['Pseudo Ground Truth'] = df.apply(lambda row: ground_truth(row['Relationship'], row['Subject']), axis=1)
	df['Count_PGT'] = df['Pseudo Ground Truth'].apply(lambda x: len(x))
	df = df.set_index(['Subject','Relationship'])
	save_dict()
	return df

def add_recall_score(df):
    df['Recall Prediction'] = np.random.randint(0, 100, df.shape[0])/100
    return df


def load_dict():
	global id_dict
	try:
		with open('id_dict.pkl', 'rb') as fp:
			id_dict = pickle.load(fp)
	except:
		print ("Creating a new Dictionary")
		id_dict = {}


def save_dict():
	with open('id_dict.pkl', 'wb') as fp:
		pickle.dump(id_dict, fp, protocol=pickle.HIGHEST_PROTOCOL)


def get_dict():
	di = {}
	with open('id_dict.pkl', 'rb') as fp:
		di = pickle.load(fp)
	return di


def Analyse(message, alt_url='https://api.rosette.com/rest/v1/'):
    """ Run the example """
    # Create an API instance
    api = API(user_key="89350904c7392a44f0f9019563be727a", service_url=alt_url)

    # Set selected API options.
    # For more information on the functionality of these
    # and other available options, see Rosette Features & Functions
    # https://developer.rosette.com/features-and-functions#morphological-analysis-introduction

    # api.set_option('modelType','perceptron') #Valid for Chinese and Japanese only
    
    # Opening the ID Dictionary
    load_dict()
    ### Will Close after Analysis of the document is completed
    
    params = DocumentParameters()
    relationships_text_data = wikipedia.page(message).content[:20000]
    params["content"] = relationships_text_data
    rel = []
    message_id = get_id(message)
    message_split = message.split(" ")
    try:
        RESULT = api.relationships(params)
        #print(RESULT)
        for r in RESULT['relationships']:
            arg2_split = r['arg2'].split(" ")
            confidence = '?'
            if "confidence" in r:
                confidence = str(round(r["confidence"],2))
            if any(s in arg2_split for s in message_split):
                if get_id(r['arg2']) == message_id:
                    rel.append({'Relationship':r['predicate']+'^-1', 'Subject':r['arg2'], 'Object':r['arg1'], 'Confidence': confidence})
            rel.append({'Relationship':r['predicate'],'Subject':r['arg1'],'Object':r['arg2'], 'Confidence': confidence})
            
        ## Closing the ID Dict
        save_dict()
        ##
        return rel, message_id
    except RosetteException as exception:
        print(exception)
    


