from django.shortcuts import render
from django.http import JsonResponse
import wikipedia
import pandas as pd
import numpy as np
from utils import Utils

# Create your views here.
def index(request):
	return render(request, "links/home.html") #django is going to find this file in links/templates folder

def suggest(request, debug=False):
    print(request)
    if request.method == "GET":
        message_data = request.GET['searchbar']
        search = wikipedia.search(message_data)
        if debug:
            print(request)
            print(search)

        return JsonResponse({'suggestions':search})



def result(request, debug=False):
    print(request)
    if request.method == "GET":
        message_data = request.GET['searchbar']
        
        try:
            result, message_id = u.Analyse(message_data)
            df = pd.DataFrame(result, columns=['Subject','Relationship','Object','Confidence'])
        
            e1Grp = df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x)).reset_index()
            rows = []
            _ = e1Grp.apply(lambda row: [rows.append([row['Subject'],row['Relationship'], e2, e3]) for e2,e3 in zip(row.Object,row.Confidence)], axis=1)
            e1Grp = pd.DataFrame(rows, columns=e1Grp.columns).set_index(['Subject','Relationship'])
            
            #######################
            id_dict = u.get_dict()

            main_df = df[df['Subject'].apply(lambda row: u.get_id(row,id_dict)) == message_id]
            other_df = df[~df.isin(main_df).all(1)]

            main_df = main_df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x))
            main_df['Count'] = main_df['Object'].apply(lambda x: len(x))
            main_df = main_df[[c for c in main_df if c not in ['Confidence']] + ['Confidence']]
            
            other_df = other_df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x))
            other_df['Count'] = other_df['Object'].apply(lambda x: len(x))
            other_df = other_df[[c for c in other_df if c not in ['Confidence']] + ['Confidence']]
            ########################

            ###### ADDING GROUND TRUTH ######
            main_df = u.add_ground_truth(main_df)
            other_df = u.add_ground_truth(other_df)
            
            ###### ADDING RECALL SCORE ######
            main_df = u.add_recall_score(main_df)
            other_df = u.add_recall_score(other_df)

            # main_df = main_df.reset_index()
            # main_df['Ground Truth'] = main_df.apply(lambda row: u.ground_truth(row['Relationship'], row['Subject']), axis=1)
            # main_df['Count_GT'] = main_df['Ground Truth'].apply(lambda x: len(x))
            # main_df = main_df.set_index(['Subject','Relationship'])

            # other_df = other_df.reset_index()
            # other_df['Ground Truth'] = other_df.apply(lambda row: u.ground_truth(row['Relationship'], row['Subject']), axis=1)
            # other_df['Count_GT'] = other_df['Ground Truth'].apply(lambda x: len(x))
            # other_df = other_df.set_index(['Subject','Relationship'])

            pd.set_option('display.max_columns', 500)
            if debug:
                print(request)
                print(message_data)
                print(main_df)
                print ("############################")
                print(other_df)
            #################################

            with pd.option_context('display.max_colwidth', -1):
                main_df = main_df.to_html()
                other_df = other_df.to_html()

            return JsonResponse({'main_df':main_df, 'debug':e1Grp.to_html(), 'other_df':other_df, 'error':""})   #, 'entity_2_group':e2Grp.to_html()})
        except Exception as e:
            return JsonResponse({'error':str(e)})



obj = Utils()
print (obj)