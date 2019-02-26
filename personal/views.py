from django.shortcuts import render
from .utils import Utils
import pandas as pd
from django.http import JsonResponse


# Create your views here.
def index(request):
	return render(request, "personal/home.html") #django is going to find this file in personal/templates folder

def result(request):
	if request.method == "GET":
		# print(request)
		message_data = request.GET['texttoanalyse']
		
		try:
			u = Utils()            
			result, message_id = u.Analyse(message_data)
			df = pd.DataFrame(result, columns=['Subject','Relationship','Object','Confidence'])
			#print (df)
			#print(message_id)
			e1Grp = df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x)).reset_index()
			rows = []
			_ = e1Grp.apply(lambda row: [rows.append([row['Subject'],row['Relationship'], e2, e3]) for e2,e3 in zip(row.Object,row.Confidence)], axis=1)
			e1Grp = pd.DataFrame(rows, columns=e1Grp.columns).set_index(['Subject','Relationship'])
			#e1Grp['Count'] = e1Grp['Object'].apply(lambda x: len(x))
			#print(e1Grp)
            
			##############################
# 			id_dict = u.get_dict()

			main_df = df[df['Subject'].apply(lambda row: u.get_id(row)) == message_id]
			other_df = df[~df.isin(main_df).all(1)]
			#print("2")
			main_df = main_df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x))
			main_df['Count'] = main_df['Object'].apply(lambda x: len(x))
			main_df = main_df[[c for c in main_df if c not in ['Confidence']] + ['Confidence']]
			#print(other_df)
            
			if (not other_df.empty):            
				other_df = other_df.sort_values('Object', ascending=True).drop_duplicates().groupby(['Subject','Relationship']).agg(lambda x: list(x))
				other_df['Count'] = other_df['Object'].apply(lambda x: len(x))
				other_df = other_df[[c for c in other_df if c not in ['Confidence']] + ['Confidence']]
			#print("4")            
	        ##############################
			del u
			with pd.option_context('display.max_colwidth', -1):
				main_df = main_df.to_html()
				other_df = other_df.to_html()
				e1Grp = e1Grp.to_html()
			#print("5")
			return JsonResponse({'main_df':main_df, 'debug':e1Grp, 'other_df':other_df, 'error':""})
		except Exception as e:
			return JsonResponse({'error':str(e)})

