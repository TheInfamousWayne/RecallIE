from django.shortcuts import render
from django.http import JsonResponse
from .utils import *
import random


# Create your views here.
def index(request):
	return render(request, "news/headlines.html") #django is going to find this file in news/templates folder

def headlines(request):
	if request.method == "GET":
		entity_name = request.GET['entity']
	headline_dict = load_headline_dict(entity_name)
	top100 = dict(random.sample(headline_dict.items(), 100))
	return JsonResponse({'headlines':top100})

def result(request):
	if request.method == "GET":
		link = request.GET['link']
		query = request.GET['query']
	# news = get_news(link)
	print(query)
	### WEB REALITY INTEGRATION ###
	main_df = pd.read_pickle("data/dumps/web_reality-{}-{}.pkl".format(100,query))
	web = main_df[['Object','Count']]
	web = web.rename(index=str, columns={"Object": "Web", "Count": "Web_Count"})
	reality = main_df[['Pseudo Ground Truth', 'Count_PGT']]


	isPerson = False

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

	### More Operations
	u = Utils()
	lock = Lock()
	isPerson = True if ('Citizen of' in list(web.reset_index()['Relationship'])) else False
	doc = News(link=link, name=query, lock=lock)
	doc.join()
	df = doc.get_main_df().drop('Confidence',axis=1)
	message_id = u.get_id(query)
	df = add_dummy(df, person=isPerson)
	df = count_confidence(df)
	df = df.join(web)
	df = df.join(reality)


	return JsonResponse({'news_df':df.to_html()})
