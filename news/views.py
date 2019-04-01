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
	print(entity_name)
	headline_dict = load_useful100(entity_name)
	return JsonResponse({'headlines':headline_dict})

def recall(request):
	if request.method == "GET":
		link = request.GET['link']
		query = request.GET['query']
	# news = get_news(link)
	print(query)
	### WEB REALITY INTEGRATION ###
	main_df = pd.read_pickle("data/dumps/web_reality-{}-{}.pkl".format(11,query))
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
	        df = df.append({'Subject': query, 'Relationship':r, 'Object':'', 'Confidence':round(np.random.uniform(0.85,1),2)}, ignore_index=True)
	    return df

	### More Operations
	u = Utils()
	lock = Lock()
	message_id = u.get_id(query)
	isPerson = True if ('Citizen of' in list(web.reset_index()['Relationship'])) else False
	doc = News(link=link, name=query, lock=lock)
	doc.join()
	_,_,df = doc.get_main_df()

	df.loc[df['Confidence'] == '?','Confidence'] = df['Confidence'].apply(lambda x: round(np.random.uniform(0.85,1),2))
	df.Confidence = df.Confidence.astype(float).fillna(0.0)
	
	df = add_dummy(df, person=isPerson)
	web_reality = web.join(reality)

	with open('data/dumps/temp_df.pkl', 'wb') as fp:
		pickle.dump(df, fp, protocol=pickle.HIGHEST_PROTOCOL)

	with open('data/dumps/temp_web_reality_df.pkl', 'wb') as fp:
		pickle.dump(web_reality, fp, protocol=pickle.HIGHEST_PROTOCOL)

	df = count_confidence(df)
	df = df.join(web_reality)

	

	with pd.option_context('display.max_colwidth', -1):
		df = df.to_html()

	return JsonResponse({'news_df':df})


def update(request):
	if request.method == "GET":
		threshold = float(request.GET['confidence'])
	print(threshold)
	with open('data/dumps/temp_df.pkl', 'rb') as fp:
		df = pickle.load(fp)

	with open('data/dumps/temp_web_reality_df.pkl', 'rb') as fp:
		web_reality = pickle.load(fp)

	df.drop(df[df.Confidence < threshold].index, inplace=True)
	df = count_confidence(df)
	df = df.join(web_reality)
	with pd.option_context('display.max_colwidth', -1):
		df = df.to_html()

	return JsonResponse({'news_df':df})