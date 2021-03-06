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
	main_df = pd.read_pickle("data/dumps/web_reality-{}-{}.pkl".format(100,query))
	web = main_df[['Object','Count']]
	web = web.rename(index=str, columns={"Object": "Web", "Count": "Web_Count"})
	reality = main_df[['Pseudo Ground Truth', 'Count_PGT']]

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

	with open('data/dumps/temp_df.pkl', 'wb') as fp:
		pickle.dump(df, fp, protocol=pickle.HIGHEST_PROTOCOL)

	web_reality = web.join(reality)
	with open('data/dumps/temp_web_reality_df.pkl', 'wb') as fp:
		pickle.dump(web_reality, fp, protocol=pickle.HIGHEST_PROTOCOL)

	df = add_dummy(df, query=query)
	df = count_confidence(df)
	df = df.join(web_reality)

	with open('data/dumps/temp_initial_df.pkl', 'wb') as fp:
		pickle.dump(df, fp, protocol=pickle.HIGHEST_PROTOCOL)

	df = add_3Recalls(df)

	with pd.option_context('display.max_colwidth', -1):
		df = df.to_html()

	return JsonResponse({'news_df':df})


def add_3Recalls(df):
	def remove_count(element):
		return element.split(':')[0]
	def r_web(df):
		extractions = df['Object']
		clean_web = list(map(remove_count, df['Web']))
		try:
			return len(list(set(clean_web) & set(extractions))) / len(clean_web)
		except Exception as e:
			return 1
	
	with open('data/dumps/temp_initial_df.pkl', 'rb') as fp:
		initial_final_df = pickle.load(fp)

	df['R_Local'] = df['Confidence'].apply(lambda x: sum(x)).fillna(1) / initial_final_df['Confidence'].apply(lambda x: sum(x)).fillna(1)
	df['R_Web'] = df.apply(r_web, axis=1)
	df['R_Reality'] = np.random.randint(0, 100, df.shape[0])/100
	remove_cols = ['Web','Web_Count','Pseudo Ground Truth','Count_PGT']
	df.drop(remove_cols, axis=1, inplace=True)
	return df




def update(request):
	if request.method == "GET":
		threshold = float(request.GET['confidence'])
		query = request.GET['query']
	print(threshold)
	with open('data/dumps/temp_df.pkl', 'rb') as fp:
		df = pickle.load(fp)

	with open('data/dumps/temp_web_reality_df.pkl', 'rb') as fp:
		web_reality = pickle.load(fp)

	df.drop(df[df.Confidence < threshold].index, inplace=True)
	#df['Object'][df.Confidence < threshold] = '--removed--'
	df = add_dummy(df, query=query)
	df = count_confidence(df)
	df = df.join(web_reality, how='inner')
	df = add_3Recalls(df)
	with pd.option_context('display.max_colwidth', -1):
		df = df.to_html()

	return JsonResponse({'news_df':df})