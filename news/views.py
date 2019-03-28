from django.shortcuts import render
from django.http import JsonResponse
from .utils import Utils, load_headline_dict, get_news
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
	news = get_news(link)
	return JsonResponse({'news':news})
