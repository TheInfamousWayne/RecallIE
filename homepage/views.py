from django.shortcuts import render



# Create your views here.
def index(request):
	return render(request, "homepage/home.html") #django is going to find this file in personal/templates folder

