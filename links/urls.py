from django.urls import path, include
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('result', views.result, name='result'),
    path('suggest', views.suggest, name='suggest')
]