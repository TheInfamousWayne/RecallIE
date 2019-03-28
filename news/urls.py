from django.urls import path, include
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('headlines/', views.headlines, name='headlines'),
    path('result/', views.result, name='result')
]