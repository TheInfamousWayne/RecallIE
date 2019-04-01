from django.urls import path, include
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('headlines/', views.headlines, name='headlines'),
    path('recall/', views.recall, name='recall'),
    path('update/', views.update, name='update')
]