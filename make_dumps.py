
# coding: utf-8

# In[ ]:


# In[1]:


from news.utils import *


# In[ ]:


N = 100
queries = ['Bill Gates', 'Donald Trump', 'Ivanka']#, 'Brad Pitt', 'Christopher Nolan', 'Christian Bale', 'Megan Fox', 'Steve Jobs', 'Hillary Clinton', 'Gerhard Weikum']


for query in queries:
	print('start for {}'.format(query))
	print(make_headline_dict(query))
	print(make_useful100(query))
