#!/usr/bin/env python
# -*- coding: utf8 -*-

from collections import defaultdict
from re import findall
from json import load as json_load
from codecs import open as codecs_open
from pymongo import MongoClient

from connect import moreover_mongo

def process_topics():
	"""Processes hand mapped topics and returns them in a dictionary of topic: mozcat"""
	
	mapping = {}
	with open('moreover_topics.tsv') as f:
		for line in f:
			if len(line) > 5:
				line = line[:-1].split('\t')
				topic = line[0]
				decision = line[1]
				if decision != 'del':
					mapping[topic] = decision	
	return mapping

def ngrams(s, n):
	"""Given a string s, splits it into ngrams"""
	ngrams = []
	#s = s.split() #regex already splits them so this is not needed
	for i in range(len(s)-n+1):
		ngrams.append(' '.join(s[i:i+n]))
	return ngrams

def unique_bigrams_per_topic():
	"""Finds the most popular bigrams that only exist in a single topic"""
	
	#first import the mozcat heirarchy
	with codecs_open('mozcat_heirarchy.json', encoding='utf8') as f:
		tree = json_load(f)
	
	#create v --> k instead of k --> v
	vk_tree = {}
	for k,v in tree.iteritems():
		if k != "world": #ignore world items for now. TODO: reenable
			vk_tree[k] = k
			for x in v:
				vk_tree[x] = k
	
	#load the mapping of topic --> mozcat and bring them up to the higher level
	mapping = process_topics()
	
	#set up connection and query to find data from things with topics
	moreover = moreover_mongo()
	m_filter = {'topics':{'$exists':True}}
	m_data = {'topics':1, 'title':1, 'url':1}
	
	#this is really considering the top level mozcat so we have to accomodate for that 
	
	bigram_topics = {} #container for bigram: [topic, count]
	#iterate through each document
	for n, doc in enumerate(moreover.find(m_filter, m_data)):
		title_tokens = findall("[a-z]+", doc['title'].lower()) #tokenize the title
		bigrams = ngrams(title_tokens, 2) #extract bigrams
		
		top_level_topics = set()
		for x in doc['topics']:
			if x in mapping:
				m = mapping[x]
				if m in vk_tree:
					top_level_topics.update([vk_tree[m]])
		
		if len(top_level_topics) == 1: #ignore this bigram if there's more than one distinct top level non-world mozcat
			top_level = list(top_level_topics)[0]
			for bigram in bigrams: #for each bigram
				if bigram in bigram_topics: #if it exists, 
					record = bigram_topics[bigram]
					if record != 0: #ignore if already spotted as a duplicate
						if bigram_topics[bigram][0] == top_level: #otherwise increment 
							bigram_topics[bigram][1] += 1
						else:
							bigram_topics[bigram] = 0 #or discard if a duplicate
				else:
					bigram_topics[bigram] = [top_level, 1] #if never seen before, start off the counter
		
		if n % 100000 == 0:
			print "Processed {0} article titles".format(n) #currently about 1m
	
	#remove the duplicates
	to_delete = []
	for bigram, topics in bigram_topics.iteritems():
		if topics == 0:
			to_delete.append(bigram) #can't delete them in-place because science
	
	for x in to_delete:
		del bigram_topics[x]
	
	print "There are {0} bigrams with only 1 top level category".format(len(bigram_topics))
	
	#convert to topic --> bigram:count
	topic_bigrams = defaultdict(list)
	for bigram, topic in bigram_topics.iteritems():
		topic_bigrams[topic[0]].append([bigram, topic[1]])
	
	#clear some memory
	bigram_topics = 0
	
	#now sort
	for topic in topic_bigrams.iterkeys():
		topic_bigrams[topic] = sorted(topic_bigrams[topic], key=lambda x: x[1], reverse=True)
	
	return topic_bigrams


