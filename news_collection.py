# -*- coding:utf-8 -*-
# Project fintech - Sai team
# Individual stock news collection & analysis - collection module
# main
# code by TAO, 08.05.2020 (MM.DD.YYYY)

# URL, TITLE, SOURCE, TIME, SYMBOL, CONTENT, KEYWORD

# finviz.com
# news - page format
# <table width="100%" cellpadding="0" cellspacing="0"><tr><td><table width="100%" cellpadding="1" cellspacing="0" border="0" id="news-table" class="fullview-news-outer">
# <tr><td width="130" align="right" style="white-space:nowrap">Aug-04-20 08:48PM&nbsp;&nbsp;</td><td align="left"><div class="news-link-container"><div class="news-link-left"><a href="https://finance.yahoo.com/news/apple-marketing-may-look-different-004840393.html" target="_blank" class="tab-link-news">Apple Marketing May Look Different Soon</a></div><div class="news-link-right"><span style="color:#aa6dc0;font-size:9px"> WWD</span></div></div></td></tr>
# <tr><td width="130" align="right">08:02PM&nbsp;&nbsp;</td><td align="left"><div class="news-link-container"><div class="news-link-left"><a href="https://finance.yahoo.com/news/disney-mulan-streaming-000200564.html" target="_blank" class="tab-link-news">Disney and Mulan' Are All In on Streaming</a></div><div class="news-link-right"><span style="color:#aa6dc0;font-size:9px"> Bloomberg</span></div></div></td></tr>

import requests
import pandas as pd
import datetime
import time
import os
import random

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from elasticsearch import Elasticsearch

# global definition
es_news = Elasticsearch()

def scrapyProxy():
	pass

# extractNewsFV - finvim.com
def extractNewsFV(url):
	ua = UserAgent()
	headers = {
		'User-Agent': ua.random
	}

	# 请求网页
	try:
		response = requests.get(url, headers=headers)
		print('Url: ', url)
		print('*'*30 + '访问页面成功!' + '*'*30)
		time.sleep(random.randint(3, 6))
	except Exception as e:
		print('!'*30 + '访问错误，错误代码: ', response.status_code)
		print('!'*30 + '报错: ', e)
		# 没有请求到页面, 结束模块
		return 0

	# 分析网页并存在news_df中
	news_df = pd.DataFrame()
	news_dict =  {}

	soup = BeautifulSoup(response.text, 'lxml')
	# news list
	news_list = soup.find('table', id='news-table').find_all('tr')

	# URL, TITLE, SOURCE, TIME, SYMBOL
	# item - list
	# item[0] - <class 'bs4.element.Tag'>
	for item in news_list:
		# timestamp
		try:
			news_time_str = item.td.text.strip()
			# Aug-07-20 08:59PM
			# or 08:59PM
			# set a recorder to record DATE
			if len(news_time_str.split(' '))==2:
				date_rec = news_time_str.split(' ')[0]
				# datetime: %b-%d-%y %H:%M%p
				news_time = datetime.datetime.strptime(news_time_str, '%b-%d-%y %H:%M%p')
			else:
				news_time_str = date_rec + ' ' + news_time_str
				news_time = datetime.datetime.strptime(news_time_str, '%b-%d-%y %H:%M%p')
			# datetime.datetime(2020, 7, 20, 8, 59)
			news_timestamp = datetime.datetime.timestamp(news_time)
			# 1596870180.0
		except Exception as e:
			print('Error in news_time: ', e)
			news_timestamp = ''
		# url
		try:
			news_url = item.find('div', class_='news-link-left').a.attrs['href']
			# https://www.barrons.com/articles/walt-disney-and-the-end-of-moviegoing-51596848369?siteid=yhoof2
		except Exception as e:
			print('Error in news_url: ', e)
			news_url = 'Noise Data'
		# title
		try:
			news_title = item.find('div', class_='news-link-left').a.text.strip()
			# Walt Disney and the End of Moviegoing
		except Exception as e:
			print('Error in news_title: ', e)
			news_title = ''
		# source
		try:
			news_source = item.find('div', class_='news-link-right').text.strip()
			# Barrons.com
		except Exception as e:
			print('Error in news_source: ', e)
			news_source = ''
		# symbol
		try:
			news_symbol = soup.title.text.strip().split(' ')[0].upper()
			# AAPL
			# same as url
		except Exception as e:
			print('Error in news_symbol: ', e)
			news_symbol = url.split('=')[1].upper()
			# url: https://finviz.com/quote.ashx?t=AAPL

		news_dict = {
			'URL': news_url,
			'TIME': news_timestamp,
			'TIME_TEXT': news_time_str,
			'TITLE': news_title,
			'SOURCE': news_source,
			'SYMBOL': news_symbol,
			'CONTENT': '',
			'KEYWORD': ''
		}
		print(news_dict)

		# to dataframe
		# news_df = news_df.append(news_dict, ignore_index=True)
		# to ES
		save2ESEach(news_dict)

	# end for item in news_list
	# code
	# news_df.sort_values(by='SYMBOL', ascending=True, inplace=True)
	# news_df = news_df.reset_index(drop='index')
	# news_df.index = news_df.index + 1
	# code

	# test - save to excel
	# code
	# today = datetime.date.today()
	# writer = pd.ExcelWriter('D:/' + today.strftime('%m%d%H%S') + 'stock_news.xlsx')
	# news_df.to_excel(writer, 'news')
	# writer.save()
	# code

	# return news_df
	return (url + 'Done.' + '$'*30)

def save2ESEach(news_dict):
	global es_news

	# ES uses parameters
	index_name = 'snews_test_2'
	doc_type = 'news'
	try:
		# if index exists
		if es_news.indices.exists(index=index_name):
			# id(primary key) -> url
			# data structure:
			# (news)url: {
			# 	timestamp: x,
			# 	timestr: x,
			# 	title: x,
			# 	source: x,
			# 	symbol: [
			# 		x,
			# 		y
			# 	],
			# 	content: x,
			# 	keyword: x
			# }
			result = es_news.get(index=index_name, doc_type=doc_type, id=news_dict['URL'], ignore=[404, 400])
			# result: {'_index': 'snews_test_2', '_type': 'news', '_id': 'https://www.investors.com/news/technology/google-stock-boost-cloud-computing-growth-could-be-bigger/?src=A00220', '_version': 1, '_seq_no': 109, '_primary_term': 1, 'found': True, '_source': {'url': 'https://www.investors.com/news/technology/google-stock-boost-cloud-computing-growth-could-be-bigger/?src=A00220', 'time': 1596760200.0, 'time_text': 'Aug-07-20 08:30AM', 'title': 'Why This FANG Stock Could Find Room To Run On The Cloud', 'source': "Investor's Business Daily", 'symbol': ['GOOGL'], 'content': '', 'keyword': ''}}
			# if id exists
			if result['found']:
				if not(news_dict['SYMBOL'] in result['_source']['symbol']):
					# return NoneType
					result['_source']['symbol'].append(news_dict['SYMBOL'])
					print('New symbol: ', result['_source']['symbol'])
					es_news.update(
						index=index_name,
						doc_type=doc_type,
						id=news_dict['URL'],
						body={
							'doc': {
								'symbol': list(
											set(result['_source']['symbol'])
											),
								'doc_as_upsert': True
							}
						}
						# body={
						# 		'script': {
						# 			'inline': 'ctx._source.symbol+=params.new_symbol',
						# 			'params': {
						# 						'new_symbol': news_dict['SYMBOL']
						# 			}
						# 		}
						# 	}
						)
					print('Symbol Changed. ', 
							es_news.get(index=index_name, doc_type=doc_type, id=news_dict['URL'], ignore=[404, 400])
						)
				else:
					print('Symbol no change.')
			else:
			# if id does not exist
				es_news.index(
						index=index_name,
						doc_type=doc_type,
						id=news_dict['URL'],
						body={
								'url': news_dict['URL'],
								'time': news_dict['TIME'],
								'time_text': news_dict['TIME_TEXT'],
								'title': news_dict['TITLE'],
								'source': news_dict['SOURCE'],
								'symbol': [news_dict['SYMBOL'],],
								'content': '',
								'keyword': ''
						}
					)
		# if index not exist
		else:
			es_news.indices.create(index=index_name, ignore=400)
			ex_news.index(
					index=index_name,
					doc_type=doc_type,
					id=news_dict['URL'],
					body={
							'url': name_dict['URL'],
							'time': news_dict['TIME'],
							'time_text': news_dict['TIME_TEXT'],
							'title': news_dict['TITLE'],
							'source': news_dict['SOURCE'],
							'symbol': [news_dict['SYMBOL'],],
							'content': '',
							'keyword': ''
					}
				)
	except Exception as e:
		print('!'*30 + 'Save ES Error: ', e)

	return ('*'*30 + 'All Done in ES.' + '*'*30)

def save2ES(news_df):
	pass

# csv files in the same directory
def symbolFromCSV():
	file_names = os.listdir()
	# filenames -> list
	csv_names = []
	# all csv files name
	for file in file_names:
		if file.endswith('.csv'):
			csv_names.append(file)
			print('Add csv file: ', file)

	# headers in csv:
	# 'Symbol', 'Name', 'LastSale', 'MarketCap', 'IPOyear', 'Sector', 'industry', 'Summary Quote'
	symbol_list = []
	for file in csv_names:
		print('Processing: ', file)
		try:
			csv = pd.read_csv(file)
			# csv -> <class 'pandas.core.frame.DataFrame'>
			# csv['Symbol'] -> <class 'pandas.core.series.Series'>
			symbol_list += (
					list(csv['Symbol'])
				)
		except Exception as e:
			print('Read CSV Error: ', e)

	print('*'*30 + 'Total symbols: ', len(symbol_list))
	return symbol_list



if __name__=='__main__':
	url_base = 'https://finviz.com/quote.ashx?t='
	url_list = symbolFromCSV()
	# url_list = [
	# 			# 'AAPL',
	# 			'GOOGL',
	# 			'AMZN'
	# ]
	for i in url_list:
		extractNewsFV(url_base + i.strip())
