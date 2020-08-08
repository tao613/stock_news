# -*- coding:utf-8 -*-
# Project fintech - Sai team
# Individual stock news collection & analysis - collection module
# main
# code by TAO, 08.05.2020 (MM.DD.YYYY)

# URL, TITLE, SOURCE, TIME, SYMBOL

# finviz.com
# news - page format
# <table width="100%" cellpadding="0" cellspacing="0"><tr><td><table width="100%" cellpadding="1" cellspacing="0" border="0" id="news-table" class="fullview-news-outer">
# <tr><td width="130" align="right" style="white-space:nowrap">Aug-04-20 08:48PM&nbsp;&nbsp;</td><td align="left"><div class="news-link-container"><div class="news-link-left"><a href="https://finance.yahoo.com/news/apple-marketing-may-look-different-004840393.html" target="_blank" class="tab-link-news">Apple Marketing May Look Different Soon</a></div><div class="news-link-right"><span style="color:#aa6dc0;font-size:9px"> WWD</span></div></div></td></tr>
# <tr><td width="130" align="right">08:02PM&nbsp;&nbsp;</td><td align="left"><div class="news-link-container"><div class="news-link-left"><a href="https://finance.yahoo.com/news/disney-mulan-streaming-000200564.html" target="_blank" class="tab-link-news">Disney and Mulan' Are All In on Streaming</a></div><div class="news-link-right"><span style="color:#aa6dc0;font-size:9px"> Bloomberg</span></div></div></td></tr>

import requests
import pandas as pd

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

def scrapyProxy():
	pass

# web_df 是一个dataframe, 用来记录页面信息
# ['']
def webElemants(url, deep_level, web_df):
	ua = UserAgent()
	headers = {
		'User-Agent': ua.random
	}

	# 请求网页
	try:
		response = requests.get(url, headers=headers)
		print('访问页面成功')
	except Exception as e:
		print('访问错误，错误代码: ', response.status_code)
		print('报错: ', e)
		# 没有请求到页面, 结束模块
		return 0

	# 分析网页
	# 变量定义: webPage -> 整个页面;
	#          webHtml -> <html></html>
	#          webHead -> <head></head>
	#          headTags -> <head>中的元素
	#          webBody -> <body></body>
	#          bodyTags -> <body>中的元素
	soup = BeautifulSoup(response.text, 'lxml')
	# webPage: list
	# webPage[0]: class 'bs4.element.Doctype'
	

# soup.contents
# soup.body.div.has_attr('id')
# soup.head.find_all(True)