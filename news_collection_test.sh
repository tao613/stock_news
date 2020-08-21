#!/bin/bash

export PATH=$PATH:/usr/local/bin
source /home/tao/venv36/bin/activate
cd /home/tao/test/stock_news && python news_collection_test.py >> news_collection_test.log
