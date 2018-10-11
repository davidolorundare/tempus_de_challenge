# import os
# from pyfakefs.fake_filesystem_unittest import Patcher
# import pytest
import json
import requests

#
# with Patcher() as patcher:
#    # start up
#    patcher.setUp()
#
#    # create fake directory
#    patcher.fs.create_dir('/data/')
#
#    # ensure it is created empty
#    if not os.listdir('/data/'):
#        print("dir is empty")
#    else:
#        print("dir is not empty")
#    # create a new file in it
#    patcher.fs.create_file('/data/xx2.txt', contents='test')
#
#    # ensure it is no longer empty
#    if not os.listdir('/data/'):
#        print("dir is empty")
#    else:
#        print("dir not empty")
#
#    # list all the contents of the directory
#    print(os.listdir('/data/'))
#
#    # close up
#    patcher.tearDown()
#
#    # ensure you have really closed up
#    print(os.listdir('.'))


# make http requests
url = "https://newsapi.org/v2/sources?language=en&apiKey=68ce2435405b42e5b4a90080249c6962"
http_call = requests.get(url)
json_response = http_call.json()
json_response_string = json.dumps(json_response)
json_encoding = http_call.encoding

# validate the json data
try:
    print("encoding:")
    print(json_encoding)
    json.loads(json_response_string, encoding=json_encoding)
    print("json loads correctly")
except ValueError as err:
    print("json loads badly")



try:
    json.dumps(json_response)
    print("json dumps correctly")
except ValueError as err:
    print("json dumps badly")
