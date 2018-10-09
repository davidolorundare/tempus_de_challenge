import os
from pyfakefs.fake_filesystem_unittest import Patcher
import pytest


with Patcher() as patcher:
   # start up
   patcher.setUp()

   # create fake directory
   patcher.fs.create_dir('/data/')
  
   # ensure it is created empty
   if not os.listdir('/data/'):
       print("dir is empty")
   else:
       print("dir is not empty")
   # create a new file in it
   patcher.fs.create_file('/data/xx2.txt', contents='test')
   
   # ensure it is no longer empty
   if not os.listdir('/data/'):
       print("dir is empty")
   else:
       print("dir not empty")

   # list all the contents of the directory
   print(os.listdir('/data/'))

   # close up
   patcher.tearDown()

   # ensure you have really closed up
   print(os.listdir('.'))
