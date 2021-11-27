"""Unit tests of the solution"""
import os
import pytest
from pyspark.sql import functions as F
from bin import  ovh00

def test_solution1(spark_session):
   """Test if solution 1 are correctly computed."""
   spark_session
   expect_rst = [['A' ,'Blue'],['B' ,'Green'],['A' ,'Black']]
   header = ['Key','Value']
   expect_collect = spark_session.createDataFrame(expect_rst, header).collect()
   data = [['A', 'Blue'], ['A', 'Red'], ['A', 'Blue'], ['A', 'Blue'], ['B', 'Purple'], ['B', 'Green'], ['B', 'Purple'],
           ['B', 'Green'], ['C', 'Black']]

   sol1_coll = ovh00.main(data, header, 1, 'app00')
   assert expect_collect == sol1_coll

def test_solution2(spark_session):
   """Test if solution 2 are correctly computed."""
   spark_session
   expect_rst = [['A' ,'Blue'],['B' ,'Green'],['A' ,'Black']]
   header = ['Key','Value']
   expect_collect = spark_session.createDataFrame(expect_rst, header).collect()
   data = [['A', 'Blue'], ['A', 'Red'], ['A', 'Blue'], ['A', 'Blue'], ['B', 'Purple'], ['B', 'Green'], ['B', 'Purple'],
           ['B', 'Green'], ['C', 'Black']]

   sol1_coll = ovh00.main(data, header, 2, 'app00')
   assert expect_collect == sol1_coll

def test_solution3(spark_session):
   """Test if solution 2 are correctly computed."""
   spark_session
   expect_rst = [['A' ,'Blue'],['B' ,'Green'],['A' ,'Black']]
   header = ['Key','Value']
   expect_collect = spark_session.createDataFrame(expect_rst, header).collect()
   data = [['A', 'Blue'], ['A', 'Red'], ['A', 'Blue'], ['A', 'Blue'], ['B', 'Purple'], ['B', 'Green'], ['B', 'Purple'],
           ['B', 'Green'], ['C', 'Black']]

   sol1_coll = ovh00.main(data, header, 3, 'app00')
   assert expect_collect == sol1_coll