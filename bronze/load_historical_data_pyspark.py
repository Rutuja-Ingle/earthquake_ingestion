from pyspark.sql import SparkSession
import argparse
import requests


if __name__ == '__main__':

       # Define spark session
        spark = SparkSession.builder.master("local[*]").appName("Historical Load").getOrCreate()


       #          
