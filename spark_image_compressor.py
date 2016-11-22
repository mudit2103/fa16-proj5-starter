import pyspark
from pyspark import SparkContext
import cv2
import numpy as np 
import scipy as sp
import struct
from helper_functions import *
from constants import *


### WRITE ALL HELPER FUNCTIONS ABOVE THIS LINE ###

def generate_Y_cb_cr_matrices(rdd):
    """
    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    return rdd

def generate_sub_blocks(rdd):
    """
    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    return rdd

def apply_transformations(rdd):
    """
    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    return rdd

def run(images):
    """
    THIS FUNCTION MUST RETURN AN RDD

    Returns an RDD where all the images will be proccessed once the RDD is aggregated.
    """
    sc = SparkContext()
    rdd = sc.parallelize(images, 16) \
        .map(truncate).repartition(16)
    rdd = generate_Y_cb_cr_matrices(rdd)
    rdd = generate_sub_blocks(rdd)
    rdd = apply_transformations(rdd)

    ### BEGIN SOLUTION HERE ###
    # Add code here to put sub-blocks back together
    # Feel free to write as many helper functions as necessary
    return  rdd













