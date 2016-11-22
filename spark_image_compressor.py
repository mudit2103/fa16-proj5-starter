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

def combine_sub_blocks(rdd):
    """
    Given an rdd of subblocks from many different images, combine them together to reform the images.
    Should your rdd should contain values that are np arrays of size (height, width).

    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    return rdd

def run(images):
    """
    THIS FUNCTION MUST RETURN AN RDD

    Returns an RDD where all the images will be proccessed once the RDD is aggregated.
    The format returned in the RDD should be (image_id, image_matrix) where image_matrix 
    is an np array of size (height, width, 3).
    """
    sc = SparkContext()
    rdd = sc.parallelize(images, 16) \
        .map(truncate).repartition(16)
    rdd = generate_Y_cb_cr_matrices(rdd)
    rdd = generate_sub_blocks(rdd)
    rdd = apply_transformations(rdd)
    rdd = combine_sub_blocks(rdd)

    ### BEGIN SOLUTION HERE ###
    # Add any other necessary functions you would like to perform on the rdd here
    # Feel free to write as many helper functions as necessary
    return  rdd













