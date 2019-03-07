mo# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 15:41:07 2018

@author: Administrator
"""

import tensorflow as tf
input1=tf.constant(3.0)
input2=tf.constant(4.0)
input3=tf.constant(5.0)
intermed=tf.add(input2,input3)
mul=tf.multiply(input1,intermed)
with tf.Session() as sess:
    result=sess.run([mul,intermed])


print(result)

