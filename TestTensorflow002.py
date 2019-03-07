# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 09:26:35 2018

@author: Administrator
"""

import tensorflow.examples.tutorials.mnist.input_data as input_data
import tensorflow as tf
sess = tf.InteractiveSession()
mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)
x = tf.placeholder("float", [None, 784])
W = tf.Variable(tf.zeros([784,10]))
b = tf.Variable(tf.zeros([10]))
y = tf.nn.softmax(tf.matmul(x,W) + b)
y_ = tf.placeholder("float", [None,10])
cross_entropy = -tf.reduce_sum(y_*tf.log(y))
train_step = tf.train.GradientDescentOptimizer(0.01).minimize(cross_entropy)
init = tf.initialize_all_variables()
#sess = tf.Session()
print(sess.run(init))