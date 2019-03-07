# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 09:18:43 2018

@author: Administrator
"""

import tensorflow as tf
hello=tf.constant("hello , tensorflow")
sess=tf.Session()
print(sess.run(hello))
a=tf.constant(10)
b=tf.constant(20)
print(sess.run(a+b))
sess.close