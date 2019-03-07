# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 09:35:20 2018

@author: Administrator
"""

import tensorflow as tf
#tf.constant是一个计算，这个计算的结果是一个张量，保存在变量a中。
a=tf.constant([1.0,2.0],name="a")
b=tf.constant([2.0,3.0],name="b")
#sess=tf.Session()
#print(sess.run(a+b))
result=tf.add(a,b,name="add")
print(result)
with tf.Session() as sess:
    print(sess.run(result))