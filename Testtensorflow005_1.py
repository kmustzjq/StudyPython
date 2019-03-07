# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 09:51:43 2018

@author: Administrator

通过设定默认会话计算张量的取值
"""

import tensorflow as tf
a=tf.constant([1.0,2.0],name="a")
b=tf.constant([2.0,3.0],name="b")
result=tf.add(a,b)
sess=tf.Session()
with sess.as_default():
    print(result.eval())
