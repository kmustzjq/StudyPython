# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 16:14:50 2018

@author: Administrator
测试神经网络
"""
import tensorflow as tf
x=tf.constant([[0.7,0.9]])
w1=tf.constant([[0.2,0.1,0.4],[0.3,-0.5,0.2]])
w2=tf.constant([[0.6],[0.1],[-0.2]])
#w1=tf.Variable(tf.random_normal([2,3],stddev=1,seed=1))
#w2=tf.Variable(tf.random_normal([3,1],stddev=1,seed=1))
a=tf.matmul(x, w1)
y=tf.matmul(a, w2)
sess=tf.Session()
#with tf.Session() as sess:
#sess.run(w1.initializer)
#sess.run(w1.initializer)
print(sess.run(y))


        


w3=tf.Variable(tf.random_normal([2,3],stddev=1))
w4=tf.Variable(tf.random_normal([3,1],stddev=1))
xx=tf.placeholder(tf.float32,shape=(1,2),name="input")
aa=tf.matmul(xx,w3)
yy=tf.matmul(aa,w4)
#init_op=tf.initialize_all_variables()
init_op=tf.global_variables_initializer()
sess.run(init_op)
print(sess.run(yy,feed_dict={x:[[0.7,0.9]]}))
sess.close()
