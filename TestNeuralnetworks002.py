# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 09:19:30 2018

@author: Administrator
"""
#线性模型
import tensorflow as tf
from numpy.random import RandomState
batch_size=16
w1=tf.Variable(tf.random_normal([2,3],stddev=1,seed=1))
w2=tf.Variable(tf.random_normal([3,1],stddev=1,seed=1))
x=tf.placeholder(tf.float32,shape=(None,2),name="x-input")
y_=tf.placeholder(tf.float32,shape=(None,1),name="y-input")
##定义神经网络前向传播的过程
a=tf.matmul(x,w1)
y=tf.matmul(a,w2)

#定义损失函数和反向传播的算法
cross_entropy=-tf.reduce_mean(
        y_*tf.log(tf.clip_by_value(y,1e-10,1.0))
        )
train_step=tf.train.AdadeltaOptimizer(0.001).minimize(cross_entropy)


#通过随机数生成一个模拟的数据集
rdm=RandomState(1)
dataset_size=128
X=rdm.rand(dataset_size,2)

#定义规则来给出样本的标签。这里所有x1+x2<1的样例都被认为是正样本（比如零件合格），
#而其他为负样本（比如零件不合格）。和tensorflow游乐场中的表示法不大一样的地方时，
#在这里使用0来表示负样本，1来表示负样本。大部分解决分类问题的神经网络都会采用
#0和1的表示方法
Y=[[int(x1+x2<1.2)] for (x1,x2) in X]

#创建一个会话来运行tensorflow程序
#with tf.Session() as sess:
sess=tf.Session()
init_op=tf.initialize_all_variables()
sess.run(init_op)
print (sess.run(w1))
print (sess.run(w2))
'''
在训练之前神经网络参数的值：
w1=[[-0.811318222,1.48459876,0.06532937]
    [-2.44270396,0.0992484,0.59122431]
]
w2=[[-0.81131822],[1.48459876],[0.06532937]]
'''

#设定训练的轮数.
STEPS=5000
for i in range(STEPS):
    #每次选取batch_size个样本进行训练
    start=(i*batch_size) % dataset_size
    end=min(start+batch_size,dataset_size)
    
    #通过选取的样本训练神经网络并更新参数。
    sess.run(train_step,feed_dict={x:X[start:end],y_:Y[start:end]})
    
    if i % 1000 == 0:
        #每隔一段时间计算在所有数据上的交叉熵并输出。
        total_cross_entropy=sess.run(
                cross_entropy,feed_dict={x:X,y_:Y})
        print("after %d training step(s),cross entropy on all data is %g"
              %(i,total_cross_entropy))

print (sess.run(w1))
print (sess.run(w2))
sess.close()

        
