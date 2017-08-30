# print('mytestfolder'.split('/'))
# print('/'.join('mytestfolder'.split('/')[:-1]))

# import re
#
# fullname = 'ab?c.xyz.xxx'
#
# if '*' in fullname or '?' in fullname:
#     print('the file name contains wildcard.')
#
# pattern = fullname.replace('*', '[0-9a-zA-Z\_]*').replace('?', '[0-9a-zA-Z\_]').replace('.', '\.')
# print(pattern)
# # testname = 'absc.xyz.xxx'
#
# print(re.match(pattern, testname))

# print(re.match(r'[0-9a-zA-Z\_]*\*[0-9a-zA-Z\_]*\.([0-9a-zA-Z\_]+|\*)','ab*c.*'))

# fullpath = 'folder1/folder2/folder3/*.txt'
#
# path = '/'.join(fullpath.split('/')[:-1])
#
# print(path)

# import os
# print(os.sep)
# print('1'.split('/'))
#
# import math
#
# # print(type(math.floor((2**16*10+1000)/6553600/0.1)))
# print(35.5.__int__())

# from multiprocessing import Process, Queue
# import os, time, random
#
# # 写数据进程执行的代码:
# def write(q):
#     print('Process to write: %s' % os.getpid())
#     for value in ['A', 'B', 'C']:
#         print('Put %s to queue...' % value)
#         q.put(value)
#         time.sleep(random.random())
#
# # 读数据进程执行的代码:
# def read(q):
#     print('Process to read: %s' % os.getpid())
#     while True:
#         value = q.get(True)
#         print('Get %s from queue.' % value)
#
# if __name__=='__main__':
#     # 父进程创建Queue，并传给各个子进程：
#     q = Queue()
#     pw = Process(target=write, args=(q,))
#     pr = Process(target=read, args=(q,))
#     # 启动子进程pw，写入:
#     pw.start()
#     # 启动子进程pr，读取:
#     pr.start()
#     # 等待pw结束:
#     pw.join()
#     # pr进程里是死循环，无法等待其结束，只能强行终止:
#     pr.terminate()

import multiprocessing as mp

import time


def foo(q, i):
    q.put('hello a:' + str(i))
    time.sleep(1)
    q.put('hello b:' + str(i))
    time.sleep(1)
    q.put('hello c:' + str(i))
    time.sleep(1)
    return str(i)

if __name__ == '__main__':
    p = mp.Pool()
    m = mp.Manager()
    q = m.Queue()
    for i in range(6):
        p.apply_async(foo, args=(q,i), callback=lambda x: print(x))
    p.close()
    count = 0
    while True:
        feedback = q.get()
        print(feedback)
        count += 1
        if count == 18:
            break

    p.join()
    print('completed')
