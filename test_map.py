# coding:utf-8

import multiprocessing as mp
from functools import partial

class A(object):
    def func1(self, x):
        return x+10


    def func2(self, *args):
        x, y = args[0]
        return x*10+y


    def main(self):
        pool = mp.Pool(2)
        l = list(range(10))
        l2 = [(x, x) for x in range(10)]
        print('l:',l)
        print('l2:',l2)
        res = pool.map(self.func1, l)
        print(res)
        res = pool.map(self.func2, l2)
        print(res)
        pool.close()
        pool.join()


if __name__ == '__main__':
    a = A()
    a.main()
