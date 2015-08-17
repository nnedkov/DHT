__author__ = 'elias'

import hashlib

BITS = 7


def work(x):

    y = 0
    z = 0
    while True:
        result = hashlib.sha256(str(y) + str(x) + str(z))
        result_dig = result.hexdigest()
        if result_dig[:BITS] == "0"*BITS:
            print result_dig
            return y, z
        if y == 4095:
            y = 0
            z += 1
        else:
            y += 1


def verify(x, y, z):
    if hashlib.sha256(str(y) + str(x) + str(z)).hexdigest()[:BITS] == "0"*BITS:
        return True
    else:
        return False


def random_with_N_digits(n):
    import random
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return random.randint(range_start, range_end)


if __name__ == '__main__':
    import time
    times = []
    for i in range(10):
        number = random_with_N_digits(50)
        #print number
        start_time = time.time()
        y, z = work(number)
        verify(number, y, z)
        end_time = time.time()
        print end_time - start_time
