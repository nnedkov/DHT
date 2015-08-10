__author__ = 'elias'

import hashlib
import time

def verify(y):

    x = 0
    z = 0
    while True:
        result = hashlib.sha256(str(x) + str(y) + str(z))
        result_dig = result.hexdigest()
        final_result = str(result_dig)
        if final_result[:8] == "0" * 8:
            print final_result
            return x, z
        x += 1
        z += 1


if __name__ == '__main__':
    start_time = time.time()
    print verify(857932514785396542)
    elapsed_time = time.time() - start_time
    print elapsed_time
