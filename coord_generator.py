import time
from datetime import datetime 
from random import seed
from random import randint
from random import choice

with open('random_seed.csv') as f:
    for n in f:
        # seed random number generator
        seed(n)

        # generate integers for coordinates

        if choice([True, False]) == True:
            rand_ra_pos = '-'
        else:
            rand_ra_pos = ''
        rand_ra_h = format(randint(0,11), '02d')
        rand_ra_m = format(randint(0,59), '02d')
        rand_ra_s = format(randint(0,59), '02d')
        rand_ra_cs = format(randint(0,99), '02d')

        if choice([True, False]) == True:
            rand_dec_pos = '-'
        else:
            rand_dec_pos = ''
        rand_dec_d = format(randint(0,89), '02d')
        rand_dec_m = format(randint(0,59), '02d')
        rand_dec_s = format(randint(0,59), '02d')
        rand_dec_cs = format(randint(0,9))

        a = '{}{}:{}:{}.{}, {}{}:{}:{}.{}'.format(rand_ra_pos,rand_ra_h,rand_ra_m,rand_ra_s,rand_ra_cs,rand_dec_pos,rand_dec_d,rand_dec_m,rand_dec_s,rand_dec_cs)

        print('[{}] {}'.format(datetime.now(),a))

        with open('test/messages.txt', 'r') as g:
            get_all = g.readlines()
        with open('test/messages.txt', 'w') as h:
            for i, line in enumerate(get_all,1):
                if i == 17:    
                    h.writelines('array_1:target:radec, {}\n'.format(a))
                else:    
                    h.writelines(line)

        exec(open("./redis_testing.py").read())

        time.sleep(5)