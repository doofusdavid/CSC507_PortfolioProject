import random

with open('hugefile1.txt', 'w') as file:
    for i in range(1000000000):
        file.write(str(random.randint(0, 32767)) + '\n')

with open('hugefile2.txt', 'w') as file:
    for i in range(1000000000):
        file.write(str(random.randint(0, 32767)) + '\n')
