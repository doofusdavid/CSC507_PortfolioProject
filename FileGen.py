import random

# Start with smaller set for testing
with open('hugefile1.txt', 'w') as file:
    for i in range(100000):
        file.write(str(random.randint(0, 32767)) + '\n')

with open('hugefile2.txt', 'w') as file:
    for i in range(100000):
        file.write(str(random.randint(0, 32767)) + '\n')
