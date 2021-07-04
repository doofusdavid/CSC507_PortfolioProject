import random


def createFile(file_name, filesize):
    with open(file_name, 'w') as file:
        for i in range(filesize):
            if i == filesize - 1:
                file.write(str(random.randint(0, 32767)))
            else:
                file.write(str(random.randint(0, 32767)) + '\n')


filesize = 1000000000
createFile("hugefile1.txt", filesize)
createFile("hugefile2.txt", filesize)
