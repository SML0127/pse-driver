import hashlib
import requests
import string

with open('compare1.txt', 'r') as file:
    data = file.read().replace('\n', '')
with open('compare2.txt', 'r') as file1:
    data1 = file1.read().replace('\n', '')


print(hashlib.sha256(data.encode()).hexdigest())
print(hashlib.sha256(data1.encode()).hexdigest())
print(hashlib.sha256(data.encode()).hexdigest() == hashlib.sha256(data1.encode()).hexdigest())

