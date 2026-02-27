print("COME ON KRISHNA")

# print("json.load() and json loads() ")
# import json
# with open("C:\\app\\zeyoplus\\practice\\mKrishna2\\Datasets\\scen.json" ,"r") as f:
#     data = json.load(f)
# print(data)
#
# json_str = '{"name": "Krishna", "age": 25}'
# data1 = json.loads(json_str)
# print(data1)

print("named tuple")
 # tuple and index
person=(1,"krishna")
print(person[1])

#nametuple

from collections import namedtuple

Person=namedtuple("Person",["no","name"])

person=Person(1,"krishna")

print(person.no, person.name)



