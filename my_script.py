print("Hello World!")

a = 10
b = 20
c = a + b
print(c)

# List
l = [1, 2, 3, 4]
print(l)

# If we need to observe list in first ref from Index at 0
print(l[0])

# Tuple
my_tuple = (1, "hello", 3.14)
print(my_tuple)
print(my_tuple[1])

# Dict
test_dict = {
    'a': 1,
    'b': 2,
}
print(test_dict)

print(test_dict['a'])
print(test_dict.get('a'))

# Set
my_set = set()
my_set.add(10)
my_set.add(10)

for i in range(11):
    print(i)
    
    
l = [1, 2, 3, 4]
for item in l:
    print(item)


d = {
    "a": 1,
    "b": 2
}
for k, v in d.items():
    print(f"Key: {k}, Value: {v}")


def greater_then_three(input_list):
    results = []
    for item in input_list:
        if item > 3:
            results.append(item)

    return results


list_1 = [1, 2, 3, 4, 5, 6]
print(greater_then_three(list_1))

class DataExtractor:

    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


data_extractor_object = DataExtractor(9)
print(data_extractor_object.get())

#from datetime import datetime
#now = datetime.now().strftime("%Y %m %d")
#print(now)

import datetime as dt
now = dt.datetime.now().strftime("%Y %m %d")
print(now)