import copy

class MyObject(object):
    def __init__(self, id, list1, list2):
        self.id = id
        self.list1 = list1
        self.list2 = list2

    def __copy__(self):
        return MyObject(self.id + 1, list(self.list1), self.list2)

    def __repr__(self):
        return str(self.id) + ': ' + str(self.list1) + ' ' + str(self.list2)



o1 = MyObject(1, ['Paul', 'Jane'], [2, 4])
o2 = copy.copy(o1)

print o1
print o2

o1.list1.append('Alice')
o1.list2.append(5)

o2.list1.append('Dave')
o2.list2.append(6)

print o1
print o2
