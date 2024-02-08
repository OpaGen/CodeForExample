'''
Инкапсуляция — ограничение доступа к составляющим
объект компонентам (методам и переменным)
'''

class I:
    def g(self):
        print('HI')
    def _private(self):
        print('Это приватный метод, но ты его видишь')
    def __private(self):
        print('Это приватный метод, и ты его не увидишь ААХХХА')

i = I()
print(i._private())
#print(i.__private())

#Наследование

class N(I):
    def _private(self):
        print('Это приватный метод, измененный через наследование')

n = N()
print(n.g() , n._private())


#полиморфизм
print (1+1 , '1'+'1')
