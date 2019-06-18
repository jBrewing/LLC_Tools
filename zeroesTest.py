
import pandas as pd

numberList=pd.Series([0,0,0,3,0,0,0,0,0,0,0,0,2,0,0,0,0,0, 0, 7])

x=0
start = 0
end = 0


numberList2 = pd.Series([20,12,13,14,15,16,17,18])
numberList2[0:8] = 1


test=[]
for i, num in enumerate(numberList):
    if num !=0:
        test[i]=x
        x = 0
    else:
        x = x+1





#for i, num in enumerate(numberList):
#    if num != 0:
#        start = i

#        if x < 5:
#            break
#        else:
#            numberList[start:end] = 1 / (x + 1)

#    else:
#        x = x+1
#        end = start + 1

#print(numberList)


#start = 0
#end = 0
#x = 0
#for i, num in enumerate(numberList):
 #   if num == 0:
#        x = x+1
#        end = i+1
#    else



print('done')
