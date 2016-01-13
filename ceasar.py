really = "13903477097402136101535360937141306306215144344084257"

def enLarge(deally):
    key = "abcdeginortuy "
    jointy = str(hex(int(deally)))[2:-1]
    i=0
    x=""
    for i in range(0,len(jointy)-1):
        z = i%2
        if z ==0:
            x+=str("0x"+jointy[i:i+2])             
    print (x)
    
if __name__ == "__main__":
    enLarge(really)