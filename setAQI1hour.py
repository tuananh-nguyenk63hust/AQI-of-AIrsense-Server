import mysql.connector as Connection
import calendar
import time
class AQI:
    I_low=[0,51,101,151,201,301,401]
    I_high=[50,100,150,200,300,400,500]
    C_low=[0.0,12.1,35.5,55.5,150.5,250.5,350.5]
    C_high=[12.0,35.4,55.4,150.4,250.4,350.4,500.4]
#TimewaitBefore=calendar.timegm(time.gmtime())
#TimewaitLate=calendar.timegm(time.gmtime())
#if TimewaitLate-TimewaitBefore<3200:
#    time.sleep(3200)
#    TimewaitLate=calendar.timegm(time.gmtime())
conn=Connection.connect(user='sparclab',password='sparclab1',host='localhost',db='Airsense')
cur=conn.cursor()
query="SELECT * FROM Data WHERE Time BETWEEN %s AND %s"
#timenow=calendar.timegm(time.gmtime())
timenow=1568874233
print(timenow)
cur.execute(query,(timenow-3600, timenow))
val=cur.fetchall()
lengrow=0
DUST_AVERAGE=[]
VALUE_DUST_AVERAGE=[]
ts=calendar.timegm(time.gmtime())
FirstArray=10000000
EndArray=99999999
for i in range(FirstArray,EndArray):
    DUST_AVERAGE.append(0)
    VALUE_DUST_AVERAGE.append(0)
for row in val:
    rowint=int(row[1])
    DUST_AVERAGE[rowint-FirstArray]=DUST_AVERAGE[rowint-FirstArray]*(VALUE_DUST_AVERAGE[rowint-FirstArray]/(VALUE_DUST_AVERAGE[rowint-FirstArray]+1))+int(row[3])/(VALUE_DUST_AVERAGE[rowint-FirstArray]+1)
    VALUE_DUST_AVERAGE[rowint-FirstArray]+=1
setAQI=AQI()
queryAQI="INSERT INTO AQI(NodeId,Time,AQI) VALUES (%(NodeId)s,%(Time)s,%(AQI)s)"
for i in range(FirstArray,EndArray):
        if DUST_AVERAGE[i-FirstArray]!=0:
            print(i)
            j=0
            for j in range(0,7):
                if (setAQI.C_low[j]<=DUST_AVERAGE[i-FirstArray]) and (setAQI.C_high[j]>=DUST_AVERAGE[i-FirstArray]):
                    DOUBLE_AQI=((setAQI.I_high[j]-setAQI.I_low[j])/(setAQI.C_high[j]-setAQI.C_low[j]))*(DUST_AVERAGE[i-FirstArray]-setAQI.C_low[j]) +setAQI.I_low[j]
                    SQL_AQI={
                        'NodeId':i,
                        'Time':timenow,
                        'AQI':DOUBLE_AQI,
                    }
                    print(DOUBLE_AQI)
                    cur.execute(queryAQI,SQL_AQI)
conn.commit()
#time.sleep(10)
print("Done!")