import datetime
import time
import requests 
import pandas as pd
#fecha inicial 01 de septiembre convertido a unix time en segundos 
fechaInicial = 1630468800
vela = 60 #tiempo en segundos para las velas 
orig = datetime.datetime.fromtimestamp(fechaInicial)
#le agrego 1 dia 
newTime = orig + datetime.timedelta(days=1)
newTime = str(newTime.timestamp())
#le quito dos caracteres porque queda asi 1630468800.0 entonces para dejarlo normal 
newTime = newTime[:-2]

startTime = 1630468800
endTime = newTime
contador = 0
#dias q quiero extraer 
diasTotales = 30
while contador==0:
    request = requests.get(f'https://ftx.com/api/markets/BTC/USD/candles?resolution={vela}&start_time={startTime}&end_time={endTime}', stream=True)
    if request.status_code == 200:
        print("continuamos")
        data = request.json()
        if "error" in data: 
            print("error")
        else: 
            print("iniciamos en: ",time.ctime())
            arrayData = []
            #le quito 1 al total de registros para q el contador me funcione como quiero
            totalData = len(data["result"]) -1
            i=0
            for x in data["result"]: 
                if i == totalData:
                    #aqui como el ultimo registro se repite con el primero del siguiente entonces no lo coloco
                    break
                arrayData.append({
                    "startTime": x["startTime"],
                    "time": x["time"],
                    "open": x["open"],
                    "high": x["high"],
                    "low": x["low"],
                    "close": x["close"],
                    "volume": x["volume"]
                })
                i=i+1
            #eth_df = pd.DataFrame(arrayData, columns=['startTime', 'time', 'open', 'high', 'low', 'close', 'volume'  ])
            eth_df = pd.DataFrame(arrayData)
            eth_df.to_csv('finex.csv', header=None, mode='a' )
            #despues de guardar resto 1 dia a los dias totales
            diasTotales = diasTotales - 1
            if diasTotales > 0:
                #mientras no lleguen a cero le coloco la nueva fecha q seria la endtime como start_time y 
                # agrego un dia al start time para ponerlo de end time 
                orig = datetime.datetime.fromtimestamp(int(endTime))
                newTime = orig + datetime.timedelta(days=1)
                newTime = str(newTime.timestamp())
                newTime = newTime[:-2]
                startTime = endTime
                endTime = newTime
            else:
                #si llego a cero los dias totales ya no necesito el ciclo while lo paso a 1 para terminarlo 
                contador = 1
    else: 
        print("error ,", request.status_code)

#aqui guardo mi csv con un header de columnas y elimino la primera columna q es la de los ids q no necesito
df = pd.read_csv('finex.csv', header=None, index_col=None)
df.columns = ['in', 'startTime', 'time', 'open', 'high', 'low', 'close', 'volume'  ]
df.set_index('startTime')
del df["in"]
df.to_csv('btcusd1sep.csv', index=False) 