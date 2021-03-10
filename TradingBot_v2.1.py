import time
import yfinance as yf
import pandas as pd
import pickle
from datetime import datetime
from datetime import timedelta
from DWX_ZeroMQ_Connector_v2_0_1_RC8 import DWX_ZeroMQ_Connector
from yahoo_fin import stock_info as si
from threading import Thread
from os import listdir

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def get_hist_data_yf(ticker):
	ticker = yf.Ticker(ticker)
	today = str(datetime.now())[8] + str(datetime.now())[8:10]
	d = ticker.history(period="1mo", interval="5m", rounding=True, end=today)
	dataMonth = d.drop(columns=["Dividends", "Stock Splits"])
	return dataMonth

def write_to_dataBase(ticker):
	data = get_hist_data_yf(ticker)
	with open("DataBase/{}.txt".format(ticker), "wb") as stock:
		pickle.dump(data, stock)

def read_from_dataBase(ticker):
	with open("DataBase/{}.txt".format(ticker), "rb") as stock:
		data = pickle.load(stock)
		return data

def get_last_date_DB(ticker):
	database = read_from_dataBase(ticker)
	last_date_database = database.index[-1]
	last_row = database.iloc[-1] #debug
	return last_date_database, last_date_database.timestamp()

def is_new_data(ticker):
	get_last_date_DB(ticker)


def get_updated_dB(ticker):   ###no funciona aun
	ticker_yf = yf.Ticker(ticker)
	now = datetime.now()
	now_unix = now.timestamp()
	actual_data = read_from_dataBase(ticker)

	if get_last_date_DB(ticker)[1] < now_unix:
		d = ticker_yf.history(start = get_last_date_DB(ticker)[0], interval="5m", rounding=True)
		datatoday = d.iloc[1:-1,:5]
		if type(d) != str:
			updated = actual_data.append(datatoday)
			return updated
		else:
			return None
	else:
		return None
	

def get_ticker_names():
	with open("dictStocks2Prices.txt", "rb") as stocks_names:
		stocks_names = pickle.load(stocks_names)
		#print(b)
		listafinal = []
		for i in stocks_names:
			#print(i[2])
			if i[2] == i[2]: # filters out NaN values
				listafinal.append(i[:2])

		return listafinal		

def create_DB():
	tickers = get_ticker_names()
	for i in tickers:
		write_to_dataBase(i[1])

def write_updated_DB(ticker): 
	data = get_updated_dB(ticker)
	if data != None:
		with open("DataBase/{}.txt".format(ticker), "wb") as stock:
			updated = pickle.dump(data, stock)
			return updated
	else:
		return None


def resistencias(ticker):
	data = read_from_dataBase(ticker)[1:-1]
	#data2 = get_hist_data_yf(ticker) #get data from yahoo
	list_of_volumes = (data['Volume'].to_list())
	promedio = round(sum(list_of_volumes) / len(list_of_volumes))
	tup = data.to_records()
	l = list(tup)
	#selecciono datos que superan el promedio + 10 %
	promedio_aum = promedio * 1.1
	dia = []
	resistencias = []

	for d in range(31):
		for i in l:
			# si la quinta columna (volumen) es mayor al promedio + 10% y dia 30, 29, 28 etc no es igual a -1
			if i[5] > promedio_aum and str(i[0]).find(str(datetime.utcnow().date() - timedelta(days = 30 - d))) != -1:
				dia.append(i)

		opn = [i[1] for i in dia]
		high = [i[2] for i in dia]
		low = [i[3] for i in dia]
		close = [i[4] for i in dia]

		if len(opn) == 0: continue
		promopn = sum(opn) / len(opn)
		promhigh = sum(high) / len(high)
		promlow = sum(low) / len(low)
		promclose = sum(close) / len(close)

		promedio_del_dia = (promclose + promopn + promlow + promhigh) / 4
		#print(promedio_del_dia)
		resistencias.append(round(promedio_del_dia, 2))
		dia = []
	return resistencias

def sign_al(ticker):
	resist = resistencias(ticker)

	#print(resistencias(ticker))
################    señales de compra ##########################
	shortterm = "BUll" if resist[-2] > resist[-3] > resist[-4] else "none"
	mediumterm = "BUll" if resist[-2] > resist[int(len(resist)/4*3)] > resist[int(len(resist)/2)] else "none"
	longterm = "BUll" if resist[-2] > resist[int(len(resist)/2)] > resist[1] else "none"

#################    señales de venta  ############################
	shortterm = "BEAR" if resist[-2] < resist[-3] < resist[-4] else "none"
	mediumterm = "BEAR" if resist[-2] < resist[int(len(resist)/4*3)] < resist[int(len(resist)/2)] else "none"
	longterm = "BEAR" if resist[-2] < resist[int(len(resist)/2)] < resist[1] else "none"

##### señal de confirmacion #####
	if longterm == mediumterm == shortterm == "BULL":
		return "BULL"	
	elif longterm == mediumterm == shortterm == "BEAR":
		return "BEAR"	
	else:
		return "+++"

def volatilidad(ticker):

	resist = resistencias(ticker)
	resist_historic = resist[:-1]
	# encuentro la resta entre elementos de resist_historic y los rendondeo a 3
	volat = [round(abs(j-i), 3) for i, j in zip(resist_historic[:-1], resist_historic[1:])]
	promediovolat = sum(volat) / len(volat)
	return(int(promediovolat * 100))# ajustado a puntos		

def get_currentprice(ticker):
	
	return round(si.get_live_price(ticker), 2)

def set_Lots_SL_TP(ticker):
	cuenta = 100
	riesgo_trade = 0.02
	perdida = cuenta * riesgo_trade # cuanto estoy dispuesto a perder por trade
	lote_base = 0.01

	if volatilidad(ticker) <= 20: # si el precio actual - el de la orden es menor a 20 puntos, ejecutar orden al mercado
		Correccion_volat = 21
	else:
		Correccion_volat = volatilidad(ticker)

	stoploss = Correccion_volat * 2
	takeprofit = Correccion_volat
	lotes = perdida / stoploss
	auth = lotes > lote_base ####revisar

	return  round(lotes, 2), stoploss, takeprofit, auth
def set_orderType_price(ticker, order):
	if order == "BULL":
		if (get_currentprice(ticker) - resistencias(ticker)[-3]) < 25:
			return 0, 0	
		else:
			return 2, resistencias(ticker)[-3]

	else:
		if (resistencias(ticker)[-3] - get_currentprice(ticker)) < 0.25:
			return [1, 0]
		else:
			return 3, resistencias(ticker)[-3]

def read_all_DB():
	tickers = get_ticker_names()
	for tick in tickers:
		all_data = read_from_dataBase(tick[1])
	return all_data

def update_all_DB():
	"""lo mismo que create_DB, pero mas rapido, solo recoge datos faltantes"""
	print("---------------------UPDATING DATA BASE---------------------------")
	tickers = get_ticker_names()
	for tick in tickers:
		try:
			all_data = write_updated_DB(tick[1])
		except:
			print("----------------------DATA BASE IS UPDATED----------------------")
			break

def process_order(ticker):

	order = {'_action': 'OPEN',
	                  '_type': 0,
	                  '_symbol': "ALGOx",
	                  '_price': 0.0,
	                  '_SL': 50, # SL/TP in POINTS, not pips.
	                  '_TP': 25,
	                  '_comment': "nothing",
	                  '_lots': 0.01,
	                  '_magic': 123456,
	                  '_ticket': 0}


	print(f"{ticker} Authorized = {set_Lots_SL_TP(ticker[1])[3]}")
	if set_Lots_SL_TP(ticker[1])[3] == True:
		order["_type"] = set_orderType_price(ticker[1], ticker[2])[0]
		order["_symbol"] = ticker[0]
		order["_price"] = set_orderType_price(ticker[1], ticker[2])[1]
		order["_SL"] = set_Lots_SL_TP(ticker[1])[1]
		order["_TP"] = set_Lots_SL_TP(ticker[1])[2]
		order["_lots"] = set_Lots_SL_TP(ticker[1])[0]
		return order
	else:
		pass

def list_of_orders():
	tickers = get_ticker_names()
	orders = []
	orders_dict = []
	
	for tick in tickers:
			try:
				
				#p = executor.submit(sign_al, tick[1])
				sign = sign_al(tick[1])
				executor = concurrent.futures.ProcessPoolExecutor()
				if sign != "+++":
					orders.append((tick[0], tick[1], sign))
					print(f"{tick[1]}" + "".join(["-" for _ in range(20-len(tick[1]))]) + f"{sign}")
					#p1 = executor.submit(process_order, orders[-1])
					#orders_dict.append(p1.result())
					#order = executor.map(process_order, orders[-1])
					order = process_order(orders[-1])
					if type(order) == dict:
						orders_dict.append(order)
				else:
					print(f"{tick[1]}" + "".join(["-" for _ in range(20-len(tick[1]))]) + f"{sign}")
			except:
				continue
	#print(orders)
	print(orders_dict)
	return orders_dict
		
	#download_Data.start()

#print(resistencias("amzn"))
#print(sign_al("amzn"))

def _RUN_():
	try:
		update_all_DB()
	except:
		pass
	orders = list_of_orders()
	zm = DWX_ZeroMQ_Connector()
	print("------------------------------LOAD EXPERT ADVISOR (10 seconds)--------------------------------")
	time.sleep(15)
	for order in orders:
		try:
			zm._DWX_MTX_NEW_TRADE_(order)
			time.sleep(1)
		except:
			continue

if __name__ == '__main__':
	#create_DB()
	_RUN_()
	#create_DB()
	#update_all_DB()
	print(get_last_date_DB("amzn"))
	print(read_from_dataBase("amzn"))