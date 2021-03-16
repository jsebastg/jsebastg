import time
import yfinance as yf
import pandas as pd
import pickle
import multiprocessing
from datetime import datetime
from datetime import timedelta
from DWX_ZeroMQ_Connector_v2_0_1_RC8 import DWX_ZeroMQ_Connector
from yahoo_fin import stock_info as si
from threading import Thread
from os import listdir

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


class StockData:
    pass

    def __init__():
        pass


def get_ticker_names():
    # print("RUN : get_ticker_names")
    with open("dictStocks2Prices.txt", "rb") as stocks_names:
        stocks_names = pickle.load(stocks_names)
        # print(b)
        listafinal = [i[:2] for i in stocks_names if i[2] == i[2]]
        return listafinal


def get_hist_data_yf(ticker):
    # print("RUN : get_hist_data_yf")
    ticker = yf.Ticker(ticker)
    d = ticker.history(period="1mo", interval="5m", rounding=True)
    dataMonth = d.drop(columns=["Dividends", "Stock Splits"])
    return dataMonth


def resistencias(ticker):
    # print("RUN : resistencias")
    data = read_from_dataBase(ticker)
    # data2 = get_hist_data_yf(ticker) #get data from yahoo
    list_of_volumes = data["Volume"].to_list()
    promedio = round(sum(list_of_volumes) / len(list_of_volumes))
    tup = data.to_records()
    l = list(tup)
    # selecciono datos que superan el promedio + 10 %
    promedio_aum = promedio * 1.1
    dia = []
    resistencias = []

    for d in range(31):
        for i in l:
            # si la quinta columna (volumen) es mayor al promedio + 10% y dia 30, 29, 28 etc no es igual a -1
            if (
                i[5] > promedio_aum
                and str(i[0]).find(
                    str(datetime.utcnow().date() - timedelta(days=30 - d))
                )
                != -1
            ):
                dia.append(i)

        opn = [i[1] for i in dia]
        high = [i[2] for i in dia]
        low = [i[3] for i in dia]
        close = [i[4] for i in dia]

        if len(opn) == 0:
            continue
        promopn = sum(opn) / len(opn)
        promhigh = sum(high) / len(high)
        promlow = sum(low) / len(low)
        promclose = sum(close) / len(close)

        promedio_del_dia = (promclose + promopn + promlow + promhigh) / 4
        # print(promedio_del_dia)
        resistencias.append(round(promedio_del_dia, 2))
        dia = []
    return resistencias


class DataBase:
    pass

    def __init__():
        pass


def write_to_dataBase(ticker):
    # print("RUN : write_to_dataBase")
    data = get_hist_data_yf(ticker)
    with open("DataBase/{}.txt".format(ticker), "wb") as stock:
        pickle.dump(data.iloc[1:-1, :], stock)


def read_from_dataBase(ticker):
    # print("RUN : read_from_dataBase")
    # print(f"reading from: {ticker}")
    with open("DataBase/{}.txt".format(ticker), "rb") as stock:
        data = pickle.load(stock)
        return data


def read_all_dataBase():
    # print("RUN : read_all_dataBase")
    # print(f"reading from: {ticker}")
    tickers = get_ticker_names()
    for tick in tickers:
        with open("DataBase/{}.txt".format(tick[1]), "rb") as stock:
            data = pickle.load(stock)
            print(tick[0], len(data))


def create_DB():
    # print("RUN : create_DB")
    print(
        "---------------------------- RESTORING DATA BASE(aprox 2min)------------------------------"
    )
    tickers = get_ticker_names()
    for i in tickers:
        write_to_dataBase(i[1])


def update_dB(ticker):
    # print("RUN : update_dB")
    actual_data = read_from_dataBase(ticker)
    last_date_database = actual_data.index[-1]
    last_date = last_date_database.timestamp()
    ticker_yf = yf.Ticker(ticker)
    now = datetime.now()
    now_unix = round(now.timestamp(), 1)

    d = ticker_yf.history(
        start=datetime.fromtimestamp(last_date), interval="5m", rounding=True
    )
    datatoday = d.iloc[1:-1, :5]
    lenghtdata = len(datatoday)
    if lenghtdata != 0:
        print(f"Updating  {ticker}  with {lenghtdata}row(s)")
        updated = actual_data.append(datatoday)
        updated2 = updated.iloc[lenghtdata:]
        with open("DataBase/{}.txt".format(ticker), "wb") as stock:
            updated = pickle.dump(updated2, stock)
        # print("done")
    else:
        print("--------  " + ticker + "   Updated")
        # return updated


def update_all_DB():
    # print("RUN : update_all_DB")
    """lo mismo que create_DB, pero mas rapido, solo recoge datos faltantes"""
    print(
        "---------------------UPDATING DATA BASE (aprox 1min)---------------------------"
    )
    tickers = get_ticker_names()
    for tick in tickers:
        try:
            all_data = update_dB(tick[1])
        except:
            continue

    print("----------------------DATA BASE IS UPDATED----------------------")


class Trader:
    pass

    def __init__():
        pass


def sign_al(ticker):
    # print("RUN : sign_al")
    resist = resistencias(ticker)
    # print(resistencias(ticker))
    ################    señales de compra ##########################
    BUY_shortterm = "BULL" if resist[-2] > resist[-3] > resist[-4] else "none"
    BUY_mediumterm = (
        "BULL"
        if resist[-2] > resist[int(len(resist) / 4 * 3)] > resist[int(len(resist) / 2)]
        else "none"
    )
    BUY_longterm = (
        "BULL" if resist[-2] > resist[int(len(resist) / 2)] > resist[1] else "none"
    )
    print(BUY_shortterm, BUY_mediumterm, BUY_longterm)
    #################    señales de venta  ############################
    SELL_shortterm = "BEAR" if resist[-2] < resist[-3] < resist[-4] else "none"
    SELL_mediumterm = (
        "BEAR"
        if resist[-2] < resist[int(len(resist) / 4 * 3)] < resist[int(len(resist) / 2)]
        else "none"
    )
    SELL_longterm = (
        "BEAR" if resist[-2] < resist[int(len(resist) / 2)] < resist[1] else "none"
    )
    print(SELL_shortterm, SELL_mediumterm, SELL_longterm)
    ##### señal de confirmacion #####
    # print(longterm == mediumterm == shortterm)
    if BUY_longterm == BUY_mediumterm == BUY_shortterm == "BULL":
        return "BULL"

    elif SELL_longterm == SELL_mediumterm == SELL_shortterm == "BEAR":
        return "BEAR"

    else:
        return "+++"


def volatilidad(ticker):
    # print("RUN : volatilidad")
    resist = resistencias(ticker)
    resist_historic = resist[:-1]
    # encuentro la resta entre elementos de resist_historic y los rendondeo a 3
    volat = [
        round(abs(j - i), 3) for i, j in zip(resist_historic[:-1], resist_historic[1:])
    ]
    promediovolat = sum(volat) / len(volat)
    return int(promediovolat * 100)  # ajustado a puntos


def get_currentprice(ticker):
    # print("RUN : get_currentprice")
    return round(si.get_live_price(ticker), 2)


def set_Lots_SL_TP(ticker):
    # print("RUN : set_Lots_SL_TP")
    cuenta = 100
    riesgo_trade = 0.02
    perdida = cuenta * riesgo_trade  # cuanto estoy dispuesto a perder por trade
    lote_base = 0.01

    if (
        volatilidad(ticker) <= 20
    ):  # si el precio actual - el de la orden es menor a 20 puntos, ejecutar orden al mercado
        Correccion_volat = 21
    else:
        Correccion_volat = volatilidad(ticker)

    stoploss = Correccion_volat * 2
    takeprofit = Correccion_volat
    lotes = perdida / stoploss
    auth = lotes > lote_base  ####revisar

    return round(lotes, 2), stoploss, takeprofit, auth


def set_orderType_price(ticker, order):
    # print("RUN : set_orderType_price")
    resis = resistencias(ticker)
    if order == "BULL":
        if (get_currentprice(ticker) - resis[-3]) < 0.25:
            return 0, 0
        else:
            return 2, resis[-3]

    else:
        if (resis[-3] - get_currentprice(ticker)) < 0.25:
            return 1, 0
        else:
            return 3, resis[-2]


def process_order(ticker):
    # print("RUN : process_order")

    order = {
        "_action": "OPEN",
        "_type": 0,
        "_symbol": "ALGOx",
        "_price": 0.0,
        "_SL": 50,  # SL/TP in POINTS, not pips.
        "_TP": 25,
        "_comment": "nothing",
        "_lots": 0.01,
        "_magic": 123456,
        "_ticket": 0,
    }

    auth = set_Lots_SL_TP(ticker[1])
    print(f"{ticker} Authorized = {auth[3]}")
    if auth[3] == True:
        tpric = set_orderType_price(ticker[1], ticker[2])
        order["_type"] = tpric[0]
        order["_symbol"] = ticker[0]
        order["_price"] = tpric[1]
        order["_SL"] = auth[1]
        order["_TP"] = auth[2]
        order["_lots"] = auth[0]
        order["_ticket"] = int("".join(str(ord(i)) for i in ticker[1]))
        order["_comment"] = ticker[1]
        print(order)
        return order
    else:
        pass


def list_of_orders():
    # print("RUN : list_of_orders")
    tickers = get_ticker_names()
    orders = []
    orders_dict = []
    magic = []

    for tick in tickers:
        try:
            sign = sign_al(tick[1])
            # print(sign)
            if sign != "+++":
                orders.append((tick[0], tick[1], sign))
                print(
                    f"{tick[1]}"
                    + "".join(["-" for _ in range(20 - len(tick[1]))])
                    + f"{sign}"
                )
                order = process_order(orders[-1])
                if type(order) == dict:
                    magic.append(order["_ticket"])
                    orders_dict.append(order)
            else:
                print(
                    f"{tick[1]}"
                    + "".join(["-" for _ in range(20 - len(tick[1]))])
                    + f"{sign}"
                )
        except:
            continue

    print(orders_dict)
    print(magic)
    return orders_dict, magic


def write_to_log(ticket):
    # print("RUN : write_to_log")
    with open("log.txt", "wb") as log:
        pickle.dump(ticket, log)


def write_to_log2(ticket):
    # print("RUN : write_to_log")
    with open("log2.txt", "wb") as log:
        pickle.dump(ticket, log)


def read_from_log():
    # print("RUN : read_from_log")
    with open("log.txt", "rb") as log:
        ticket = pickle.load(log)
        return ticket


def _RUN_():
    """
    p1 = multiprocessing.Process(target = update_all_DB)
    p2 = multiprocessing.Process(target = list_of_orders)
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    """
    update_all_DB()
    orders, magic = list_of_orders()
    zm = DWX_ZeroMQ_Connector()
    print(
        "------------------------------LOAD EXPERT ADVISOR (10 seconds)--------------------------------"
    )
    time.sleep(10)
    print(
        "----------------------------- Closing Obsolete Trades-----------------------------"
    )
    for m in magic:
        if m not in read_from_log():
            try:
                pass
                # zm._DWX_MTX_CLOSE_TRADES_BY_MAGIC_(m)
            except:
                continue
    write_to_log(magic)

    for order in orders:
        try:
            zm._DWX_MTX_NEW_TRADE_(order)
            time.sleep(1)
        except:
            continue


if __name__ == "__main__":

    # update_all_DB()
    # create_DB()
    # _RUN_()
    # get_ticker_names()
    print(read_from_dataBase("amzn"))
    print(read_from_log())
