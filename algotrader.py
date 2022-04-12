from fileinput import close
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import pandas as pd
import time
import pytz
import schedule
import talib as ta
import numpy as np

def connect(account):
    account = int(account)
    mt5.initialize(login=7775278, password="W3HpdLsn", server="FBS-Demo")
    authorized=mt5.login(account, server="FBS-Demo")

    if authorized:
        print("Conectado: Conectando no MetaTrader 5.")
    else:
        print("Falha ao se conectar a conta #{}, código de erro: {}"
              .format(account, mt5.last_error()))

def open_position(pair, order_type, size, tp_distance=None, stop_distance=None):
    symbol_info = mt5.symbol_info(pair)
    if symbol_info is None:
        print(pair, "não encontrado.")
        return

    if not symbol_info.visible:
        print(pair, "não está visível, tente mudar para on.")
        if not mt5.symbol_select(pair, True):
            print("symbol_select({}}) falhou, sair.",pair)
            return
    print(pair, "encontrado!")

    point = symbol_info.point

    
    if(order_type == "BUY"):
        print("Tipo de Ordem:", order_type)
        order = mt5.ORDER_TYPE_BUY
        price = mt5.symbol_info_tick(pair).ask
        if(stop_distance):
            sl = price - (stop_distance * point)
        if(tp_distance):
            tp = price + (tp_distance * point)

            
    if(order_type == "SELL"):
        print("Tipo de Ordem: SELL")
        order = mt5.ORDER_TYPE_SELL
        price = mt5.symbol_info_tick(pair).bid
        if(stop_distance):
            sl = price + (stop_distance * point)
        if(tp_distance):
            tp = price - (tp_distance * point)

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": pair,
        "volume": float(size),
        "type": order,
        "price": price,
        "sl": sl,
        "tp": tp,
        "magic": 234000,
        "comment": "Python Script Opened.",
        "deviation":  20,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    print("Tentativa de envio de ordem...")
    print(result.retcode)

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print("Falha ao enviar a ordem :(")
    else:
        print ("Ordem enviada com sucesso!")

def positions_get(symbol=None):
    if(symbol is None):
        res = mt5.positions_get()
    else:
        res = mt5.positions_get(symbol=symbol)

    if(res is not None and res != ()):
        df = pd.DataFrame(list(res),columns=res[0]._asdict().keys())
        df['time'] = pd.to_datetime(df['time'], unit='s')
        print(df)
        return df
    
    return pd.DataFrame()

def close_position(deal_id):
    open_positions = positions_get()
    open_positions = open_positions[open_positions['ticket'] == deal_id]
    order_type  = open_positions["type"][0]
    symbol = open_positions['symbol'][0]
    volume = open_positions['volume'][0]

    if(order_type == mt5.ORDER_TYPE_BUY):
        order_type = mt5.ORDER_TYPE_SELL
        price = mt5.symbol_info_tick(symbol).bid
    else:
        order_type = mt5.ORDER_TYPE_BUY
        price = mt5.symbol_info_tick(symbol).ask
	
    close_request={
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(volume),
        "type": order_type,
        "position": deal_id,
        "price": price,
        "magic": 234000,
        "comment": "Encerrar trade",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(close_request)
    
    if result.retcode != mt5.TRADE_RETCODE_PLACED:
        print("Falha ao encerrar a ordem :(")
    else:
        print ("Ordem encerrada com sucesso!")

def close_positions_by_symbol(symbol):
    open_positions = positions_get(symbol)
    open_positions['ticket'].apply(lambda x: close_position(x))

def get_data(time_frame):
    pairs = ['EURUSD']
    pair_data = dict()
    for pair in pairs:
        utc_from = datetime(2021, 1, 1, tzinfo=pytz.timezone('America/Sao_Paulo'))
        date_to = datetime.now().astimezone(pytz.timezone('America/Sao_Paulo'))
        date_to = datetime(date_to.year, date_to.month, date_to.day, hour=date_to.hour, minute=date_to.minute)
        rates = mt5.copy_rates_range(pair, time_frame, utc_from, date_to)
        rates_frame = pd.DataFrame(rates)
     
        print(rates_frame)
        rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')
        rates_frame.drop(rates_frame.tail(1).index, inplace = True)
        pair_data[pair] = rates_frame
    return pair_data
    



def live_trading():
    schedule.every().hour.at(":00").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":05").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":10").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":15").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":20").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":25").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":30").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":35").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":40").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":45").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":50").do(run_trader, mt5.TIMEFRAME_M5)
    schedule.every().hour.at(":55").do(run_trader, mt5.TIMEFRAME_M5)

    while True:
        schedule.run_pending()
        time.sleep(1)

def check_trades(time_frame, pair_data, max_holding=None):
    for pair, data in pair_data.items():

        up, mid, low = ta.BBANDS(data['close'], timeperiod=20, nbdevup=2, nbdevdn = 2, matype=0)
        bbp = (data['close'] - low) / (up - low)
        data['BBANDS_UP'] = up
        data['BBANDS_MID'] = mid
        data['BBANDS_LOW'] = low
        data['RSI'] = ta.RSI(data['close'], timeperiod=14)

        last_row = data.tail(1)

        print(last_row)

        open_positions = positions_get()

        current_dt = datetime.now().astimezone(pytz.timezone('America/Sao_Paulo'))

        for index, position in open_positions.iterrows():
            # Check to see if the trade has exceeded the time limit
            trade_open_dt = position['time'].replace(tzinfo = pytz.timezone('America/Sao_Paulo'))
            deal_id = position['ticket']
            if(current_dt - trade_open_dt >= timedelta(hours = 2)):
                close_position(deal_id)

        current_dt = datetime.now().astimezone(pytz.timezone('America/Sao_Paulo'))

        #Condition for trade order
        if (last_row['close'].any() >= last_row['BBANDS_UP'].any() and last_row['RSI'].any() > 57):
            open_position(pair, "SELL", 1, 200, 100)
        elif (last_row['close'].any() <= last_row['BBANDS_LOW'].any() and last_row['RSI'].any() < 37):
            open_position(pair, "BUY", 1, 200, 100)
        else:
            print("Com base nos parâmetros do BOT, no momento, sem oportunidade de compra.")


def run_trader(time_frame):
    print("Executando Bot Trader as", datetime.now())
    connect(7775278)
    pair_data = get_data(time_frame)
    check_trades(time_frame, pair_data)

if __name__ == '__main__':
    print("Iniciando o programa...")
    live_trading()
