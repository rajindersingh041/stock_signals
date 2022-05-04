import logging
import json
import datetime 
from alice_blue import *
from datetime import datetime, date, timedelta
from time import sleep
import sys
import pandas as pd
import pandas_ta as pta
import numpy as np
import os
from pytz import timezone
import psutil

# gives a single float value


def run_alice_blue(filepath):
    with open(filepath) as f:
        d = json.load(f)
        aliceUser = d['ALICEUSERNAME']
        alicePass = d['ALICEPASS']
        alice2FA = d['ALICE2FA']
        aliceAPI = d['ALICEAPI']
        aliceAPPID = d['ALICEAPPID']

    access_token = AliceBlue.login_and_get_access_token(username=aliceUser,
                                                        password=alicePass, 
                                                        twoFA=alice2FA,
                                                        api_secret=aliceAPI,
                                                        app_id=aliceAPPID)

    alice = AliceBlue(username=aliceUser, password=alicePass, access_token=access_token, master_contracts_to_download=['NSE','MCX'])

    return alice

def get_current_ist():
    india = timezone('Asia/Kolkata')
    ist = datetime.now(india)
    return ist


#%% Initialize logging facility
def getLogger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    if not log.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('[%(asctime)s] %(name)s [%(levelname)s] %(message)s',datefmt='%Y-%m-%d %H:%M:%S'))
        log.addHandler(handler)
    return log


def abc(alice,myinstrument):
    socket_opened = False


    def event_handler_quote_update(message):
        mySymbol = message['instrument'].symbol
        ltp = float(message['ltp']) 
        et = datetime.fromtimestamp(message['exchange_time_stamp'])
    
        global tickdata
        tickdata['et'] = et
        tickdata['ltp'] = ltp

    def open_callback():
        global socket_opened
        socket_opened = True

    alice.start_websocket(subscribe_callback=event_handler_quote_update,
                        socket_open_callback=open_callback,
                        run_in_background=True)

    alice.subscribe(alice.get_instrument_by_symbol('MCX', myinstrument), LiveFeedType.MARKET_DATA)
    logger.info('Alice Subs')

    sleep(0.5)

    candles_5, candles_15, candles_60  = {}, {}, {}
    candles_5[instrument], candles_15[instrument], candles_60[instrument] = {}, {}, {}

    buy_signal = False
    sell_signal = False

    logger.info('Los gehts')


    oldDf = {}
    myfiles = os.listdir('/home/ubuntu/myIntraday_files/')
    _5minsFile = [x for x in myfiles if "bnf" in x.lower() and "_5mins" in x.lower()]
    _15minsFile = [x for x in myfiles if "bnf" in x.lower() and "_15mins" in x.lower()]
    _60minsFile = [x for x in myfiles if "bnf" in x.lower() and "_60mins" in x.lower()]
    
    if len(_5minsFile) > 0:
        old5mins = pd.read_csv(f'/home/ubuntu/myIntraday_files/{_5minsFile[0]}')
        oldDf['5mins'] = old5mins.set_index('index').T.to_dict()

    if len(_15minsFile) > 0:
        old15mins = pd.read_csv(f'/home/ubuntu/myIntraday_files/{_15minsFile[0]}')
        oldDf['15mins'] = old15mins.set_index('index').T.to_dict()

    if len(_60minsFile) > 0:
        old60mins = pd.read_csv(f'/home/ubuntu/myIntraday_files/{_60minsFile[0]}')
        oldDf['60mins'] = old60mins.set_index('index').T.to_dict()


    while True:
        # if psutil.cpu_percent() > 50:

            #logger.info(f'CPU usage {psutil.cpu_percent()} and used ram pct{psutil.virtual_memory().percent}')

        ltt = get_current_ist()
        # market_start_time = ltt.replace(hour = 0,minute= 15,second = 0, microsecond=0)
        market_start_time = ltt.replace(hour = 9,minute= 15,second = 0, microsecond=0)
        market_close_time = ltt.replace(hour = 21,minute=30 ,second = 30, microsecond=0)

        if ltt >= market_start_time and ltt <= market_close_time:

            ltt_min_5 = datetime(ltt.year,ltt.month,ltt.day,ltt.hour,ltt.minute//1 * 1)
            ltt_min_15 = datetime(ltt.year,ltt.month,ltt.day,ltt.hour,ltt.minute//2 * 2)
            ltt_min_60 = datetime(ltt.year,ltt.month,ltt.day,ltt.hour,ltt.minute//3 * 3) + timedelta(minutes = 15)

            try:
              if ltt_min_5 in candles_5[instrument]:
                candles_5[instrument][ltt_min_5]["high"]=max(candles_5[instrument][ltt_min_5]["high"],tickdata['ltp']) #1
                candles_5[instrument][ltt_min_5]["low"]=min(candles_5[instrument][ltt_min_5]["low"],tickdata['ltp']) #2
                candles_5[instrument][ltt_min_5]["close"]=tickdata['ltp'] #3
          
              else:
                # 5 mins candle
                candles_5[instrument][ltt_min_5]={}
                candles_5[instrument][ltt_min_5]["open"]=tickdata['ltp'] #6
                candles_5[instrument][ltt_min_5]["high"]=tickdata['ltp'] #4
                candles_5[instrument][ltt_min_5]["low"]=tickdata['ltp'] #5
                candles_5[instrument][ltt_min_5]["close"]=tickdata['ltp'] #7
                if "5mins" in oldDf :
                    candles_5[instrument] = {**candles_5[instrument], **oldDf['5mins']}

              if ltt_min_15 in candles_15[instrument]:
                candles_15[instrument][ltt_min_15]["high"]=max(candles_15[instrument][ltt_min_15]["high"],tickdata['ltp']) #1
                candles_15[instrument][ltt_min_15]["low"]=min(candles_15[instrument][ltt_min_15]["low"],tickdata['ltp']) #2
                candles_15[instrument][ltt_min_15]["close"]=tickdata['ltp'] #3
          
              else:
                # 15 mins candle
                candles_15[instrument][ltt_min_15]={}
                candles_15[instrument][ltt_min_15]["open"]=tickdata['ltp'] #6
                candles_15[instrument][ltt_min_15]["high"]=tickdata['ltp'] #4
                candles_15[instrument][ltt_min_15]["low"]=tickdata['ltp'] #5
                candles_15[instrument][ltt_min_15]["close"]=tickdata['ltp'] #7
                if "15mins" in oldDf :
                    candles_15[instrument] = {**candles_15[instrument], **oldDf['15mins']}

              if ltt_min_60 in candles_60[instrument]:
                candles_60[instrument][ltt_min_60]["high"]=max(candles_60[instrument][ltt_min_60]["high"],tickdata['ltp']) #1
                candles_60[instrument][ltt_min_60]["low"]=min(candles_60[instrument][ltt_min_60]["low"],tickdata['ltp']) #2
                candles_60[instrument][ltt_min_60]["close"]=tickdata['ltp'] #3
          
              else:
                # 60 mins candle
                candles_60[instrument][ltt_min_60]={}
                candles_60[instrument][ltt_min_60]["open"]=tickdata['ltp'] #6
                candles_60[instrument][ltt_min_60]["high"]=tickdata['ltp'] #4
                candles_60[instrument][ltt_min_60]["low"]=tickdata['ltp'] #5
                candles_60[instrument][ltt_min_60]["close"]=tickdata['ltp'] #7

                if "60mins" in oldDf :
                    candles_60[instrument] = {**candles_60[instrument], **oldDf['60mins']}
            except:
              logger.error('Issue in tick to candle')
            
            mydf5mins = pd.DataFrame.from_dict(candles_5[instrument]).T.reset_index()
            mydf15mins = pd.DataFrame.from_dict(candles_15[instrument]).T.reset_index()
            mydf60mins = pd.DataFrame.from_dict(candles_60[instrument]).T.reset_index()
            
            # if "5mins" in oldDf and "15mins" in oldDf and "60mins" in oldDf:
            #     mydf5mins = pd.concat([oldDf['5mins'],mydf5mins])
            #     mydf15mins = pd.concat([oldDf['15mins'],mydf15mins])
            #     mydf60mins = pd.concat([oldDf['60mins'],mydf60mins])

            mydf5mins['rsi'] = pta.rsi(mydf5mins['close'],3)
            mydf15mins['rsi'] = pta.rsi(mydf15mins['close'],3)
            mydf60mins['rsi'] = pta.rsi(mydf60mins['close'],3)

            # print(mydf5mins.tail(3))

            if mydf5mins.shape[0] > 2 and mydf15mins.shape[0] > 2 and mydf60mins.shape[0] > 2 and buy_signal == False:
                if mydf5mins.iloc[-2]['rsi'] < 40 and mydf15mins.iloc[-2]['rsi'] >= 60 and mydf60mins.iloc[-2]['rsi'] >= 60:
                    logger.info('Good opportunity to buy')
                    buy_signal = True

            if buy_signal:
                stoploss = mydf5mins.iloc[-2]['close']
                buy_ltp_signal = tickdata['ltp']

                print(stoploss,buy_ltp_signal)
                #order sell

            if sell_signal:
                pass



        if get_current_ist() >= get_current_ist().replace(hour=21,minute=30,second=0, microsecond = 0):
        # if get_current_ist() >= get_current_ist().replace(hour=15,minute=30,second=0, microsecond = 0):
            #check if existing files exist

            mydf5mins.to_csv(f'/home/ubuntu/myIntraday_files/bnf_5mins.csv',index=False)
            mydf15mins.to_csv(f'/home/ubuntu/myIntraday_files/bnf_15mins.csv',index=False)
            mydf60mins.to_csv(f'/home/ubuntu/myIntraday_files/bnf_60mins.csv',index=False)

            # myfiles = os.listdir('/home/ubuntu/myIntraday_files/')
            # _5minsFile = [x for x in myfiles if "bnf" in x.lower() and "_5mins" in x.lower()]
            # _15minsFile = [x for x in myfiles if "bnf" in x.lower() and "_15mins" in x.lower()]
            # _60minsFile = [x for x in myfiles if "bnf" in x.lower() and "_60mins" in x.lower()]

            # if len(_5minsFile) > 0:
            #     myexisting5mins = pd.read_csv(f'/home/ubuntu/myIntraday_files/{_5minsFile[0]}')
            #     myexisting5mins = pd.concat([myexisting5mins, mydf5mins])
            #     myexisting5mins.to_csv(f'/home/ubuntu/myIntraday_files/{_5minsFile[0]}')

            # else:
            #     mydf5mins.reset_index().to_csv(f'/home/ubuntu/myIntraday_files/bnf_5mins.csv',index=False)
            
            # if len(_15minsFile) > 0:
            #     logger.info(_15minsFile[0])
            #     myexisting15mins = pd.read_csv(f'/home/ubuntu/myIntraday_files/{_15minsFile[0]}')
            #     myexisting15mins = pd.concat([myexisting15mins, mydf15mins])
            #     myexisting15mins.to_csv(f'/home/ubuntu/myIntraday_files/{_15minsFile[0]}')

            # else:
            #     mydf15mins.reset_index().to_csv(f'/home/ubuntu/myIntraday_files/bnf_15mins.csv',index=False)


            # if len(_60minsFile) > 0:
            #     logger.info(_60minsFile[0])
            #     myexisting60mins = pd.read_csv(f'/home/ubuntu/myIntraday_files/{_60minsFile[0]}')
            #     myexisting60mins = pd.concat([myexisting60mins, mydf60mins])
            #     myexisting60mins.to_csv(f'/home/ubuntu/myIntraday_files/{_60minsFile[0]}')
            # else:
            #     mydf60mins.reset_index().to_csv(f'/home/ubuntu/myIntraday_files/bnf_60mins.csv',index=False)

            sys.exit()

        sleep(3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = getLogger(__name__)
    instrument = 'CRUDEOIL MAY FUT'
    tickdata = {}
    myfilepath = '/home/ubuntu/mycreds.json'

    myalice = run_alice_blue(myfilepath)
    abc(myalice,instrument)

