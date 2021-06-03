import websocket
import json
import pandas
import time
try:
    import thread
except ImportError:
    import _thread as thread


pandas.set_option('display.width', 1400)
pandas.set_option('display.max_columns', None)


def on_open(ws):
    def run(*args):
        params = {
            "method": "SUBSCRIBE",
            "params": [
                "btcusdt@depth20@100ms",
                "btcusdt@trade"
                ],
            "id": 1
            }
        ws.send(json.dumps(params))
    thread.start_new_thread(run, ())


def on_message(ws, response):
    global binance_wrapper
    response = json.loads(response)
    binance_wrapper.receive_and_distribute_responses_to_handlers(ws, response)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


class binance_wrapper_class:
    # At Start: Establish connection with the Binance-websocket and datafeed
    # Then: Receives and handles responses from the Bitmex-websocket -> stores bids and asks in lists and the
    #   best bid and best ask (price and size)

    def __init__(self, pandas):
        self.pd = pandas
        self.timestamp_orderbook_update = None  # initVarFirstTime
        self.bid_list = None  # initVarFirstTime
        self.ask_list = None  # initVarFirstTime

        self.timestamp_recent_trade = None  # initVarFirstTime
        self.traded_price_buyer_is_maker = 0  # initVarFirstTime
        self.traded_quantity_buyer_is_maker = 0  # initVarFirstTime
        self.traded_price_buyer_is_taker = 0  # initVarFirstTime
        self.traded_quantity_buyer_is_taker = 0  # initVarFirstTime

        self.df = self.pd.DataFrame()

        self.start_time = time.time()  # initVarFirstTime

    def receive_and_distribute_responses_to_handlers(self, ws, response):
        try:
            if response["e"] == "depthUpdate":
                self.fill_orderbook_lists(response)

            elif response["e"] == "trade":
                self.fill_recent_trades_variables(response)

            if self.bid_list is not None and self.ask_list is not None:
                self.append_new_data_to_dataframe()

            if time.time()-self.start_time > 10:
                self.start_time = time.time()
                self.df.to_json("binance_data_captured2.json")
                print(binance_wrapper.df)

        except:
            try:
                if response["id"] == 1 and response["result"] is None:
                    print("--- Subscribed, everything good ---", response)
                else:
                    ws.close()
            except:
                ws.close()

    def fill_orderbook_lists(self, response):
        self.timestamp_orderbook_update = float(response["T"])
        self.bid_list = self.pd.DataFrame(response["b"]).apply(self.pd.to_numeric)
        self.ask_list = self.pd.DataFrame(response["a"]).apply(self.pd.to_numeric)

    def fill_recent_trades_variables(self, response):
        self.traded_price_buyer_is_maker = 0
        self.traded_quantity_buyer_is_maker = 0
        self.traded_price_buyer_is_taker = 0
        self.traded_quantity_buyer_is_taker = 0
        self.timestamp_recent_trade = float(response["T"])
        if response["m"]:
            self.traded_price_buyer_is_maker = float(response["p"])
            self.traded_quantity_buyer_is_maker = float(response["q"])
        elif not response["m"]:
            self.traded_price_buyer_is_taker = float(response["p"])
            self.traded_quantity_buyer_is_taker = float(response["q"])

    def append_new_data_to_dataframe(self):

        self.df = self.df.append([{"time": time.time(),
                                   "timestamp_recent_trades": self.timestamp_recent_trade,
                                   "timestamp_orderbook_update": self.timestamp_orderbook_update,
                                   "traded_price_buyer_is_maker": self.traded_price_buyer_is_maker,
                                   "traded_quantity_buyer_is_maker": self.traded_quantity_buyer_is_maker,
                                   "traded_price_buyer_is_taker": self.traded_price_buyer_is_taker,
                                   "traded_quantity_buyer_is_taker": self.traded_quantity_buyer_is_taker,
                                   "bid_price1": float(self.bid_list[0][0]), "ask_price1": float(self.ask_list[0][0]),
                                   "bid_size1": float(self.bid_list[1][0]), "ask_size1": float(self.ask_list[1][0]),
                                   "bid_price2": float(self.bid_list[0][1]), "ask_price2": float(self.ask_list[0][1]),
                                   "bid_size2": float(self.bid_list[1][1]), "ask_size2": float(self.ask_list[1][1]),
                                   "bid_price3": float(self.bid_list[0][2]), "ask_price3": float(self.ask_list[0][2]),
                                   "bid_size3": float(self.bid_list[1][2]), "ask_size3": float(self.ask_list[1][2]),
                                   "bid_price4": float(self.bid_list[0][3]), "ask_price4": float(self.ask_list[0][3]),
                                   "bid_size4": float(self.bid_list[1][3]), "ask_size4": float(self.ask_list[1][3]),
                                   "bid_price5": float(self.bid_list[0][4]), "ask_price5": float(self.ask_list[0][4]),
                                   "bid_size5": float(self.bid_list[1][4]), "ask_size5": float(self.ask_list[1][4]),
                                   "bid_price6": float(self.bid_list[0][5]), "ask_price6": float(self.ask_list[0][5]),
                                   "bid_size6": float(self.bid_list[1][5]), "ask_size6": float(self.ask_list[1][5]),
                                   "bid_price7": float(self.bid_list[0][6]), "ask_price7": float(self.ask_list[0][6]),
                                   "bid_size7": float(self.bid_list[1][6]), "ask_size7": float(self.ask_list[1][6]),
                                   "bid_price8": float(self.bid_list[0][7]), "ask_price8": float(self.ask_list[0][7]),
                                   "bid_size8": float(self.bid_list[1][7]), "ask_size8": float(self.ask_list[1][7]),
                                   "bid_price9": float(self.bid_list[0][8]), "ask_price9": float(self.ask_list[0][8]),
                                   "bid_size9": float(self.bid_list[1][8]), "ask_size9": float(self.ask_list[1][8]),
                                   "bid_price10": float(self.bid_list[0][9]), "ask_price10": float(self.ask_list[0][9]),
                                   "bid_size10": float(self.bid_list[1][9]), "ask_size10": float(self.ask_list[1][9]),
                                   "bid_price11": float(self.bid_list[0][10]), "ask_price11": float(self.ask_list[0][10]),
                                   "bid_size11": float(self.bid_list[1][10]), "ask_size11": float(self.ask_list[1][10]),
                                   "bid_price12": float(self.bid_list[0][11]), "ask_price12": float(self.ask_list[0][11]),
                                   "bid_size12": float(self.bid_list[1][11]), "ask_size12": float(self.ask_list[1][11]),
                                   "bid_price13": float(self.bid_list[0][12]), "ask_price13": float(self.ask_list[0][12]),
                                   "bid_size13": float(self.bid_list[1][12]), "ask_size13": float(self.ask_list[1][12]),
                                   "bid_price14": float(self.bid_list[0][13]), "ask_price14": float(self.ask_list[0][13]),
                                   "bid_size14": float(self.bid_list[1][13]), "ask_size14": float(self.ask_list[1][13]),
                                   "bid_price15": float(self.bid_list[0][14]), "ask_price15": float(self.ask_list[0][14]),
                                   "bid_size15": float(self.bid_list[1][14]), "ask_size15": float(self.ask_list[1][14]),
                                   "bid_price16": float(self.bid_list[0][15]), "ask_price16": float(self.ask_list[0][15]),
                                   "bid_size16": float(self.bid_list[1][15]), "ask_size16": float(self.ask_list[1][15]),
                                   "bid_price17": float(self.bid_list[0][16]), "ask_price17": float(self.ask_list[0][16]),
                                   "bid_size17": float(self.bid_list[1][16]), "ask_size17": float(self.ask_list[1][16]),
                                   "bid_price18": float(self.bid_list[0][17]), "ask_price18": float(self.ask_list[0][17]),
                                   "bid_size18": float(self.bid_list[1][17]), "ask_size18": float(self.ask_list[1][17]),
                                   "bid_price19": float(self.bid_list[0][18]), "ask_price19": float(self.ask_list[0][18]),
                                   "bid_size19": float(self.bid_list[1][18]), "ask_size19": float(self.ask_list[1][18]),
                                   "bid_price20": float(self.bid_list[0][19]), "ask_price20": float(self.ask_list[0][19]),
                                   "bid_size20": float(self.bid_list[1][19]), "ask_size20": float(self.ask_list[1][19])}],
                                 ignore_index=True)

        self.traded_quantity_buyer_is_maker, self.traded_quantity_buyer_is_taker = 0, 0


if __name__ == "__main__":

    websocket.enableTrace(True)

    socket = "wss://fstream.binance.com/ws/"
    ws1 = websocket.WebSocketApp(socket,
                                 on_open=on_open,
                                 on_message=on_message,
                                 on_error=on_error,
                                 on_close=on_close)

    binance_wrapper = binance_wrapper_class(pandas)

    ws1.run_forever()



