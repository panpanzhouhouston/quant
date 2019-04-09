import grpc

# import the generated classes
import broker_pb2
import broker_pb2_grpc
import common_pb2
import common_pb2_grpc
import numpy as np

import asyncio

import time
from collections import defaultdict, deque
from multiprocessing import Pool

greedy_level = {'A001.PSE': 0.01, 'A002.PSE': 0.01, 'B001.PSE': 0.01, 'B002.PSE': 0.01}

short_points_ave = {'A001.PSE': deque(maxlen=30),
                  'A002.PSE': deque(maxlen=30),
                  'B001.PSE': deque(maxlen=30),
                  'B002.PSE': deque(maxlen=30)}

long_points_ave = {'A001.PSE': deque(maxlen=200),
                  'A002.PSE': deque(maxlen=200),
                  'B001.PSE': deque(maxlen=200),
                  'B002.PSE': deque(maxlen=200)}


class Broker(object):

    def __init__(self, trader_id, trader_pin, channel):
        self.trader_id = trader_id
        self.trader_pin = trader_pin
        self.channel = grpc.insecure_channel(channel)
        self.stub = broker_pb2_grpc.BrokerStub(self.channel)

    def GET_TRADER(self):
        request = broker_pb2.TraderRequest(trader_id=self.trader_id,
                                           trader_pin=self.trader_pin,
                                           request_type="FULL_INFO")

        return self.stub.get_trader(request)

    def GET_TRADERS_INFO(self):
        request = broker_pb2.TraderRequest(trader_id=self.trader_id,
                                           trader_pin=self.trader_pin,
                                           request_type="FULL_INFO")

        return self.stub.get_traders_info(request)

    def NEW_ORDER(self, SYMBAL, SIDE, VOL, PRICE, POS_TYPE, IS_MARKET=False):
        request = broker_pb2.TraderRequest(trader_id=self.trader_id,
                                           trader_pin=self.trader_pin,
                                           request_type="NEW_ORDER",
                                           side=SIDE,
                                           symbol=SYMBAL,
                                           volume=VOL,
                                           price=PRICE,
                                           pos_type=POS_TYPE,
                                           is_market=IS_MARKET)

        return self.stub.new_order(request)

    def NEW_ORDER_ON_REQUEST(self, request):
        return self.stub.new_order(request)

    def CANCEL_ORDER(self, order_id):
        request = broker_pb2.TraderRequest(trader_id=self.trader_id,
                                           trader_pin=self.trader_pin,
                                           request_type="CANCEL_ORDER",
                                           order_id=order_id)
        return self.stub.cancel_order(request)


def instrument_parser(item, symbols):
    all_instruments = item.instruments
    all_instrument_dict = defaultdict(dict)
    for instrument in all_instruments:
        if instrument.symbol in ['A000.PSE', 'B000.PSE']:
            continue
        else:
            all_instrument_dict[instrument.symbol]['last_price'] = instrument.last_price
            all_instrument_dict[instrument.symbol]['bid_order_count'] = instrument.bid_order_count
            all_instrument_dict[instrument.symbol]['ask_order_count'] = instrument.ask_order_count
            all_instrument_dict[instrument.symbol]['bid_depth'] = instrument.bid_depth
            all_instrument_dict[instrument.symbol]['ask_depth'] = instrument.ask_depth
            all_instrument_dict[instrument.symbol]['deliver_price'] = instrument.deliver_price
            all_instrument_dict[instrument.symbol]['bid_levels'] = [[x.price, x.volume] for x in instrument.bid_levels]
            all_instrument_dict[instrument.symbol]['ask_levels'] = [[x.price, x.volume] for x in instrument.ask_levels]
    return all_instrument_dict


def position_order_parser(response):
    account_detail = {}
    account_detail['time_stamp'] = response.timestamp
    account_detail['total_cash'] = response.account.total_cash
    account_detail['locked_cash'] = response.account.locked_cash
    account_detail['long_pos'] = defaultdict(dict)
    account_detail['short_pos'] = defaultdict(dict)
    for key, position in response.positions.long_positions.items():
        account_detail['long_pos'][key] = (position.avg_price,
                                           position.volume,
                                           position.occupied_cash)
    for key, position in response.positions.short_positions.items():
        account_detail['short_pos'][key] = (position.avg_price,
                                            position.volume,
                                            position.occupied_cash)
    account_detail['orders'] = defaultdict(list)
    for key, val in response.orders.orders.items():
        account_detail['orders'][(val.symbol, val.side, val.pos_type)].append([val.order_id,
                                                                               val.init_price,
                                                                               val.volume])
    return account_detail


def cancel_order(broker, order_id):
    # print('cancel order')
    result = broker.CANCEL_ORDER(order_id)
    return result


def new_order(broker, SYMBAL, SIDE, VOL, PRICE, POS_TYPE, IS_MARKET):
    # print('put new order')

    result = broker.NEW_ORDER(SYMBAL, SIDE, VOL, PRICE, POS_TYPE, IS_MARKET)
    """
    SYMBAL, SIDE, VOL, PRICE, POS_TYPE, IS_MARKET
    """
    return result


def order_on_task(task):
    # broker = Broker(25, 'qWC6G7nao', '113.208.112.25:57500')
    if task[0] == 'cancel':
        result = broker.CANCEL_ORDER(task[1])
    else:
        result = broker.NEW_ORDER(task[1], task[2], task[3], task[4], task[5], task[6])
    return result


def task_generator(account_detail, all_instrument_dict, ticker, long_ave, short_ave):
    deliver_price = all_instrument_dict[ticker]['deliver_price']

    if ticker == 'A002.PSE':
        trend = (short_ave - long_ave)/2
    else:
        trend = (short_ave - long_ave) / 2.5
        
    price = round(short_ave + trend, 2)

    bid_level = [0, 0, 0]
    ask_level = [0, 0, 0]
    task = []

    val = account_detail['long_pos'][ticker]
    #print(val)
    if val and val[1] > 50:
        task.append(('order', ticker, 'ASK', val[1] - 25,
                     round(val[0], 2) + greedy_level[ticker],
                     'LONG', False))

    val = account_detail['short_pos'][ticker]
    #print(val)
    if val and val[1] > 50:

        task.append(('order', ticker, 'BID', val[1] - 25,
                     round(val[0], 2) - greedy_level[ticker],
                     'SHORT', False))

    for order in account_detail['orders'][(ticker, 0, 0)]:
        if price - order[1] < 0.08 or price - order[1] > 0.11:
            task.append(('cancel', order[0]))
        elif round(price - order[1], 2) == 0.08:
            bid_level[0] += order[2]
        elif round(price - order[1], 2) == 0.09:
            bid_level[1] += order[2]
        elif round(price - order[1], 2) == 0.10:
            bid_level[2] += order[2]

    for order in account_detail['orders'][(ticker, 1, 1)]:
        if -price + order[1] < 0.08 or -price + order[1] > 0.11:
            task.append(('cancel', order[0]))
        elif round(-price + order[1], 2) == 0.08:
            ask_level[0] += order[2]
        elif round(-price + order[1], 2) == 0.09:
            ask_level[1] += order[2]
        elif round(-price + order[1], 2) == 0.10:
            ask_level[2] += order[2]




    for i in range(3):
        if bid_level[i] < 5:
            # print('adding bid tick at ', DEL_PRC - 0.08 - 0.01*i, ' with vol', 5-bid_level[i])
            task.append(('order', ticker, 'BID', 5 - bid_level[i], price - 0.08 - 0.01 * i, 'LONG', False))

        if ask_level[i] < 5:
            # print('adding ask tick at ', DEL_PRC + 0.08 + 0.01*i, ' with vol', 5-ask_level[i])
            task.append(('order', ticker, 'ASK', 5 - ask_level[i], price + 0.08 + 0.01 * i, 'SHORT', False))

    return task


def main():
    market_channel = grpc.insecure_channel('113.208.112.25:57600')
    market_stub = broker_pb2_grpc.MarketDataStub(market_channel)
    pool = Pool(processes=200)
    broker = Broker(5, 'qWC6G7nao', '113.208.112.25:57502')
    data = market_stub.subscribe(common_pb2.Empty())
    try:
        i = 0
        for item in data:
            # print('before')
            all_instrument_dict = instrument_parser(item, ['A001.PSE', 'A002.PSE', 'B001.PSE', 'B002.PSE'])
            for sym in ['A001.PSE', 'A002.PSE', 'B001.PSE', 'B002.PSE']:
                long_points_ave[sym].append(all_instrument_dict[sym]['deliver_price'])
                short_points_ave[sym].append(all_instrument_dict[sym]['deliver_price'])

            if i % 20 == 19:
                # print('in')
                account_detail = position_order_parser(broker.GET_TRADER())

                tasks = []
                for ticker in ['A001.PSE', 'A002.PSE', 'B001.PSE', 'B002.PSE']:
                    long_ave = np.mean(long_points_ave[ticker])
                    short_ave = np.mean(short_points_ave[ticker])

                    tasks.extend(task_generator(account_detail, all_instrument_dict, ticker, long_ave, short_ave))

                t0 = time.time()
                print(len(tasks))
                if not tasks:
                    pass
                else:
                    results = pool.map_async(order_on_task, tasks)
                    results.wait(timeout=2.5)

                # pool.close()
                print(time.time() - t0)
                print(account_detail['time_stamp'])
            i += 1
            # print('out')

    except KeyboardInterrupt:
        print('Program stop by keyboardinterupt')


def f(x):
    # print(x)

    response = cancel_order(broker, 342)
    return response


def test_neworder():
    t0 = time.time()
    pool = Pool(processes=200)
    for i in range(100):
        result = pool.map_async(f, range(50))
        result.wait(timeout=2)
        print(time.time() - t0)

    print('all tasks done in ', time.time() - t0)




broker = Broker(5, 'qWC6G7nao', '113.208.112.25:57502')
main()





