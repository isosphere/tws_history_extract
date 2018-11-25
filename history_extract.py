from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.common import BarData

import logging
import sys
import sqlite3
import os

class InitializableContract(Contract):
    def __init__(self, symbol="", secType="", currency="", exchange=""):
        self.conId = 0
        self.symbol = symbol
        self.secType = secType
        self.lastTradeDateOrContractMonth = ""
        self.strike = 0.  # float !!
        self.right = ""
        self.multiplier = ""
        self.exchange = exchange
        self.primaryExchange = "" # pick an actual (ie non-aggregate) exchange that the contract trades on.  DO NOT SET TO SMART.
        self.currency = currency
        self.localSymbol = ""
        self.tradingClass = ""
        self.includeExpired = False
        self.secIdType = ""	  # CUSIP;SEDOL;ISIN;RIC
        self.secId = ""

        #combos
        self.comboLegsDescrip = ""  # type: str; received in open order 14 and up for all combos
        self.comboLegs = None     # type: list<ComboLeg>
        self.deltaNeutralContract = None    


class HistoryWrapper(EWrapper):
    def __init__(self, sqlite_connection):
        EWrapper.__init__(self)
        self.contract_information = dict()
        self.targets = list()
        self.sqlite_connection = sqlite_connection
        self.tickerIdCounter = 0

    def fetchHistoryStarts(self):
        for target_id in range(0, len(self.targets)):
            app.reqHeadTimeStamp(target_id, self.targets[target_id], "TRADES", 1, 1)

    def fetchHistory(self):
        for target_id in range(0, len(self.targets)):
            app.reqHistoricalData(
                target_id, self.targets[target_id], "", "15 Y", "1 day", "TRADES", 1, 1, False, []
            )

    def addTarget(self, contract):
        # check that this contact is in the database
        cursor = self.sqlite_connection.cursor()
        cursor.execute('''
            SELECT id 
            FROM security WHERE 
                symbol=? AND 
                type=? AND 
                currency=? AND 
                exchange=?
        ''', (
            contract.symbol,
            contract.secType,
            contract.currency,
            contract.exchange
        ))

        result = cursor.fetchone()

        min_datestamp, max_datestamp = None, None

        if not result:
            cursor.execute('''
                INSERT INTO security (symbol, type, currency,exchange) VALUES(?, ?, ?, ?)
                ''', (
                    contract.symbol,
                    contract.secType,
                    contract.currency,
                    contract.exchange
                )
            )
            
            security_dbid = int(cursor.lastrowid)
        else:
            security_dbid = int(result[-1])

            cursor.execute('SELECT MIN(datestamp), MAX(datestamp) FROM prices WHERE security_id = ?', (security_dbid,))
            result = cursor.fetchone()

            if result:
                min_datestamp, max_datestamp = result
        
        cursor.close()
        self.sqlite_connection.commit()

        self.contract_information[self.tickerIdCounter] = {
            'database_id':security_dbid
        }

        if min_datestamp and max_datestamp:
            self.contract_information[self.tickerIdCounter]["min_datestamp"] = min_datestamp
            self.contract_information[self.tickerIdCounter]["max_datestamp"] = max_datestamp

        self.tickerIdCounter += 1

        self.targets.append(contract)
    
    def headTimestamp(self, reqId:int, headTimestamp:str):
        self.contract_information[reqId]["headTimestamp"] = headTimestamp
        print("HeadTimestamp: ", reqId, " ", headTimestamp)

    def historicalData(self, reqId:int, bar: BarData):
        if "history" not in self.contract_information[reqId]:
            self.contract_information[reqId]["history"] = list()

        self.contract_information[reqId]["history"].append({
            "date": bar.date, 
            "open": bar.open, 
            "high": bar.high, 
            "low": bar.low, 
            "close": bar.close, 
            "volume": bar.volume
        })

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd ", reqId, "from", start, "to", end)
        # wipe existing historical data
        cursor = self.sqlite_connection.cursor()
        cursor.execute('DELETE FROM prices WHERE security_id = ?', (self.contract_information[reqId]['database_id'],))

        for row in self.contract_information[reqId]["history"]:
            cursor.execute(
                'INSERT INTO prices(security_id, datestamp, open, high, low, close, volume) VALUES(?,?,?,?,?,?,?)',
                (self.contract_information[reqId]['database_id'], row["date"], row["open"], row["high"], row["low"], row["close"], row["volume"])
            )
        
        cursor.close()
        self.sqlite_connection.commit()    

class HistoryClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class HistoryApp(HistoryWrapper, HistoryClient):
    def __init__(self, sqlite_connection):
        HistoryWrapper.__init__(self, sqlite_connection=sqlite_connection)
        HistoryClient.__init__(self, wrapper=self)

def create_database(filename):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    cursor.execute('''
        CREATE TABLE security (
            id INTEGER PRIMARY KEY, 
            symbol TEXT, 
            type TEXT, 
            currency TEXT, 
            exchange TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE prices (
            id INTEGER PRIMARY KEY, 
            security_id INTEGER, 
            datestamp TEXT,  
            open REAL, 
            high REAL, 
            low REAL, 
            close REAL, 
            volume INT,
            FOREIGN KEY(security_id) REFERENCES security(id)
        )
    ''')

    cursor.close()

    connection.commit()
    connection.close()

if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    database_file = 'database.db'

    if not os.path.exists(database_file):
        create_database(database_file)

    connection = sqlite3.connect(database_file)   

    app = HistoryApp(sqlite_connection=connection)

    app.wrapper.addTarget(InitializableContract(symbol="DTX", secType="IND", currency="USD", exchange="CBOE")) # transports
    app.wrapper.addTarget(InitializableContract(symbol="INDU", secType="IND", currency="USD", exchange="CME")) # industrials
    app.wrapper.addTarget(InitializableContract(symbol="SPY", secType="STK", currency="USD", exchange="ARCA"))  
    #app.wrapper.addTarget(InitializableContract(symbol="WPK", secType="STK", currency="CAD", exchange="TSE"))
    #app.wrapper.addTarget(InitializableContract(symbol="USD", secType="CASH", currency="CAD", exchange="IDEALPRO")) # no historical data?

    app.connect("127.0.0.1", 7496, clientId=0)  # tws
    #app.connect("127.0.0.1", 4001, clientId=0) # gateway

    logger.info("serverVersion:%s connectionTime:%s" % (app.serverVersion(), app.twsConnectionTime()))
    app.wrapper.fetchHistoryStarts()
    app.wrapper.fetchHistory()

    try:
        app.run()
    except KeyboardInterrupt:
        logger.warning("Caught keyboard interrupt, committing database data before exiting.")
        connection.commit()
        connection.close()

