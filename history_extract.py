from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.common import BarData

import logging
import sys
import sqlite3
import os

class InitializableContract(Contract):
    def __init__(self, symbol="", secType="", currency="", exchange="", lastTradeDateOrContractMonth="", tradingClass="", multiplier="", primaryExchange="", localSymbol=""):
        self.conId = 0
        self.symbol = symbol
        self.secType = secType
        self.lastTradeDateOrContractMonth = lastTradeDateOrContractMonth
        self.strike = 0.  # float !!
        self.right = ""
        self.multiplier = multiplier
        self.exchange = exchange
        self.primaryExchange = primaryExchange # pick an actual (ie non-aggregate) exchange that the contract trades on.  DO NOT SET TO SMART.
        self.currency = currency
        self.localSymbol = localSymbol
        self.tradingClass = tradingClass
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
                target_id, 
                self.targets[target_id], "", "15 Y", "1 day", "TRADES", 1, 1, False, []
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
                exchange=? AND
                contract=? AND
                strike=?
        ''', (
            contract.symbol,
            contract.secType,
            contract.currency,
            contract.exchange,
            contract.lastTradeDateOrContractMonth,
            contract.strike
        ))

        result = cursor.fetchone()

        min_datestamp, max_datestamp = None, None

        if not result:
            cursor.execute('''
                INSERT INTO security (symbol, type, currency, exchange, contract, strike) VALUES(?, ?, ?, ?, ?, ?)
                ''', (
                    contract.symbol,
                    contract.secType,
                    contract.currency,
                    contract.exchange,
                    contract.lastTradeDateOrContractMonth,
                    contract.strike
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

    # From the contracts overview in the documentation:
    
    # > "The simplest way to define a contract is by providing its symbol, security type, 
    # > currency and exchange. The vast majority of stocks, CFDs, Indexes or FX pairs can 
    # > be uniquely defined through these four attributes. More complex contracts such as 
    # > options and futures require some extra information due to their nature."

    # For futures, the "lastTradeDateOrContractMonth" attribute combined with the above mentioned
    # fully define the contract object. Options also require a strike.
    cursor.execute('''
        CREATE TABLE security (
            id INTEGER PRIMARY KEY,            -- only relevant internally

            symbol TEXT NOT NULL,
            type TEXT NOT NULL,                -- type of security, i.e.: IND for index, FUT for future, STK for stock
            currency TEXT NOT NULL,            -- i.e.: USD, CAD
            exchange TEXT NOT NULL,            -- i.e.: GLOBEX, CBOE, CME, ARCA, TSE
            contract TEXT NOT NULL DEFAULT "", -- i.e.: 201902. Required for futures.
            strike FLOAT NOT NULL DEFAULT 0.0, -- i.e.: 54.0. Option strike.

            -- for this unique constraint to work, we have to use default values rather than NULLs.
            -- that's fine, because it's the same thing the IB API does.
            UNIQUE (symbol, type, currency, exchange, contract, strike) 
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

    #app.wrapper.addTarget(InitializableContract(symbol="DTX", secType="IND", currency="USD", exchange="CBOE")) # transports
    #app.wrapper.addTarget(InitializableContract(symbol="INDU", secType="IND", currency="USD", exchange="CME")) # industrials
    #app.wrapper.addTarget(InitializableContract(symbol="SPY", secType="STK", currency="USD", exchange="ARCA"))  
    #app.wrapper.addTarget(InitializableContract(symbol="WPK", secType="STK", currency="CAD", exchange="TSE"))
    #app.wrapper.addTarget(InitializableContract(symbol="USD", secType="CASH", currency="CAD", exchange="IDEALPRO")) # no historical data?
    #app.wrapper.addTarget(InitializableContract(symbol="HEG9", secType="FUT", currency="USD", exchange="GLOBEX"))
    
    #for year in (2018, 2019):
    #    for month in ('12', '02', '04', '05', '06', '07', '08', '10'):
    for year in (2019,):
        for month in ('02', '04'):
            app.wrapper.addTarget(InitializableContract(
                symbol="HE",
                secType="FUT", 
                currency="USD", 
                exchange="GLOBEX", 
                lastTradeDateOrContractMonth=f"{year}{month}", 
            ))

    #app.connect("127.0.0.1", 7496, clientId=0)  # tws
    app.connect("127.0.0.1", 4001, clientId=0) # gateway

    logger.info(f"serverVersion:{app.serverVersion()} connectionTime:{app.twsConnectionTime()}")
    app.wrapper.fetchHistoryStarts()
    app.wrapper.fetchHistory()

        app.run()
