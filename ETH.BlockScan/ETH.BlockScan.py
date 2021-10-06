# Indexer for Ethereum to get transaction list by ETH address
# https://github.com/Adamant-im/ETH-transactions-storage
# 2021 ADAMANT Foundation (devs@adamant.im), Francesco Bonanno
# (mibofra@parrotsec.org),
# Guénolé de Cadoudal (guenoledc@yahoo.fr), Drew Wells (drew.wells00@gmail.com)
# 2020-2021 ADAMANT Foundation (devs@adamant.im): Aleksei Lebedev
# 2017-2020 ADAMANT TECH LABS LP (pr@adamant.im): Artem Brunov, Aleksei Lebedev
# v2.0
from os import environ
from web3 import Web3
from web3.middleware import geth_poa_middleware
import psycopg2
import time
import sys
import logging
import adodbapi

# MsSql
conStr = "PROVIDER=SQLOLEDB;Data Source={0};Database={1}; \
       trusted_connection=yes;UID={2};PWD={3};".format("127.0.0.1","Exchange","exchange","exchange1")

# PostgreSql
serverPostgreSql = '127.0.0.1'
databasePostgreSql = 'index'
usernamePostgreSql = 'postgres'
passwordPostgreSql = 'admin'
portPostgreSql = '5432'


startBlock =  13366439
pollingPeriod = 20



web3 = Web3()
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

# Start logger
#logger = logging.getLogger("EthIndexerLog")
logger = logging.getLogger("eth-sync")
logger.setLevel(logging.INFO)

# File logger
#lfh = logging.FileHandler("/var/log/ethindexer.log")
lfh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
lfh.setFormatter(formatter)
logger.addHandler(lfh)

# Adds all transactions from Ethereum block
def insertion(blockid, tr, addressRows):
    time = web3.eth.getBlock(blockid)['timestamp']
    for x in range(0, tr):
        trans = web3.eth.getTransactionByBlock(blockid, x)
        # Save also transaction status, should be null if pre byzantium blocks
        status = bool(web3.eth.get_transaction_receipt(trans['hash']).status)
        txhash = trans['hash'].hex()
        value = trans['value']
        inputinfo = trans['input']
        # Check if transaction is a contract transfer
        if (value == 0 and not inputinfo.startswith('0xa9059cbb')):
            continue
        fr = trans['from'].lower()
        to = trans['to'].lower()
        gasprice = trans['gasPrice']
        gas = web3.eth.getTransactionReceipt(trans['hash'])['gasUsed']
        contract_to = ''
        contract_value = ''
        # Check if transaction is a contract transfer
        if inputinfo.startswith('0xa9059cbb'):
            contract_to = inputinfo[10:-64]
            contract_value = inputinfo[74:]
        # Correct contract transfer transaction represents '0x' + 4 bytes
        # 'a9059cbb' + 32 bytes (64 chars) for contract address and 32 bytes
        # for its value
        # Some buggy txs can break up Indexer, so we'll filter it
        if len(contract_to) > 128:
            logger.info('Skipping ' + str(txhash) + ' tx. Incorrect contract_to length: ' + str(len(contract_to)))
            contract_to = ''
            contract_value = ''
        try:
            if to in addressRows:
                  cursorPSQL.execute(
                    'INSERT INTO public.ethtxs(time, txfrom, txto, value, gas, gasprice, block, txhash, contract_to, contract_value, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (time, fr, to, value, gas, gasprice, blockid, txhash, contract_to, contract_value, status))
        except BaseException :
            continue

while True:
    try:
        connPSQl = psycopg2.connect(
        database = databasePostgreSql, 
        user = usernamePostgreSql, 
        password = passwordPostgreSql, 
        host = serverPostgreSql, 
        port = portPostgreSql
        )
        connPSQl.autocommit = True
    except:
        logger.error("Unable to connect to database PSQL")

    cursorPSQL = connPSQl.cursor()
    cursorPSQL.execute('SELECT Max(block) from public.ethtxs')
    maxblockindb = cursorPSQL.fetchone()[0]

    # On first start, we index transactions from a block number you indicate
    if maxblockindb is None:
        maxblockindb = int(startBlock)

    try:
        try:
            connMSSQL = adodbapi.connect(conStr)
            cursorMSSQL = connMSSQL.cursor()
        except:
            logger.error("Unable to connect to database MsSql")

        cursorMSSQL.execute("SELECT Address from IncomeWallets Where CurrencyAcronim = 'ETH'")
        addressRows = cursorMSSQL.fetchall()
        cursorMSSQL.close()
        connMSSQL.close()

        for block in range(maxblockindb + 1, maxblockindb + 1000):
            logger.info('Current best block in index: ' + str(maxblockindb))
            transactions = web3.eth.getBlockTransactionCount(block)
            if transactions > 0:
                insertion(block, transactions, addressRows.ado_results[0])
        else:
            logger.debug('Block ' + str(block) + ' does not contain transactions')
    except:
        logger.error("block does not exist")

    cursorPSQL.close()
    connPSQl.close()
    time.sleep(int(pollingPeriod))
