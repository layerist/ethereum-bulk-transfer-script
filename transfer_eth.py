import threading
import time
import logging
from web3 import Web3
import json

# Set up logging
logging.basicConfig(filename='transfer_log.txt', level=logging.INFO, format='%(asctime)s %(message)s')

# Configuration
INFURA_URL = 'YOUR_INFURA_URL'
RECIPIENT_ADDRESS = 'RECIPIENT_ETH_ADDRESS'
GAS_PRICE_GWEI = 50  # Adjust as necessary

# Initialize web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

# Ensure connection is successful
if not web3.isConnected():
    raise ConnectionError("Failed to connect to the Ethereum network")

# Load wallet addresses from a file
with open('wallets.txt') as file:
    wallet_addresses = file.read().splitlines()

def send_eth_from_wallet(wallet_address, private_key):
    try:
        # Get the balance of the wallet
        balance = web3.eth.get_balance(wallet_address)
        if balance == 0:
            logging.info(f'No balance in wallet: {wallet_address}')
            return

        # Get nonce
        nonce = web3.eth.get_transaction_count(wallet_address)

        # Build the transaction
        gas_price = web3.toWei(GAS_PRICE_GWEI, 'gwei')
        tx = {
            'nonce': nonce,
            'to': RECIPIENT_ADDRESS,
            'value': balance,
            'gas': 21000,
            'gasPrice': gas_price
        }

        # Sign the transaction
        signed_tx = web3.eth.account.sign_transaction(tx, private_key)

        # Send the transaction
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)

        logging.info(f'Transaction sent from {wallet_address}. TX Hash: {web3.toHex(tx_hash)}')

    except Exception as e:
        logging.error(f'Error sending ETH from {wallet_address}: {str(e)}')

def process_wallets(wallet_addresses):
    threads = []
    for wallet in wallet_addresses:
        wallet_address, private_key = wallet.split(',')
        thread = threading.Thread(target=send_eth_from_wallet, args=(wallet_address, private_key))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    start_time = time.time()
    process_wallets(wallet_addresses)
    end_time = time.time()
    logging.info(f"Transfer process completed in {end_time - start_time} seconds")
