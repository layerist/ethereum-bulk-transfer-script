import threading
import time
import logging
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    filename='transfer_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants and configuration
INFURA_URL = 'YOUR_INFURA_URL'
RECIPIENT_ADDRESS = 'RECIPIENT_ETH_ADDRESS'
GAS_PRICE_GWEI = 50  # Adjust as necessary
GAS_LIMIT = 21000
MAX_WORKERS = 10  # Max number of threads

# Initialize web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

# Ensure connection is successful
if not web3.isConnected():
    logging.error("Failed to connect to the Ethereum network")
    raise ConnectionError("Failed to connect to the Ethereum network")

def load_wallet_addresses(file_path='wallets.txt'):
    """Load wallet addresses and private keys from a file."""
    try:
        with open(file_path, 'r') as file:
            wallets = file.read().strip().splitlines()
        return [wallet.split(',') for wallet in wallets if ',' in wallet]
    except FileNotFoundError:
        logging.error("Wallet file not found")
        raise
    except Exception as e:
        logging.error(f"Error reading wallet file: {str(e)}")
        raise

def send_eth_from_wallet(wallet_address, private_key):
    """Send ETH from a wallet to the recipient address."""
    try:
        # Get the balance of the wallet
        balance = web3.eth.get_balance(wallet_address)
        required_gas_fee = web3.toWei(GAS_PRICE_GWEI, 'gwei') * GAS_LIMIT

        if balance <= required_gas_fee:
            logging.info(f'Insufficient balance in wallet: {wallet_address}')
            return

        # Get nonce
        nonce = web3.eth.get_transaction_count(wallet_address)

        # Build the transaction
        gas_price = web3.toWei(GAS_PRICE_GWEI, 'gwei')
        tx = {
            'nonce': nonce,
            'to': RECIPIENT_ADDRESS,
            'value': balance - required_gas_fee,
            'gas': GAS_LIMIT,
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
    """Process each wallet using a thread pool."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_wallet = {executor.submit(send_eth_from_wallet, wallet, key): (wallet, key) for wallet, key in wallet_addresses}
        for future in as_completed(future_to_wallet):
            wallet, key = future_to_wallet[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing wallet {wallet}: {str(e)}")

if __name__ == "__main__":
    try:
        wallet_addresses = load_wallet_addresses()
        start_time = time.time()
        process_wallets(wallet_addresses)
        end_time = time.time()
        logging.info(f"Transfer process completed in {end_time - start_time:.2f} seconds")
    except Exception as e:
        logging.error(f"An error occurred during the transfer process: {str(e)}")
