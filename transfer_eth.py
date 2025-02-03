import os
import time
import logging
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import Timeout

# Configure logging
logging.basicConfig(
    filename='transfer_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants and configuration
INFURA_URL = os.getenv('INFURA_URL', 'YOUR_INFURA_URL')
RECIPIENT_ADDRESS = os.getenv('RECIPIENT_ADDRESS', 'RECIPIENT_ETH_ADDRESS')
GAS_PRICE_GWEI = int(os.getenv('GAS_PRICE_GWEI', 50))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))
RETRY_LIMIT = int(os.getenv('RETRY_LIMIT', 3))
WALLET_FILE = os.getenv('WALLET_FILE', 'wallets.txt')

# Initialize Web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.isConnected():
    logging.critical("Failed to connect to the Ethereum network.")
    raise ConnectionError("Unable to connect to the Ethereum network.")

def load_wallets(file_path=WALLET_FILE):
    """Load wallet addresses and private keys from a file."""
    try:
        with open(file_path, 'r') as file:
            wallets = [line.strip().split(',') for line in file if ',' in line]
        if not wallets:
            raise ValueError("Wallet file contains no valid data.")
        return wallets
    except FileNotFoundError:
        logging.critical(f"Wallet file '{file_path}' not found.")
        raise
    except Exception as e:
        logging.error(f"Error reading wallet file: {e}")
        raise

def calculate_transaction_fee(gas_limit):
    """Calculate the transaction fee in Wei using dynamic gas price."""
    gas_price = web3.eth.gas_price
    return gas_price * gas_limit

def send_eth(wallet_address, private_key, retries=0):
    """Send ETH from a wallet with retry logic."""
    try:
        balance = web3.eth.get_balance(wallet_address)
        gas_price = web3.eth.gas_price  # Use current gas price
        estimated_gas = web3.eth.estimate_gas({
            'from': wallet_address,
            'to': RECIPIENT_ADDRESS,
            'value': balance
        })
        transaction_fee = calculate_transaction_fee(estimated_gas)

        if balance <= transaction_fee:
            logging.warning(f"Skipping {wallet_address} - Insufficient balance. "
                            f"Balance: {web3.fromWei(balance, 'ether')} ETH, Required: {web3.fromWei(transaction_fee, 'ether')} ETH")
            return

        nonce = web3.eth.get_transaction_count(wallet_address)
        value_to_send = balance - transaction_fee

        if value_to_send <= 0:
            logging.warning(f"Skipping {wallet_address} due to low balance after fees.")
            return

        tx = {
            'nonce': nonce,
            'to': RECIPIENT_ADDRESS,
            'value': value_to_send,
            'gas': estimated_gas,
            'gasPrice': gas_price
        }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logging.info(f"Sent {web3.fromWei(value_to_send, 'ether')} ETH from {wallet_address}. TX Hash: {web3.toHex(tx_hash)}")

    except Timeout:
        if retries < RETRY_LIMIT:
            logging.warning(f"Timeout for {wallet_address}. Retrying {retries + 1}/{RETRY_LIMIT}.")
            return send_eth(wallet_address, private_key, retries + 1)
        logging.error(f"Failed to send ETH from {wallet_address} after {RETRY_LIMIT} retries.")
    except ValueError as e:
        logging.error(f"Transaction error for {wallet_address}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for {wallet_address}: {e}")

def process_wallets(wallets):
    """Process wallets in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_wallet = {executor.submit(send_eth, w, k): (w, k) for w, k in wallets}
        for future in as_completed(future_to_wallet):
            wallet, _ = future_to_wallet[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing {wallet}: {e}")

if __name__ == "__main__":
    try:
        start_time = time.time()
        wallets = load_wallets()
        process_wallets(wallets)
        logging.info(f"Transfer process completed in {time.time() - start_time:.2f} seconds.")
    except Exception as e:
        logging.critical(f"Critical error: {e}")
