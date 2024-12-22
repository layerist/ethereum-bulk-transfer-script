import threading
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
INFURA_URL = 'YOUR_INFURA_URL'
RECIPIENT_ADDRESS = 'RECIPIENT_ETH_ADDRESS'
GAS_PRICE_GWEI = 50  # Set gas price in Gwei
MAX_WORKERS = 10  # Max number of concurrent threads
RETRY_LIMIT = 3  # Max retries for failed transactions
WALLET_FILE = 'wallets.txt'  # Path to wallet file

# Initialize Web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.isConnected():
    logging.critical("Failed to connect to the Ethereum network.")
    raise ConnectionError("Unable to connect to the Ethereum network.")

def load_wallet_addresses(file_path=WALLET_FILE):
    """
    Load wallet addresses and private keys from a file.
    Expects lines in the format: wallet_address,private_key.
    """
    try:
        with open(file_path, 'r') as file:
            wallets = [line.strip().split(',') for line in file if ',' in line]
            if not wallets:
                logging.critical("Wallet file is empty or incorrectly formatted.")
                raise ValueError("Wallet file contains no valid data.")
            return wallets
    except FileNotFoundError:
        logging.critical(f"Wallet file '{file_path}' not found.")
        raise
    except Exception as e:
        logging.error(f"Error reading wallet file: {e}")
        raise

def calculate_transaction_fee(gas_limit):
    """Calculate the transaction fee in Wei."""
    gas_price = web3.toWei(GAS_PRICE_GWEI, 'gwei')
    return gas_price * gas_limit

def send_eth(wallet_address, private_key, retries=0):
    """
    Send ETH from a wallet, with retry logic for handling timeouts and errors.
    Retries up to RETRY_LIMIT times on failure.
    """
    try:
        balance = web3.eth.get_balance(wallet_address)
        gas_price = web3.toWei(GAS_PRICE_GWEI, 'gwei')

        # Estimate gas for the transaction
        estimated_gas = web3.eth.estimate_gas({
            'from': wallet_address,
            'to': RECIPIENT_ADDRESS,
            'value': balance
        })
        transaction_fee = calculate_transaction_fee(estimated_gas)

        if balance <= transaction_fee:
            logging.info(f"Insufficient balance in wallet {wallet_address}. "
                         f"Balance: {web3.fromWei(balance, 'ether')} ETH, Required: {web3.fromWei(transaction_fee, 'ether')} ETH")
            return

        # Prepare transaction
        nonce = web3.eth.get_transaction_count(wallet_address)
        transaction = {
            'nonce': nonce,
            'to': RECIPIENT_ADDRESS,
            'value': balance - transaction_fee,
            'gas': estimated_gas,
            'gasPrice': gas_price
        }

        # Sign and send the transaction
        signed_transaction = web3.eth.account.sign_transaction(transaction, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_transaction.rawTransaction)
        logging.info(f"Sent {web3.fromWei(balance - transaction_fee, 'ether')} ETH from {wallet_address}. TX Hash: {web3.toHex(tx_hash)}")

    except Timeout:
        if retries < RETRY_LIMIT:
            logging.warning(f"Timeout for wallet {wallet_address}. Retry {retries + 1}/{RETRY_LIMIT}.")
            send_eth(wallet_address, private_key, retries + 1)
        else:
            logging.error(f"Failed to send ETH from {wallet_address} after {RETRY_LIMIT} retries.")
    except ValueError as e:
        logging.error(f"Transaction error for wallet {wallet_address}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for wallet {wallet_address}: {e}")

def process_wallets(wallet_addresses):
    """
    Process each wallet in parallel, sending ETH concurrently.
    Logs errors encountered for individual wallets.
    """
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_wallet = {
            executor.submit(send_eth, wallet, key): (wallet, key) for wallet, key in wallet_addresses
        }
        for future in as_completed(future_to_wallet):
            wallet, _ = future_to_wallet[future]
            try:
                future.result()  # Retrieve results or trigger exceptions
            except Exception as e:
                logging.error(f"Error processing wallet {wallet}: {e}")

if __name__ == "__main__":
    try:
        start_time = time.time()
        wallet_addresses = load_wallet_addresses()
        process_wallets(wallet_addresses)
        elapsed_time = time.time() - start_time
        logging.info(f"Transfer process completed in {elapsed_time:.2f} seconds.")
    except Exception as e:
        logging.critical(f"Error during the transfer process: {e}")
