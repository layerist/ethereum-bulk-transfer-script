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
    logging.critical("Failed to connect to the Ethereum network")
    raise ConnectionError("Failed to connect to the Ethereum network")


def load_wallet_addresses(file_path='wallets.txt'):
    """
    Load wallet addresses and private keys from a file.
    Each line in the file should be formatted as: address,private_key
    """
    try:
        with open(file_path, 'r') as file:
            wallets = file.read().strip().splitlines()
        return [wallet.split(',') for wallet in wallets if ',' in wallet]
    except FileNotFoundError:
        logging.critical("Wallet file not found")
        raise
    except Exception as e:
        logging.error(f"Error reading wallet file: {str(e)}")
        raise


def calculate_transaction_fee():
    """Calculate the transaction fee in Wei based on the set gas price and limit."""
    return web3.toWei(GAS_PRICE_GWEI, 'gwei') * GAS_LIMIT


def send_eth_from_wallet(wallet_address, private_key):
    """
    Send ETH from a wallet to the recipient address.
    This function handles signing and sending the transaction.
    """
    try:
        # Fetch wallet balance and calculate transaction fee
        balance = web3.eth.get_balance(wallet_address)
        transaction_fee = calculate_transaction_fee()

        if balance <= transaction_fee:
            logging.info(f'Insufficient balance in wallet {wallet_address}. Balance: {web3.fromWei(balance, "ether")} ETH')
            return

        # Build and sign the transaction
        nonce = web3.eth.get_transaction_count(wallet_address)
        gas_price = web3.toWei(GAS_PRICE_GWEI, 'gwei')
        value_to_send = balance - transaction_fee

        transaction = {
            'nonce': nonce,
            'to': RECIPIENT_ADDRESS,
            'value': value_to_send,
            'gas': GAS_LIMIT,
            'gasPrice': gas_price
        }

        signed_transaction = web3.eth.account.sign_transaction(transaction, private_key)

        # Send the transaction and log the result
        tx_hash = web3.eth.send_raw_transaction(signed_transaction.rawTransaction)
        logging.info(f'Transaction sent from {wallet_address}. TX Hash: {web3.toHex(tx_hash)}')

    except ValueError as e:
        # Web3 errors, typically related to nonce or transaction specifics
        logging.error(f'Web3 error while sending ETH from {wallet_address}: {str(e)}')
    except Exception as e:
        # Catch any unexpected errors
        logging.error(f'Unexpected error while sending ETH from {wallet_address}: {str(e)}')


def process_wallets(wallet_addresses):
    """
    Process each wallet using a thread pool to send ETH concurrently.
    """
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_wallet = {
            executor.submit(send_eth_from_wallet, wallet, key): (wallet, key)
            for wallet, key in wallet_addresses
        }

        for future in as_completed(future_to_wallet):
            wallet, key = future_to_wallet[future]
            try:
                future.result()  # Raise exception if the transaction failed
            except Exception as e:
                logging.error(f"Error processing wallet {wallet}: {str(e)}")


if __name__ == "__main__":
    try:
        start_time = time.time()
        wallet_addresses = load_wallet_addresses()

        if not wallet_addresses:
            logging.critical("No wallets found to process.")
            raise ValueError("No wallets loaded.")

        process_wallets(wallet_addresses)

        end_time = time.time()
        logging.info(f"Transfer process completed in {end_time - start_time:.2f} seconds")

    except Exception as e:
        logging.critical(f"An error occurred during the transfer process: {str(e)}")
