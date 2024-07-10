# Ethereum Bulk Transfer Script

This script uses the `web3` library to transfer all ETH from multiple Ethereum wallets to a specified recipient wallet. It supports multi-threading to speed up the process and logs all activities.

## Requirements

- Python 3.x
- `web3` library
- An Ethereum node provider (e.g., Infura)

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/layerist/ethereum-bulk-transfer.git
    cd ethereum-bulk-transfer
    ```

2. Install the required Python packages:
    ```sh
    pip install web3
    ```

## Configuration

1. Replace `YOUR_INFURA_URL` in the script with your actual Infura project URL or another Ethereum node provider URL.

2. Replace `RECIPIENT_ETH_ADDRESS` with the address to which you want to send all ETH.

3. Adjust `GAS_PRICE_GWEI` if needed.

## Usage

1. Create a `wallets.txt` file in the same directory as the script, containing wallet addresses and their private keys separated by a comma:

    ```
    0xYourWalletAddress1,YourPrivateKey1
    0xYourWalletAddress2,YourPrivateKey2
    ...
    ```

2. Run the script:
    ```sh
    python transfer_eth.py
    ```

3. Check `transfer_log.txt` for detailed logs of the transfer process.

## Notes

- Ensure that the private keys are kept secure and never exposed to the public.
- Use at your own risk. Test with small amounts of ETH first.
- The script currently assumes a fixed gas price. You may want to implement dynamic gas price fetching for better efficiency.

## License

This project is licensed under the MIT License.
