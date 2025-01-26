import os
import threading
import random
import time
from datetime import datetime

# Directory to store account files
ACCOUNTS_DIR = "accounts"

# Ensure the accounts directory exists
if not os.path.exists(ACCOUNTS_DIR):
    os.makedirs(ACCOUNTS_DIR)

# Dictionary to hold locks for each account
account_locks = {}
global_lock = threading.RLock()

# Lock for the central transaction log
transaction_log_lock = threading.Lock()

# Path for the central transaction log
CENTRAL_TRANSACTION_LOG = os.path.join(ACCOUNTS_DIR, "transactions.log")

def get_account_filepath(account_id):
    """Returns the file path for a given account."""
    return os.path.join(ACCOUNTS_DIR, f"{account_id}.txt")

def get_account_lock(account_id):
    """Retrieves or creates a lock for the given account."""
    with global_lock:
        if account_id not in account_locks:
            account_locks[account_id] = threading.RLock()
        return account_locks[account_id]

def read_balance(account_id):
    """Reads the balance of the specified account."""
    filepath = get_account_filepath(account_id)
    if not os.path.exists(filepath):
        print(f"Account {account_id} does not exist.")
        return None
    with open(filepath, "r") as file:
        try:
            balance = int(file.read().strip())
            return balance
        except ValueError:
            print(f"Invalid balance format for account {account_id}.")
            return None

def write_balance_atomic(account_id, new_balance):
    
    filepath = get_account_filepath(account_id)
    temp_filepath = filepath + ".tmp"
    try:
        with open(temp_filepath, "w") as temp_file:
            temp_file.write(f"{new_balance}\n")
        os.replace(temp_filepath, filepath)
        return True
    except Exception as e:
        print(f"Error writing balance for {account_id}: {e}")
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
        return False

def log_transaction_atomic(operation_type, user_id, details, status):
    """Logs a transaction to the central transaction log."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"{timestamp} | {operation_type} | {user_id} | {details} | {status}\n"
    with transaction_log_lock:
        try:
            with open(CENTRAL_TRANSACTION_LOG, "a") as log_file:
                log_file.write(log_entry)
        except Exception as e:
            print(f"Error logging transaction: {e}")

def create_account(account_id, initial_balance):
    """Creates a new account with the specified initial balance."""
    lock = get_account_lock(account_id)
    with lock:
        filepath = get_account_filepath(account_id)
        if os.path.exists(filepath):
            print(f"Account {account_id} already exists.")
            return
        success = write_balance_atomic(account_id, initial_balance)
        if success:
            log_transaction_atomic("Create Account", account_id, f"Initial balance: {initial_balance}", "Success")
            print(f"Account {account_id} created successfully.")
        else:
            log_transaction_atomic("Create Account", account_id, f"Initial balance: {initial_balance}", "Failed")
            print(f"Failed to create account {account_id}.")

def transfer(from_account, to_account, amount):
    """Transfers amount from from_account to to_account atomically."""
    if from_account == to_account:
        print("Cannot transfer to the same account.")
        log_transaction_atomic("Transfer", from_account, f"Attempted to transfer to self: {amount}", "Failed")
        return

    ordered_accounts = sorted([from_account, to_account])
    lock1 = get_account_lock(ordered_accounts[0])
    lock2 = get_account_lock(ordered_accounts[1])

    with lock1:
        with lock2:
            try:
                balance_from = read_balance(from_account)
                balance_to = read_balance(to_account)

                if balance_from is None or balance_to is None:
                    print(f"One or both accounts ({from_account}, {to_account}) do not exist.")
                    log_transaction_atomic("Transfer", from_account, f"To: {to_account}, Amount: {amount}", "Failed")
                    return

                if balance_from < amount:
                    print(f"Transfer failed: Insufficient funds in account {from_account}. Current balance: {balance_from}")
                    log_transaction_atomic("Transfer", from_account, f"To: {to_account}, Amount: {amount}", "Failed")
                    return

            
                new_balance_from = balance_from - amount
                new_balance_to = balance_to + amount

               
                success_from = write_balance_atomic(from_account, new_balance_from)
                success_to = write_balance_atomic(to_account, new_balance_to)

                if success_from and success_to:
                    log_transaction_atomic("Transfer", from_account, f"To: {to_account}, Amount: {amount}", "Success")
                    log_transaction_atomic("Transfer", to_account, f"From: {from_account}, Amount: {amount}", "Success")
                    print(f"Transferred {amount} from {from_account} to {to_account}.")
                else:
                    # If any write failed, rollback
                    if success_from:
                        # Attempt to rollback from_account
                        write_balance_atomic(from_account, balance_from)
                    if success_to:
                        # Attempt to rollback to_account
                        write_balance_atomic(to_account, balance_to)
                    log_transaction_atomic("Transfer", from_account, f"To: {to_account}, Amount: {amount}", "Failed")
                    log_transaction_atomic("Transfer", to_account, f"From: {from_account}, Amount: {amount}", "Failed")
                    print(f"Transfer of {amount} from {from_account} to {to_account} failed and has been rolled back.")

            except Exception as e:
                print(f"Error during transfer: {e}")
                log_transaction_atomic("Transfer", from_account, f"To: {to_account}, Amount: {amount}", "Failed")

def deposit(account_id, amount):
    """Deposits the specified amount into the account atomically."""
    lock = get_account_lock(account_id)
    with lock:
        try:
            balance = read_balance(account_id)
            if balance is None:
                print(f"Deposit failed: Account {account_id} does not exist.")
                log_transaction_atomic("Deposit", account_id, f"Amount: {amount}", "Failed")
                return
            new_balance = balance + amount
            success = write_balance_atomic(account_id, new_balance)
            if success:
                log_transaction_atomic("Deposit", account_id, f"Amount: {amount}", "Success")
                print(f"Deposited {amount} to {account_id}. New balance: {new_balance}")
            else:
                log_transaction_atomic("Deposit", account_id, f"Amount: {amount}", "Failed")
                print(f"Failed to deposit {amount} to {account_id}.")
        except Exception as e:
            print(f"Error during deposit: {e}")
            log_transaction_atomic("Deposit", account_id, f"Amount: {amount}", "Failed")

def withdraw(account_id, amount):
    """Withdraws the specified amount from the account atomically."""
    lock = get_account_lock(account_id)
    with lock:
        try:
            balance = read_balance(account_id)
            if balance is None:
                print(f"Withdrawal failed: Account {account_id} does not exist.")
                log_transaction_atomic("Withdraw", account_id, f"Amount: {amount}", "Failed")
                return
            if balance < amount:
                print(f"Withdrawal failed: Insufficient funds in account {account_id}. Current balance: {balance}")
                log_transaction_atomic("Withdraw", account_id, f"Amount: {amount}", "Failed")
                return
            new_balance = balance - amount
            success = write_balance_atomic(account_id, new_balance)
            if success:
                log_transaction_atomic("Withdraw", account_id, f"Amount: {amount}", "Success")
                print(f"Withdrew {amount} from {account_id}. New balance: {new_balance}")
            else:
                log_transaction_atomic("Withdraw", account_id, f"Amount: {amount}", "Failed")
                print(f"Failed to withdraw {amount} from {account_id}.")
        except Exception as e:
            print(f"Error during withdrawal: {e}")
            log_transaction_atomic("Withdraw", account_id, f"Amount: {amount}", "Failed")

def view_balance(account_id):
    """Displays the current balance of the account."""
    try:
        balance = read_balance(account_id)
        if balance is not None:
            print(f"Account {account_id} Balance: {balance}")
            log_transaction_atomic("View Balance", account_id, f"Balance: {balance}", "Success")
    except Exception as e:
        print(f"Error viewing balance: {e}")
        log_transaction_atomic("View Balance", account_id, f"Error: {e}", "Failed")

def generate_central_log():
    """Generates a central log of all account balances."""
    central_log_path = os.path.join(ACCOUNTS_DIR, "central_log.txt")
    with open(central_log_path, "w") as log_file:
        log_file.write("Central Log - Account Balances\n")
        log_file.write("-" * 50 + "\n")
        for account_file in os.listdir(ACCOUNTS_DIR):
            if account_file.endswith(".txt") and not account_file.endswith("_log.txt") and account_file != "transactions.log":
                account_id = account_file.replace(".txt", "")
                balance = read_balance(account_id)
                if balance is not None:
                    log_file.write(f"Account: {account_id}, Balance: {balance}\n")
    print(f"Central log generated at: {central_log_path}")

def user_operations(user_id, all_users):
    """Performs operations for a user: transfers, deposits, withdrawals, and view balance."""
    # Number of operations per user
    num_operations = 10

    for _ in range(num_operations):
        operation = random.choice(['transfer', 'deposit', 'withdraw', 'view_balance'])

        if operation == 'transfer':
            # Select a random target account excluding self
            target_account = random.choice([user for user in all_users if user != user_id])
            amount = random.randint(10, 1000)  # Random transfer amount
            transfer(user_id, target_account, amount)

        elif operation == 'deposit':
            amount = random.randint(10, 500)  # Random deposit amount
            deposit(user_id, amount)

        elif operation == 'withdraw':
            amount = random.randint(10, 500)  # Random withdrawal amount
            withdraw(user_id, amount)

        elif operation == 'view_balance':
            view_balance(user_id)

        # Random sleep to simulate real-world operations
        time.sleep(random.uniform(0.1, 0.5))

def main():
    # List of all user IDs
    user_ids = [f"User{i}" for i in range(1, 4)]  # User1 to User10

    # Initial balance for each account
    initial_balance = 1000

    # Pre-create all accounts before starting threads
    print("Creating all user accounts...")
    for user_id in user_ids:
        create_account(user_id, initial_balance)
    print("All accounts created.\n")

    # Start threads for each user
    threads = []
    for user_id in user_ids:
         thread = threading.Thread(target=user_operations, args=(user_id, user_ids))
         threads.append(thread)
         thread.start()

  

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Generate central log after all operations
    generate_central_log()
    print("All operations completed.")

if __name__ == "__main__":
    main()
