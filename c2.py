import os
import threading
import random
import time
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox

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
        return None
    with open(filepath, "r") as file:
        try:
            balance = int(file.read().strip())
            return balance
        except ValueError:
            return None

def write_balance_atomic(account_id, new_balance):
    """Writes the new balance to the account file atomically."""
    filepath = get_account_filepath(account_id)
    temp_filepath = filepath + ".tmp"
    try:
        with open(temp_filepath, "w") as temp_file:
            temp_file.write(f"{new_balance}\n")
        os.replace(temp_filepath, filepath)
        return True
    except Exception as e:
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
            pass  # In a real-world scenario, handle this appropriately

def create_account(account_id, initial_balance, callback=None):
    """Creates a new account with the specified initial balance."""
    def task():
        lock = get_account_lock(account_id)
        with lock:
            filepath = get_account_filepath(account_id)
            if os.path.exists(filepath):
                if callback:
                    callback(f"Account '{account_id}' already exists.", False)
                return
            success = write_balance_atomic(account_id, initial_balance)
            if success:
                log_transaction_atomic("Create Account", account_id, f"Initial balance: {initial_balance}", "Success")
                if callback:
                    callback(f"Account '{account_id}' created successfully.", True)
            else:
                log_transaction_atomic("Create Account", account_id, f"Initial balance: {initial_balance}", "Failed")
                if callback:
                    callback(f"Failed to create account '{account_id}'.", False)
    
    threading.Thread(target=task).start()

def transfer(from_account, to_account, amount, callback=None):
    """Transfers amount from from_account to to_account atomically."""
    def task():
        if from_account == to_account:
            if callback:
                callback("Cannot transfer to the same account.", False)
            return

        ordered_accounts = sorted([from_account, to_account])
        lock1 = get_account_lock(ordered_accounts[0])
        lock2 = get_account_lock(ordered_accounts[1])

        with lock1:
            with lock2:
                balance_from = read_balance(from_account)
                balance_to = read_balance(to_account)

                if balance_from is None or balance_to is None:
                    if callback:
                        callback("One or both accounts do not exist.", False)
                    return

                if balance_from < amount:
                    if callback:
                        callback(f"Insufficient funds in account '{from_account}'. Current balance: {balance_from}", False)
                    return

                new_balance_from = balance_from - amount
                new_balance_to = balance_to + amount

                success_from = write_balance_atomic(from_account, new_balance_from)
                success_to = write_balance_atomic(to_account, new_balance_to)

                if success_from and success_to:
                    log_transaction_atomic("Transfer", from_account, f"To: {to_account}, Amount: {amount}", "Success")
                    log_transaction_atomic("Transfer", to_account, f"From: {from_account}, Amount: {amount}", "Success")
                    if callback:
                        callback(f"Transferred {amount} from '{from_account}' to '{to_account}'.", True)
                else:
                    # If any write failed, rollback
                    if success_from:
                        write_balance_atomic(from_account, balance_from)
                    if success_to:
                        write_balance_atomic(to_account, balance_to)
                    log_transaction_atomic("Transfer", from_account, f"To: {to_account}, Amount: {amount}", "Failed")
                    log_transaction_atomic("Transfer", to_account, f"From: {from_account}, Amount: {amount}", "Failed")
                    if callback:
                        callback(f"Transfer of {amount} from '{from_account}' to '{to_account}' failed and has been rolled back.", False)
    
    threading.Thread(target=task).start()

def deposit(account_id, amount, callback=None):
    """Deposits the specified amount into the account atomically."""
    def task():
        lock = get_account_lock(account_id)
        with lock:
            balance = read_balance(account_id)
            if balance is None:
                if callback:
                    callback(f"Deposit failed: Account '{account_id}' does not exist.", False)
                return
            new_balance = balance + amount
            success = write_balance_atomic(account_id, new_balance)
            if success:
                log_transaction_atomic("Deposit", account_id, f"Amount: {amount}", "Success")
                if callback:
                    callback(f"Deposited {amount} to '{account_id}'. New balance: {new_balance}", True)
            else:
                log_transaction_atomic("Deposit", account_id, f"Amount: {amount}", "Failed")
                if callback:
                    callback(f"Failed to deposit {amount} to '{account_id}'.", False)
    
    threading.Thread(target=task).start()

def withdraw(account_id, amount, callback=None):
    """Withdraws the specified amount from the account atomically."""
    def task():
        lock = get_account_lock(account_id)
        with lock:
            balance = read_balance(account_id)
            if balance is None:
                if callback:
                    callback(f"Withdrawal failed: Account '{account_id}' does not exist.", False)
                return
            if balance < amount:
                if callback:
                    callback(f"Withdrawal failed: Insufficient funds in account '{account_id}'. Current balance: {balance}", False)
                return
            new_balance = balance - amount
            success = write_balance_atomic(account_id, new_balance)
            if success:
                log_transaction_atomic("Withdraw", account_id, f"Amount: {amount}", "Success")
                if callback:
                    callback(f"Withdrew {amount} from '{account_id}'. New balance: {new_balance}", True)
            else:
                log_transaction_atomic("Withdraw", account_id, f"Amount: {amount}", "Failed")
                if callback:
                    callback(f"Failed to withdraw {amount} from '{account_id}'.", False)
    
    threading.Thread(target=task).start()

def view_balance(account_id, callback=None):
    """Displays the current balance of the account."""
    def task():
        balance = read_balance(account_id)
        if balance is not None:
            log_transaction_atomic("View Balance", account_id, f"Balance: {balance}", "Success")
            if callback:
                callback(f"Account '{account_id}' Balance: {balance}", True)
        else:
            log_transaction_atomic("View Balance", account_id, "Account does not exist", "Failed")
            if callback:
                callback(f"View balance failed: Account '{account_id}' does not exist.", False)
    
    threading.Thread(target=task).start()

def generate_central_log(callback=None):
    """Generates a central log of all account balances."""
    def task():
        central_log_path = os.path.join(ACCOUNTS_DIR, "central_log.txt")
        try:
            with open(central_log_path, "w") as log_file:
                log_file.write("Central Log - Account Balances\n")
                log_file.write("-" * 50 + "\n")
                with global_lock:
                    for account_file in os.listdir(ACCOUNTS_DIR):
                        if account_file.endswith(".txt") and not account_file.endswith("_log.txt") and account_file != "transactions.log":
                            account_id = account_file.replace(".txt", "")
                            balance = read_balance(account_id)
                            if balance is not None:
                                log_file.write(f"Account: {account_id}, Balance: {balance}\n")
            if callback:
                callback(f"Central log generated at: {central_log_path}", True)
        except Exception as e:
            if callback:
                callback(f"Error generating central log: {e}", False)
    
    threading.Thread(target=task).start()

# GUI Implementation using Tkinter
class BankApp:
    def __init__(self, master):
        self.master = master
        master.title("Bank Account Management System")
        master.geometry("400x400")
        
        # Create main frame
        self.main_frame = ttk.Frame(master, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Welcome Label
        self.welcome_label = ttk.Label(self.main_frame, text="Welcome to the Bank System", font=("Helvetica", 16))
        self.welcome_label.pack(pady=10)
        
        # Buttons for operations
        self.create_account_button = ttk.Button(self.main_frame, text="Create Account", command=self.create_account_window)
        self.create_account_button.pack(fill=tk.X, pady=5)
        
        self.transfer_funds_button = ttk.Button(self.main_frame, text="Transfer Funds", command=self.transfer_funds_window)
        self.transfer_funds_button.pack(fill=tk.X, pady=5)
        
        self.deposit_button = ttk.Button(self.main_frame, text="Deposit Funds", command=self.deposit_window)
        self.deposit_button.pack(fill=tk.X, pady=5)
        
        self.withdraw_button = ttk.Button(self.main_frame, text="Withdraw Funds", command=self.withdraw_window)
        self.withdraw_button.pack(fill=tk.X, pady=5)
        
        self.view_balance_button = ttk.Button(self.main_frame, text="View Account Balance", command=self.view_balance_window)
        self.view_balance_button.pack(fill=tk.X, pady=5)
        
        self.generate_log_button = ttk.Button(self.main_frame, text="Generate Central Log", command=self.generate_log)
        self.generate_log_button.pack(fill=tk.X, pady=5)
        
        self.exit_button = ttk.Button(self.main_frame, text="Exit", command=master.quit)
        self.exit_button.pack(fill=tk.X, pady=20)
    
    def create_account_window(self):
        window = tk.Toplevel(self.master)
        window.title("Create Account")
        window.geometry("300x200")
        
        frame = ttk.Frame(window, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="Account ID:").pack(pady=5)
        account_id_entry = ttk.Entry(frame)
        account_id_entry.pack(pady=5)
        
        ttk.Label(frame, text="Initial Balance:").pack(pady=5)
        initial_balance_entry = ttk.Entry(frame)
        initial_balance_entry.pack(pady=5)
        
        def submit():
            account_id = account_id_entry.get().strip()
            try:
                initial_balance = int(initial_balance_entry.get().strip())
                if initial_balance < 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Invalid Input", "Please enter a valid positive integer for balance.")
                return
            create_account(account_id, initial_balance, callback=lambda msg, success: self.display_message(msg, success))
        
        submit_button = ttk.Button(frame, text="Create", command=submit)
        submit_button.pack(pady=10)
    
    def transfer_funds_window(self):
        window = tk.Toplevel(self.master)
        window.title("Transfer Funds")
        window.geometry("300x250")
        
        frame = ttk.Frame(window, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="From Account ID:").pack(pady=5)
        from_account_entry = ttk.Entry(frame)
        from_account_entry.pack(pady=5)
        
        ttk.Label(frame, text="To Account ID:").pack(pady=5)
        to_account_entry = ttk.Entry(frame)
        to_account_entry.pack(pady=5)
        
        ttk.Label(frame, text="Amount:").pack(pady=5)
        amount_entry = ttk.Entry(frame)
        amount_entry.pack(pady=5)
        
        def submit():
            from_account = from_account_entry.get().strip()
            to_account = to_account_entry.get().strip()
            try:
                amount = int(amount_entry.get().strip())
                if amount <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Invalid Input", "Please enter a valid positive integer for amount.")
                return
            transfer(from_account, to_account, amount, callback=lambda msg, success: self.display_message(msg, success))
        
        submit_button = ttk.Button(frame, text="Transfer", command=submit)
        submit_button.pack(pady=10)
    
    def deposit_window(self):
        window = tk.Toplevel(self.master)
        window.title("Deposit Funds")
        window.geometry("300x200")
        
        frame = ttk.Frame(window, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="Account ID:").pack(pady=5)
        account_id_entry = ttk.Entry(frame)
        account_id_entry.pack(pady=5)
        
        ttk.Label(frame, text="Amount:").pack(pady=5)
        amount_entry = ttk.Entry(frame)
        amount_entry.pack(pady=5)
        
        def submit():
            account_id = account_id_entry.get().strip()
            try:
                amount = int(amount_entry.get().strip())
                if amount <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Invalid Input", "Please enter a valid positive integer for amount.")
                return
            deposit(account_id, amount, callback=lambda msg, success: self.display_message(msg, success))
        
        submit_button = ttk.Button(frame, text="Deposit", command=submit)
        submit_button.pack(pady=10)
    
    def withdraw_window(self):
        window = tk.Toplevel(self.master)
        window.title("Withdraw Funds")
        window.geometry("300x200")
        
        frame = ttk.Frame(window, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="Account ID:").pack(pady=5)
        account_id_entry = ttk.Entry(frame)
        account_id_entry.pack(pady=5)
        
        ttk.Label(frame, text="Amount:").pack(pady=5)
        amount_entry = ttk.Entry(frame)
        amount_entry.pack(pady=5)
        
        def submit():
            account_id = account_id_entry.get().strip()
            try:
                amount = int(amount_entry.get().strip())
                if amount <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Invalid Input", "Please enter a valid positive integer for amount.")
                return
            withdraw(account_id, amount, callback=lambda msg, success: self.display_message(msg, success))
        
        submit_button = ttk.Button(frame, text="Withdraw", command=submit)
        submit_button.pack(pady=10)
    
    def view_balance_window(self):
        window = tk.Toplevel(self.master)
        window.title("View Account Balance")
        window.geometry("300x150")
        
        frame = ttk.Frame(window, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="Account ID:").pack(pady=5)
        account_id_entry = ttk.Entry(frame)
        account_id_entry.pack(pady=5)
        
        def submit():
            account_id = account_id_entry.get().strip()
            view_balance(account_id, callback=lambda msg, success: self.display_message(msg, success))
        
        submit_button = ttk.Button(frame, text="View Balance", command=submit)
        submit_button.pack(pady=10)
    
    def generate_log(self):
        generate_central_log(callback=lambda msg, success: self.display_message(msg, success))
    
    def display_message(self, message, success):
        if success:
            messagebox.showinfo("Success", message)
        else:
            messagebox.showerror("Error", message)

def main():
    root = tk.Tk()
    app = BankApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
