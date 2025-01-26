#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>

// Directory to store account files
#define ACCOUNTS_DIR "accounts"

// Maximum number of accounts
#define MAX_ACCOUNTS 100

// Name of the transaction log file
#define TRANSACTION_LOG "transactions.log"

// Structure representing an account
typedef struct {
    char account_id[50];
    pthread_mutex_t lock;
} Account;

// Array of accounts
Account accounts[MAX_ACCOUNTS];
int account_count = 0;

// Mutex for managing the accounts array
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;

// Mutex for the transaction log
pthread_mutex_t transaction_log_lock = PTHREAD_MUTEX_INITIALIZER;

// Path to the central transaction log
char central_transaction_log[100] = ACCOUNTS_DIR "/" TRANSACTION_LOG;

// Function to get the file path for an account
void get_account_filepath(const char *account_id, char *filepath) {
    sprintf(filepath, "%s/%s.txt", ACCOUNTS_DIR, account_id);
}

// Function to retrieve or create an account
Account* get_account(const char *account_id) {
    pthread_mutex_lock(&global_lock);
    for(int i = 0; i < account_count; i++) {
        if(strcmp(accounts[i].account_id, account_id) == 0) {
            pthread_mutex_unlock(&global_lock);
            return &accounts[i];
        }
    }
    // If account does not exist, create it
    if(account_count < MAX_ACCOUNTS) {
        strcpy(accounts[account_count].account_id, account_id);
        pthread_mutex_init(&accounts[account_count].lock, NULL);
        account_count++;
        pthread_mutex_unlock(&global_lock);
        return &accounts[account_count-1];
    }
    pthread_mutex_unlock(&global_lock);
    return NULL;
}

// Function to read the balance of an account
int read_balance(const char *account_id, int *balance) {
    char filepath[100];
    get_account_filepath(account_id, filepath);
    FILE *file = fopen(filepath, "r");
    if(file == NULL) {
        printf("Account %s does not exist.\n", account_id);
        return -1;
    }
    if(fscanf(file, "%d", balance) != 1) {
        printf("Invalid balance format for account %s.\n", account_id);
        fclose(file);
        return -1;
    }
    fclose(file);
    return 0;
}

// Function to write the balance to an account file atomically
int write_balance_atomic(const char *account_id, int new_balance) {
    char filepath[100];
    char temp_filepath[150];
    get_account_filepath(account_id, filepath);
    sprintf(temp_filepath, "%s.tmp", filepath);
    
    FILE *temp_file = fopen(temp_filepath, "w");
    if(temp_file == NULL) {
        printf("Error opening temporary file for account %s.\n", account_id);
        return -1;
    }
    fprintf(temp_file, "%d\n", new_balance);
    fclose(temp_file);
    
    // Replace the original file with the temporary file
    if(rename(temp_filepath, filepath) != 0) {
        printf("Error replacing file for account %s.\n", account_id);
        unlink(temp_filepath);
        return -1;
    }
    return 0;
}

// Function to log transactions atomically
void log_transaction_atomic(const char *operation_type, const char *user_id, const char *details, const char *status) {
    pthread_mutex_lock(&transaction_log_lock);
    FILE *log_file = fopen(central_transaction_log, "a");
    if(log_file == NULL) {
        printf("Error opening transaction log file.\n");
        pthread_mutex_unlock(&transaction_log_lock);
        return;
    }
    // Get current time
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char timestamp[20];
    strftime(timestamp, sizeof(timestamp)-1, "%Y-%m-%d %H:%M:%S", t);
    
    fprintf(log_file, "%s | %s | %s | %s | %s\n", timestamp, operation_type, user_id, details, status);
    fclose(log_file);
    pthread_mutex_unlock(&transaction_log_lock);
}

// Function to create a new account
void create_account(const char *account_id, int initial_balance) {
    Account *account = get_account(account_id);
    if(account == NULL) {
        printf("Error creating account %s.\n", account_id);
        log_transaction_atomic("Create Account", account_id, "Initial balance", "Failed");
        return;
    }
    
    pthread_mutex_lock(&account->lock);
    char filepath[100];
    get_account_filepath(account_id, filepath);
    
    // Check if account file already exists
    FILE *file = fopen(filepath, "r");
    if(file != NULL) {
        printf("Account %s already exists.\n", account_id);
        fclose(file);
        pthread_mutex_unlock(&account->lock);
        log_transaction_atomic("Create Account", account_id, "Initial balance", "Failed");
        return;
    }
    
    // Write initial balance atomically
    if(write_balance_atomic(account_id, initial_balance) == 0) {
        printf("Account %s created with initial balance %d.\n", account_id, initial_balance);
        log_transaction_atomic("Create Account", account_id, "Initial balance", "Success");
    } else {
        printf("Failed to create account %s.\n", account_id);
        log_transaction_atomic("Create Account", account_id, "Initial balance", "Failed");
    }
    pthread_mutex_unlock(&account->lock);
}

// Function to transfer funds atomically
void transfer(const char *from_account_id, const char *to_account_id, int amount) {
    if(strcmp(from_account_id, to_account_id) == 0) {
        printf("Cannot transfer to the same account.\n");
        log_transaction_atomic("Transfer", from_account_id, "Attempted to transfer to self", "Failed");
        return;
    }
    
    // Order accounts to prevent deadlock
    const char *first, *second;
    if(strcmp(from_account_id, to_account_id) < 0) {
        first = from_account_id;
        second = to_account_id;
    } else {
        first = to_account_id;
        second = from_account_id;
    }
    
    Account *first_account = get_account(first);
    Account *second_account = get_account(second);
    
    if(first_account == NULL || second_account == NULL) {
        printf("One or both accounts (%s or %s) do not exist.\n", from_account_id, to_account_id);
        log_transaction_atomic("Transfer", from_account_id, "One or both accounts do not exist", "Failed");
        return;
    }
    
    // Lock both accounts in order
    pthread_mutex_lock(&first_account->lock);
    pthread_mutex_lock(&second_account->lock);
    
    int balance_from, balance_to;
    if(read_balance(from_account_id, &balance_from) != 0 ||
       read_balance(to_account_id, &balance_to) != 0) {
        printf("Error reading account balances.\n");
        log_transaction_atomic("Transfer", from_account_id, "Reading balances failed", "Failed");
        pthread_mutex_unlock(&second_account->lock);
        pthread_mutex_unlock(&first_account->lock);
        return;
    }
    
    if(balance_from < amount) {
        printf("Transfer failed: Insufficient funds in account %s. Current balance: %d\n", from_account_id, balance_from);
        log_transaction_atomic("Transfer", from_account_id, "Insufficient funds", "Failed");
        pthread_mutex_unlock(&second_account->lock);
        pthread_mutex_unlock(&first_account->lock);
        return;
    }
    
    int new_balance_from = balance_from - amount;
    int new_balance_to = balance_to + amount;
    
    // Write new balances atomically
    if(write_balance_atomic(from_account_id, new_balance_from) == 0 &&
       write_balance_atomic(to_account_id, new_balance_to) == 0) {
        printf("Transferred %d from %s to %s.\n", amount, from_account_id, to_account_id);
        log_transaction_atomic("Transfer", from_account_id, "Transfer successful", "Success");
        log_transaction_atomic("Transfer", to_account_id, "Transfer received", "Success");
    } else {
        // Rollback in case of failure
        write_balance_atomic(from_account_id, balance_from);
        write_balance_atomic(to_account_id, balance_to);
        printf("Transfer from %s to %s failed and has been rolled back.\n", from_account_id, to_account_id);
        log_transaction_atomic("Transfer", from_account_id, "Transfer failed and rolled back", "Failed");
        log_transaction_atomic("Transfer", to_account_id, "Transfer failed and rolled back", "Failed");
    }
    
    pthread_mutex_unlock(&second_account->lock);
    pthread_mutex_unlock(&first_account->lock);
}

// Function to deposit funds into an account
void deposit(const char *account_id, int amount) {
    Account *account = get_account(account_id);
    if(account == NULL) {
        printf("Deposit failed: Account %s does not exist.\n", account_id);
        log_transaction_atomic("Deposit", account_id, "Account does not exist", "Failed");
        return;
    }
    
    pthread_mutex_lock(&account->lock);
    int balance;
    if(read_balance(account_id, &balance) != 0) {
        printf("Error reading balance for account %s.\n", account_id);
        log_transaction_atomic("Deposit", account_id, "Reading balance failed", "Failed");
        pthread_mutex_unlock(&account->lock);
        return;
    }
    
    int new_balance = balance + amount;
    if(write_balance_atomic(account_id, new_balance) == 0) {
        printf("Deposited %d to account %s. New balance: %d\n", amount, account_id, new_balance);
        log_transaction_atomic("Deposit", account_id, "Deposit successful", "Success");
    } else {
        printf("Failed to deposit %d to account %s.\n", amount, account_id);
        log_transaction_atomic("Deposit", account_id, "Deposit failed", "Failed");
    }
    pthread_mutex_unlock(&account->lock);
}

// Function to withdraw funds from an account
void withdraw(const char *account_id, int amount) {
    Account *account = get_account(account_id);
    if(account == NULL) {
        printf("Withdrawal failed: Account %s does not exist.\n", account_id);
        log_transaction_atomic("Withdraw", account_id, "Account does not exist", "Failed");
        return;
    }
    
    pthread_mutex_lock(&account->lock);
    int balance;
    if(read_balance(account_id, &balance) != 0) {
        printf("Error reading balance for account %s.\n", account_id);
        log_transaction_atomic("Withdraw", account_id, "Reading balance failed", "Failed");
        pthread_mutex_unlock(&account->lock);
        return;
    }
    
    if(balance < amount) {
        printf("Withdrawal failed: Insufficient funds in account %s. Current balance: %d\n", account_id, balance);
        log_transaction_atomic("Withdraw", account_id, "Insufficient funds", "Failed");
        pthread_mutex_unlock(&account->lock);
        return;
    }
    
    int new_balance = balance - amount;
    if(write_balance_atomic(account_id, new_balance) == 0) {
        printf("Withdrew %d from account %s. New balance: %d\n", amount, account_id, new_balance);
        log_transaction_atomic("Withdraw", account_id, "Withdrawal successful", "Success");
    } else {
        printf("Failed to withdraw %d from account %s.\n", amount, account_id);
        log_transaction_atomic("Withdraw", account_id, "Withdrawal failed", "Failed");
    }
    pthread_mutex_unlock(&account->lock);
}

// Function to view the balance of an account
void view_balance(const char *account_id) {
    Account *account = get_account(account_id);
    if(account == NULL) {
        printf("View balance failed: Account %s does not exist.\n", account_id);
        log_transaction_atomic("View Balance", account_id, "Account does not exist", "Failed");
        return;
    }
    
    pthread_mutex_lock(&account->lock);
    int balance;
    if(read_balance(account_id, &balance) != 0) {
        printf("Error reading balance for account %s.\n", account_id);
        log_transaction_atomic("View Balance", account_id, "Reading balance failed", "Failed");
        pthread_mutex_unlock(&account->lock);
        return;
    }
    
    printf("Account %s Balance: %d\n", account_id, balance);
    log_transaction_atomic("View Balance", account_id, "Balance viewed", "Success");
    pthread_mutex_unlock(&account->lock);
}

// Function to generate a central log of all account balances
void generate_central_log() {
    char central_log_path[100];
    sprintf(central_log_path, "%s/central_log.txt", ACCOUNTS_DIR);
    FILE *log_file = fopen(central_log_path, "w");
    if(log_file == NULL) {
        printf("Error creating central log.\n");
        return;
    }
    fprintf(log_file, "Central Log - Account Balances\n");
    fprintf(log_file, "--------------------------------------------------\n");
    
    pthread_mutex_lock(&global_lock);
    for(int i = 0; i < account_count; i++) {
        pthread_mutex_lock(&accounts[i].lock);
        int balance;
        if(read_balance(accounts[i].account_id, &balance) == 0) {
            fprintf(log_file, "Account: %s, Balance: %d\n", accounts[i].account_id, balance);
        }
        pthread_mutex_unlock(&accounts[i].lock);
    }
    pthread_mutex_unlock(&global_lock);
    
    fclose(log_file);
    printf("Central log created at: %s\n", central_log_path);
}

// Structure representing a user operation
typedef struct {
    char user_id[50];
    char operation[20];
    char target_account[50];
    int amount;
} UserOperation;

// Thread function to perform user operations
void* user_operations(void *arg) {
    UserOperation *op = (UserOperation*)arg;
    if(strcmp(op->operation, "transfer") == 0) {
        transfer(op->user_id, op->target_account, op->amount);
    }
    else if(strcmp(op->operation, "deposit") == 0) {
        deposit(op->user_id, op->amount);
    }
    else if(strcmp(op->operation, "withdraw") == 0) {
        withdraw(op->user_id, op->amount);
    }
    else if(strcmp(op->operation, "view_balance") == 0) {
        view_balance(op->user_id);
    }
    // Simulate delay
    usleep((rand() % 400 + 100) * 1000); // 100 to 500 milliseconds
    free(op);
    pthread_exit(NULL);
}

// Main function
int main() {
    // Ensure the accounts directory exists
    struct stat st = {0};
    if (stat(ACCOUNTS_DIR, &st) == -1) {
        mkdir(ACCOUNTS_DIR, 0700);
    }
    
    // Set the path for the central transaction log
    strcpy(central_transaction_log, ACCOUNTS_DIR "/" TRANSACTION_LOG);
    
    // List of user IDs
    char *user_ids[] = {"User1", "User2", "User3"};
    int num_users = sizeof(user_ids)/sizeof(user_ids[0]);
    
    // Initial balance for each account
    int initial_balance = 1000;
    
    // Create user accounts
    printf("Creating user accounts...\n");
    for(int i = 0; i < num_users; i++) {
        create_account(user_ids[i], initial_balance);
    }
    printf("All accounts created.\n\n");
    
    // Define threads
    pthread_t threads[10];
    int thread_count = 0;
    
    // Define user operations
    // Example: Transfer from User1 to User2, transfer from User2 to User3, transfer from User3 to User1
    UserOperation *op1 = malloc(sizeof(UserOperation));
    strcpy(op1->user_id, "User1");
    strcpy(op1->operation, "transfer");
    strcpy(op1->target_account, "User2");
    op1->amount = 500;
    pthread_create(&threads[thread_count++], NULL, user_operations, (void*)op1);
    
    UserOperation *op2 = malloc(sizeof(UserOperation));
    strcpy(op2->user_id, "User2");
    strcpy(op2->operation, "transfer");
    strcpy(op2->target_account, "User3");
    op2->amount = 300;
    pthread_create(&threads[thread_count++], NULL, user_operations, (void*)op2);
    
    UserOperation *op3 = malloc(sizeof(UserOperation));
    strcpy(op3->user_id, "User3");
    strcpy(op3->operation, "transfer");
    strcpy(op3->target_account, "User1");
    op3->amount = 200;
    pthread_create(&threads[thread_count++], NULL, user_operations, (void*)op3);
    
    // Additional operations can be added here
    // For example:
    // UserOperation *op4 = malloc(sizeof(UserOperation));
    // strcpy(op4->user_id, "User1");
    // strcpy(op4->operation, "deposit");
    // op4->amount = 150;
    // pthread_create(&threads[thread_count++], NULL, user_operations, (void*)op4);
    
    // Wait for all threads to complete
    for(int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Generate central log after all operations
    generate_central_log();
    printf("All operations completed.\n");
    
    // Destroy mutexes
    for(int i = 0; i < account_count; i++) {
        pthread_mutex_destroy(&accounts[i].lock);
    }
    pthread_mutex_destroy(&global_lock);
    pthread_mutex_destroy(&transaction_log_lock);
    
    return 0;
}
