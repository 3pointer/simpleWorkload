# Bank Workload

A MySQL/TiDB workload simulator that performs banking transactions and balance verifications.

## Overview

This workload simulates a banking system with the following characteristics:

- Creates accounts with initial balances
- Performs concurrent money transfers between accounts
- Provides continuous verification that the total balance remains consistent
- Supports high and low contention modes

## Features

- Customizable number of accounts and concurrency level
- Configurable test duration and verification interval
- Transaction logs including transaction history and legs
- Support for TiDB-specific features like TiFlash

## Installation

```bash
# Clone the repository
git clone https://github.com/3pointer/simpleWorkload.git
cd simpleWorkload

# Install dependencies
go mod tidy

# Build the project
go build
```

## Usage

```bash
# Run with default parameters
./main

# Run with custom parameters
./main -dsn "root:password@tcp(127.0.0.1:4000)/test" -accounts 5000 -concurrency 100 -duration 30m

# Available flags:
#   -dsn string        Database connection string (default "root:@tcp(127.0.0.1:4000)/test")
#   -accounts int      Number of bank accounts (default 1000)
#   -concurrency int   Number of concurrent workers (default 50)
#   -duration duration Test duration (default 10m0s)
#   -interval duration Verification interval (default 2s)
#   -contention string Contention level: low, high (default "low")
```

## Database Schema

The workload uses three tables:

1. `bank2_accounts` - Stores account information and balances
2. `bank2_transaction` - Records transaction metadata
3. `bank2_transaction_leg` - Records individual legs of each transaction

## Example Output

```
2025/03/18 12:00:00 Start bank workload
2025/03/18 12:00:00 Setting up bank tables and accounts...
2025/03/18 12:00:05 Created 1000 accounts with initial balance 1000
2025/03/18 12:00:05 Starting bank transfers...
2025/03/18 12:00:07 Balance verification passed, total=2000000
2025/03/18 12:10:05 Runtime completed, stopping workload
```

## License

MIT
