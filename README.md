# Toy Payments Engine

## Usage

```bash
cargo run -- transactions.csv >accounts.out
```

## Design

The `Account` type implements the transaction logic. The `handle_*` methods assume that transactions are well-formed: a deposit or a withdrawal with `amount` set to `None` will result in a panic.

Since the `Transaction` type can represent a malformed transaction—a deposit or a withdrawal with no amount present—and validation cannot be implemented during deserialization due to the limitations of Serde's API, an additional check needs to be made before a `Transaction` is handed over to the `Account`. This is done in the `process_csv` function.

If a transaction fails, the `handle_*` methods return an `anyhow::Error`. Errors returned from the `handle_*` methods are reported to standard error.

## Testing

Unit tests in `account.rs` check the transaction logic. Unit test in `main.rs` is an end-to-end test that checks CSV parsing and output generation.
