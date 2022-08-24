use std::collections::BTreeMap;

use anyhow::{bail, Result};
use serde::{Serialize, Serializer};

use crate::transaction::{Transaction, TransactionType};
use crate::types::{Amount, ClientId, TransactionId};

#[derive(Debug, Serialize)]
pub struct Account {
    #[serde(rename = "client")]
    pub client_id: ClientId,
    #[serde(skip_serializing)]
    pub transactions: BTreeMap<TransactionId, Transaction>,
    #[serde(serialize_with = "serialize_amount")]
    pub available: Amount,
    #[serde(serialize_with = "serialize_amount")]
    pub held: Amount,
    #[serde(serialize_with = "serialize_amount")]
    pub total: Amount,
    pub locked: bool,
}

fn serialize_amount<S>(amount: &Amount, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{:.4}", amount))
}

impl Account {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            transactions: BTreeMap::new(),
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
        }
    }

    fn handle_deposit(&mut self, transaction: Transaction) -> Result<()> {
        let amount = transaction.amount.expect("malformed transaction");
        self.available += amount;
        self.total += amount;
        self.transactions
            .insert(transaction.transaction_id, transaction);
        Ok(())
    }

    fn handle_withdraw(&mut self, transaction: Transaction) -> Result<()> {
        let amount = transaction.amount.expect("malformed transaction");

        if self.available < amount {
            bail!("withdraw error: insufficient funds");
        }

        self.available -= amount;
        self.total -= amount;
        self.transactions
            .insert(transaction.transaction_id, transaction);
        Ok(())
    }

    fn handle_dispute(&mut self, transaction: Transaction) -> Result<()> {
        match self.transactions.get_mut(&transaction.transaction_id) {
            Some(referenced_transaction) => {
                if referenced_transaction.under_dispute {
                    bail!(
                        "dispute error: transaction {} is already under dispute",
                        referenced_transaction.transaction_id
                    );
                }

                match referenced_transaction.transaction_type {
                    TransactionType::Deposit => {
                        let amount = referenced_transaction
                            .amount
                            .expect("malformed transaction");

                        if self.available < amount {
                            bail!("dispute error: insufficient funds");
                        }

                        self.available -= amount;
                        self.held += amount;
                    }
                    TransactionType::Withdrawal => {
                        bail!("dispute error: withdrawal is not a valid target");
                    }
                    _ => panic!("the 'impossible' happened"),
                }

                referenced_transaction.under_dispute = true;
            }
            None => {
                bail!(format!(
                    "dispute error: transaction {} not found for client {}",
                    transaction.transaction_id, transaction.client_id
                ));
            }
        }

        Ok(())
    }

    fn handle_resolve(&mut self, transaction: Transaction) -> Result<()> {
        match self.transactions.get_mut(&transaction.transaction_id) {
            Some(referenced_transaction) => {
                if !referenced_transaction.under_dispute {
                    bail!(
                        "resolve error: transaction {} is not under dispute",
                        referenced_transaction.transaction_id
                    );
                }

                match referenced_transaction.transaction_type {
                    TransactionType::Deposit => {
                        let amount = referenced_transaction
                            .amount
                            .expect("malformed transaction");
                        self.available += amount;
                        self.held -= amount;
                    }
                    TransactionType::Withdrawal => {
                        bail!("resolve error: withdrawal is not a valid target");
                    }
                    _ => panic!("the 'impossible' happened"),
                }

                referenced_transaction.under_dispute = false;
            }
            None => {
                bail!(format!(
                    "resolve error: transaction {} not found for client {}",
                    transaction.transaction_id, transaction.client_id
                ));
            }
        }

        Ok(())
    }

    fn handle_chargeback(&mut self, transaction: Transaction) -> Result<()> {
        match self.transactions.get_mut(&transaction.transaction_id) {
            Some(referenced_transaction) => {
                if !referenced_transaction.under_dispute {
                    bail!(
                        "chargeback error: transaction {} is not under dispute",
                        referenced_transaction.transaction_id
                    );
                }

                let amount = referenced_transaction
                    .amount
                    .expect("malformed transaction");
                self.held -= amount;
                self.total -= amount;
                self.locked = true;
            }
            None => {
                bail!(format!(
                    "chargeback error: transaction {} not found for client {}",
                    transaction.transaction_id, transaction.client_id
                ));
            }
        }

        Ok(())
    }

    pub fn execute(&mut self, transaction: Transaction) -> Result<()> {
        if self.locked {
            bail!("account locked");
        }

        match transaction.transaction_type {
            TransactionType::Deposit => self.handle_deposit(transaction)?,
            TransactionType::Withdrawal => self.handle_withdraw(transaction)?,
            TransactionType::Dispute => self.handle_dispute(transaction)?,
            TransactionType::Resolve => self.handle_resolve(transaction)?,
            TransactionType::Chargeback => self.handle_chargeback(transaction)?,
        }

        self.check_consistency();
        Ok(())
    }

    fn check_consistency(&self) {
        let eps = 0.0001;
        assert!((self.total - (self.available + self.held)).abs() < eps);
        assert!((self.held - (self.total - self.available)).abs() < eps);
        assert!((self.available - (self.total - self.held)).abs() < eps);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    fn deposit(client_id: ClientId, amount: Amount) -> Transaction {
        Transaction {
            client_id,
            transaction_type: TransactionType::Deposit,
            transaction_id: next_transaction_id(),
            amount: Some(amount),
            under_dispute: false,
        }
    }

    fn withdrawal(client_id: ClientId, amount: Amount) -> Transaction {
        Transaction {
            client_id,
            transaction_type: TransactionType::Withdrawal,
            transaction_id: next_transaction_id(),
            amount: Some(amount),
            under_dispute: false,
        }
    }

    fn dispute(client_id: ClientId, transaction_id: TransactionId) -> Transaction {
        Transaction {
            client_id,
            transaction_type: TransactionType::Dispute,
            transaction_id,
            amount: None,
            under_dispute: false,
        }
    }

    fn resolve(client_id: ClientId, transaction_id: TransactionId) -> Transaction {
        Transaction {
            client_id,
            transaction_type: TransactionType::Resolve,
            transaction_id,
            amount: None,
            under_dispute: false,
        }
    }

    fn chargeback(client_id: ClientId, transaction_id: TransactionId) -> Transaction {
        Transaction {
            client_id,
            transaction_type: TransactionType::Chargeback,
            transaction_id,
            amount: None,
            under_dispute: false,
        }
    }

    fn check(account: &Account, available: Amount, held: Amount, total: Amount, locked: bool) {
        assert_eq!(account.available, available);
        assert_eq!(account.held, held);
        assert_eq!(account.total, total);
        assert_eq!(account.locked, locked);
    }

    fn next_transaction_id() -> TransactionId {
        static NEXT_TRANSACTION_ID: AtomicU32 = AtomicU32::new(0);
        NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn deposit_withdraw_flow() {
        let client_id = 0;
        let mut account = Account::new(client_id);
        assert!(account.execute(deposit(client_id, 2.0)).is_ok());
        check(&account, 2.0, 0.0, 2.0, false);
        assert!(account.execute(deposit(client_id, 3.0)).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        assert!(account.execute(withdrawal(client_id, 1.0)).is_ok());
        check(&account, 4.0, 0.0, 4.0, false);
    }

    #[test]
    fn withdrawal_should_not_result_in_negative_balance() {
        let client_id = 0;
        let mut account = Account::new(client_id);
        assert!(account.execute(deposit(client_id, 2.0)).is_ok());
        check(&account, 2.0, 0.0, 2.0, false);
        assert!(account.execute(withdrawal(client_id, 3.0)).is_err());
        check(&account, 2.0, 0.0, 2.0, false);
    }

    #[test]
    fn dispute_of_disputed_transaction_should_fail() {
        let client_id = 0;
        let mut account = Account::new(client_id);
        let deposit_1 = deposit(client_id, 5.0);
        assert!(account.execute(deposit_1.clone()).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        assert!(account
            .execute(dispute(client_id, deposit_1.transaction_id))
            .is_ok());
        assert!(account
            .execute(dispute(client_id, deposit_1.transaction_id))
            .is_err());
    }

    #[test]
    fn dispute_should_not_result_in_negative_balance() {
        let mut account = Account::new(0);
        let deposit_1 = deposit(account.client_id, 5.0);
        assert!(account.execute(deposit_1.clone()).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        assert!(account.execute(withdrawal(account.client_id, 3.0)).is_ok());
        check(&account, 2.0, 0.0, 2.0, false);
        assert!(account
            .execute(dispute(account.client_id, deposit_1.transaction_id))
            .is_err());
    }

    #[test]
    fn dispute_resolve_flow() {
        let mut account = Account::new(0);
        let deposit_1 = deposit(account.client_id, 5.0);
        assert!(account.execute(deposit_1.clone()).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        assert!(account
            .execute(dispute(account.client_id, deposit_1.transaction_id))
            .is_ok());
        check(&account, 0.0, 5.0, 5.0, false);
        assert!(
            account
                .transactions
                .get(&deposit_1.transaction_id)
                .unwrap()
                .under_dispute
        );
        assert!(account
            .execute(resolve(account.client_id, deposit_1.transaction_id))
            .is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        assert!(
            !account
                .transactions
                .get(&deposit_1.transaction_id)
                .unwrap()
                .under_dispute
        );
    }

    #[test]
    fn dispute_of_withdrawal_should_fail() {
        let mut account = Account::new(0);
        assert!(account.execute(deposit(account.client_id, 5.0)).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        let withdrawal_1 = withdrawal(account.client_id, 3.0);
        assert!(account.execute(withdrawal_1.clone()).is_ok());
        check(&account, 2.0, 0.0, 2.0, false);
        assert!(account
            .execute(dispute(account.client_id, withdrawal_1.transaction_id))
            .is_err());
    }

    #[test]
    fn resolve_of_undisputed_transaction_should_fail() {
        let mut account = Account::new(0);
        let deposit_1 = deposit(account.client_id, 5.0);
        assert!(account.execute(deposit_1.clone()).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        assert!(account
            .execute(resolve(account.client_id, deposit_1.transaction_id))
            .is_err());
    }

    #[test]
    fn chargeback_of_undisputed_transaction_should_fail() {
        let mut account = Account::new(0);
        let deposit_1 = deposit(account.client_id, 5.0);
        assert!(account.execute(deposit_1.clone()).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        assert!(account
            .execute(chargeback(account.client_id, deposit_1.transaction_id))
            .is_err());
    }

    #[test]
    fn chargeback_flow() {
        let mut account = Account::new(0);
        let deposit_1 = deposit(account.client_id, 2.0);
        assert!(account.execute(deposit_1.clone()).is_ok());
        let deposit_2 = deposit(account.client_id, 3.0);
        assert!(account.execute(deposit_2.clone()).is_ok());
        check(&account, 5.0, 0.0, 5.0, false);
        // Since the transaction is not under dispute, chargeback should fail
        assert!(account
            .execute(chargeback(account.client_id, deposit_1.transaction_id))
            .is_err());
        check(&account, 5.0, 0.0, 5.0, false);
        // Dispute
        assert!(account
            .execute(dispute(account.client_id, deposit_1.transaction_id))
            .is_ok());
        // The transaction is now under dispute, the account is not locked
        check(&account, 3.0, 2.0, 5.0, false);
        assert!(account
            .execute(chargeback(account.client_id, deposit_1.transaction_id))
            .is_ok());
        // The account is now locked
        check(&account, 3.0, 0.0, 3.0, true);
        // All subsequent transactions should fail
        assert!(account.execute(deposit(account.client_id, 7.0)).is_err());
        assert!(account.execute(withdrawal(account.client_id, 3.0)).is_err());
        assert!(account
            .execute(dispute(account.client_id, deposit_2.transaction_id))
            .is_err());
        assert!(account
            .execute(resolve(account.client_id, deposit_1.transaction_id))
            .is_err());
        assert!(account
            .execute(chargeback(account.client_id, deposit_1.transaction_id))
            .is_err());
    }
}
