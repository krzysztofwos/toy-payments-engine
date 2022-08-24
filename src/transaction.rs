use serde::Deserialize;

use crate::types::{Amount, ClientId, TransactionId};

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub transaction_type: TransactionType,
    #[serde(rename = "client")]
    pub client_id: ClientId,
    #[serde(rename = "tx")]
    pub transaction_id: TransactionId,
    pub amount: Option<Amount>,
    #[serde(skip_deserializing)]
    pub under_dispute: bool,
}

impl Transaction {
    pub fn requires_amount(&self) -> bool {
        matches!(
            self.transaction_type,
            TransactionType::Deposit | TransactionType::Withdrawal
        )
    }
}
