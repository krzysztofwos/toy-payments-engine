mod account;
mod transaction;
mod types;

use std::{collections::BTreeMap, io};

use anyhow::Result;
use clap::Parser;

use account::Account;
use transaction::{Transaction, TransactionType};
use types::ClientId;

fn process_csv<Input, Output>(
    reader: &mut csv::Reader<Input>,
    writer: &mut csv::Writer<Output>,
) -> Result<()>
where
    Input: io::Read,
    Output: io::Write,
{
    let mut accounts: BTreeMap<ClientId, Account> = BTreeMap::new();

    for result in reader.deserialize() {
        if let Err(e) = result {
            eprintln!("Warning: {}", e);
            continue;
        }

        let transaction: Transaction = result?;

        if transaction.requires_amount() && transaction.amount.is_none() {
            eprintln!(
                "Warning: transaction {} requires an amount but none was provided",
                transaction.transaction_id
            );
            continue;
        }

        let account = accounts
            .entry(transaction.client_id)
            .or_insert_with(|| Account::new(transaction.client_id));
        let transaction_id = transaction.transaction_id;
        let transaction_type = transaction.transaction_type;

        if let Err(e) = account.execute(transaction) {
            if matches!(
                transaction_type,
                TransactionType::Deposit | TransactionType::Withdrawal
            ) {
                eprintln!("Warning: transaction {} failed: {}", transaction_id, e)
            } else {
                eprintln!("Warning: transaction failed: {}", e)
            }
        }
    }

    for (_account_id, account) in accounts.into_iter() {
        writer.serialize(account)?;
    }

    writer.flush()?;
    Ok(())
}

fn process_csv_file(filename: &str) -> Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path(filename)?;
    let mut writer = csv::Writer::from_writer(io::stdout());
    process_csv(&mut reader, &mut writer)
}

#[derive(Debug, Parser)]
struct Args {
    filename: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    process_csv_file(&args.filename)
}

#[cfg(test)]
mod tests {
    fn run_process_csv(input: &str) -> anyhow::Result<String> {
        let mut reader = csv::ReaderBuilder::new()
            .flexible(true)
            .has_headers(true)
            .trim(csv::Trim::All)
            .from_reader(input.as_bytes());
        let mut writer = csv::WriterBuilder::new().from_writer(vec![]);
        super::process_csv(&mut reader, &mut writer)?;
        let output = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        Ok(output)
    }

    #[test]
    fn process_csv() {
        let input = "\
type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,4,1.5
withdrawal,2,5,3.0
deposit,3,6,5.0
deposit,3,7,1.017
dispute,3,7
resolve,3,7
chargeback,3,7
dispute,3,7
deposit,4,8,3.0
deposit,4,9,4.0
dispute,4,8
charge
chargeback,
chargeback,4
chargeback,4,
chargeback,4,8
";
        let expected_output = "\
client,available,held,total,locked
1,1.5000,0.0000,1.5000,false
2,2.0000,0.0000,2.0000,false
3,5.0000,1.0170,6.0170,false
4,4.0000,0.0000,4.0000,true
";
        let output = run_process_csv(input).unwrap();
        assert_eq!(output, expected_output);
    }
}
