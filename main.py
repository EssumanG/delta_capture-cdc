import psycopg2
# from psycopg2 import _T_conn
import faker
from datetime import datetime
import random
from dataclasses import dataclass


@dataclass
class Transaction:
    transactionId: str
    userId: str
    timestamp: datetime
    amount: float
    currency: str
    city: str
    country: str
    merchantName: str
    paymentMethod: str
    ipAddress: str
    voucherCode: str
    affiliateId: str



fake = faker.Faker()

def generate_transaction() -> Transaction:
    """
    Generate a dummy/fake transaction record using faker 
    """
    user = fake.simple_profile()
    transaction_record = Transaction(
        transactionId=fake.uuid4(),
        userId= user['username'],
        amount=round(random.uniform(10, 1000), 2),
        timestamp= datetime.now(),
        currency=random.choice(['USD', 'GHS', 'GBP']),
        city= fake.city(),
        country= fake.country(),
        paymentMethod= random.choice(['credit_card', 'debit_card', 'online_transfer']),
        ipAddress= fake.ipv4(),
        voucherCode= random.choice(['', 'DISC_10', '']),
        merchantName= fake.company(),
        affiliateId=fake.uuid4()
        )
    return transaction_record


def create_table(conn: psycopg2.extensions.connection) -> None:
    """
    Creates a database table `transactions`
    """

    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),
            voucher_code VARCHAR(255),
            affiliated_id VARCHAR(255)
        );
        """)
    
    conn.commit()
    cursor.close()

def insert_into_table(transaction_record: Transaction, conn: psycopg2.extensions.connection) -> None:
    """
    Insert the transaction record into the table transactions
    """
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city,
        country, merchant_name, payment_method, ip_address, voucher_code, affiliated_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s)
        """,  (transaction_record.transactionId, transaction_record.userId, transaction_record.timestamp,
               transaction_record.amount, transaction_record.currency, transaction_record.city, 
               transaction_record.country, transaction_record.merchantName, transaction_record.paymentMethod,
               transaction_record.ipAddress, transaction_record.voucherCode, transaction_record.affiliateId))
    

    conn.commit()
    cursor.close()


def main()-> None:
    connection = psycopg2.connect(
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5434
    )

    create_table(connection)
    transaction_record = generate_transaction()
    insert_into_table(transaction_record, connection)

    connection.close()
if  __name__ == "__main__":
    main()