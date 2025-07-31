import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import random

# Set seed for reproducibility
fake = Faker()
Faker.seed(0)
np.random.seed(42)
# Define size of table
num_accounts = 500  # Adjust as needed

#BUIDLING THE PARTY TABLE
# Generate 500 party_ids
party_ids = np.random.choice(range(100000, 999999), size=num_accounts, replace=False)

# Generate random names
names = [f"Name_{i}" for i in party_ids]

# Generate unique emails
emails = [f"user{i}@example.com" for i in party_ids]

# Randomly assign party_type
party_types = np.random.choice(['customer', 'merchant'], size=num_accounts, replace=True)

# Random created_at datetime within the last 2 years
def random_datetime(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

start_date = datetime.now() - timedelta(days=730)
end_date = datetime.now()
created_at = [random_datetime(start_date, end_date) for _ in range(500)]

# Create DataFrame
party_df = pd.DataFrame({
    'party_id': party_ids,
    'name': names,
    'email': emails,
    'party_type': party_types,
    'created_at': created_at
})

# Preview
print(party_df.head())

#save to CSV
party_df.to_csv("party_table.csv", index=False)

import os
print("Saved to:", os.path.abspath("party_data.csv"))

# BUILDING THE CUSTOMER TABLE
#Filter for customers
customer_df = party_df[party_df["party_type"] == "customer"].copy()

#Add customer-specific fields
customer_df["dob"] = [fake.date_of_birth(minimum_age=18, maximum_age=70) for _ in range(len(customer_df))]
customer_df["phone_number"] = [fake.phone_number() for _ in range(len(customer_df))]
customer_df["registration_date"] = [fake.date_between(start_date='-2y', end_date='today') for _ in range(len(customer_df))]

#Keep only required columns
customer_df = customer_df[["party_id", "dob", "phone_number", "registration_date"]]
#Save to CSV
customer_df.to_csv("customer_data.csv", index=False)

print("Party and Customer tables saved as 'party_table.csv' and 'customer_data.csv'.")

#BUILDING THE MERCHANT TABLE
#Filter for merchants
merchant_df = party_df[party_df["party_type"] == "merchant"].copy()

#Define some example merchant category codes
category_codes = [
    'electronics', 'clothing', 'food', 'books', 'beauty',
    'furniture', 'sports', 'automotive', 'health', 'travel'
]

#Assign random category codes
merchant_df["category_code"] = np.random.choice(category_codes, size=len(merchant_df))
merchant_df["category_code"] = merchant_df["category_code"].str[:10]  #enforce VARCHAR(10)


#Keep only required columns
merchant_df = merchant_df[["party_id", "category_code"]]

#Save to CSV
merchant_df.to_csv("merchant_data.csv", index=False)

print("Merchant table saved as 'merchant_data.csv'.")


# Generate unique account IDs (6-digit numbers)
account_ids = np.random.choice(range(100000, 999999), size=num_accounts, replace=False)

# Generate random opened dates (within last 3 years)
start_date = datetime.now() - timedelta(days=3*365)
opened_dates = [fake.date_between(start_date=start_date, end_date='today') for _ in range(num_accounts)]

# Define possible statuses
statuses = ['active', 'suspended', 'closed', 'pending']
status_values = np.random.choice(statuses, size=num_accounts)

# Build DataFrame
account_df = pd.DataFrame({
    'account_id': account_ids,
    'opened_date': opened_dates,
    'status': status_values
})

#Save to CSV
account_df.to_csv("account_data.csv", index=False)

# BUILDING THE CUSTOMERACCOUNT TABLE
#Filter only customer party_ids
customer_ids = customer_df["party_id"].values

#You can choose how many relationships to create.
#For simplicity, assume each customer has 1-3 accounts (with reuse allowed)
relationship_count = 600  # total number of customer-account links

#Sample customer and account IDs
customer_account_links = pd.DataFrame({
    "party_id": np.random.choice(customer_ids, size=relationship_count, replace=True),
    "account_id": np.random.choice(account_ids, size=relationship_count, replace=True),
    "role": np.random.choice(["primary", "joint"], size=relationship_count)
})

#Drop duplicates if a customer is accidentally assigned the same account more than once with the same role
customer_account_links = customer_account_links.drop_duplicates(subset=["party_id", "account_id"])

# Save to CSV
customer_account_links.to_csv("customer_account_data.csv", index=False)
print("CustomerAccount table saved as 'customer_account_data.csv'.")

# BUILDING THE CARD TABLE
#Load account_ids from CustomerAccount table
customer_account_df = pd.read_csv("customer_account_data.csv")
account_ids_from_links = customer_account_df["account_id"].unique()

#Define number of cards
num_cards = 700  # adjust as needed

#Generate unique card IDs
def generate_unique_card_ids(n):
    card_ids = set()
    while len(card_ids) < n:
        card_id = random.randint(10**15, 10**16 - 1)  # ensures it's 16 digits
        card_ids.add(card_id)
    return list(card_ids)
card_ids = generate_unique_card_ids(num_cards)

#Randomly select account_ids for cards
card_account_ids = np.random.choice(account_ids_from_links, size=num_cards, replace=True)

#Generate issue dates within the last 3 years
issue_dates = [fake.date_between(start_date='-3y', end_date='today') for _ in range(num_cards)]

#Generate expiration dates 3–5 years after issue
expiration_dates = [
    issue_date + timedelta(days=random.randint(365*3, 365*5))
    for issue_date in issue_dates
]

#Define card statuses
card_statuses = ['active', 'blocked', 'expired']
statuses = np.random.choice(card_statuses, size=num_cards)

#Build DataFrame
card_df = pd.DataFrame({
    'card_id': card_ids,
    'account_id': card_account_ids,
    'issue_date': issue_dates,
    'expiration_date': expiration_dates,
    'status': statuses
})

#Save to CSV
card_df.to_csv("card_data.csv", index=False)
print("Card table saved as 'card_data.csv'.")

#BUILDING THE TRANSACTION TABLE
#Number of transactions
num_transactions = 2000

#Get list of party_ids
party_ids = party_df["party_id"].tolist()

#Generate transaction IDs
transaction_ids = list(range(1, num_transactions + 1))

#Generate payer and payee IDs (ensuring they're different)
payer_ids = []
payee_ids = []

while len(payer_ids) < num_transactions:
    payer = np.random.choice(party_ids)
    payee = np.random.choice(party_ids)
    if payer != payee:
        payer_ids.append(payer)
        payee_ids.append(payee)

#Generate random amounts between 1 and 10,000 (2 decimal places)
amounts = np.round(np.random.uniform(1.00, 10000.00, size=num_transactions), 2)

#Set currency codes
currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
currency_values = np.random.choice(currencies, size=num_transactions)

#Generate timestamps in the past 2 years
start_date = datetime.now() - timedelta(days=730)
timestamps = [fake.date_time_between(start_date=start_date, end_date='now') for _ in range(num_transactions)]

#Generate is_fraud (e.g., 5% fraud rate)
is_fraud_flags = np.random.choice([False, True], size=num_transactions, p=[0.95, 0.05])

#Fraud score: higher if fraud, lower otherwise
fraud_scores = [
    round(np.random.uniform(70, 100), 2) if is_fraud else round(np.random.uniform(0, 50), 2)
    for is_fraud in is_fraud_flags
]

#Build DataFrame
transaction_df = pd.DataFrame({
    'transaction_id': transaction_ids,
    'payer_party_id': payer_ids,
    'payee_party_id': payee_ids,
    'amount': amounts,
    'currency': currency_values,
    'timestamp': timestamps,
    'is_fraud': is_fraud_flags,
    'fraud_score': fraud_scores
})

#Save to CSV
transaction_df.to_csv("transaction_data.csv", index=False)

print("Transaction table saved as 'transaction_data.csv'.")

#BUILDING THE DEVICE TABLE
num_devices = 1000
device_ids = list(range(1, num_devices + 1))
device_types = ['mobile', 'desktop', 'tablet']
oses = ['Windows', 'macOS', 'Linux', 'iOS', 'Android']
ip_addresses = [fake.ipv4() for _ in range(num_devices)]
fingerprints = [fake.sha1() for _ in range(num_devices)]

device_df = pd.DataFrame({
    'device_id': device_ids,
    'device_type': np.random.choice(device_types, num_devices),
    'os': np.random.choice(oses, num_devices),
    'ip_address': ip_addresses,
    'browser_fingerprint': fingerprints
})
device_df.to_csv("device_data.csv", index=False)
print("Device table saved as 'device_data.csv'.")

#BUILDING THE LOCATION TABLE
num_locations = 500
location_ids = list(range(1, num_locations + 1))
latitudes = np.round(np.random.uniform(-90, 90, num_locations), 6)
longitudes = np.round(np.random.uniform(-180, 180, num_locations), 6)
cities = [fake.city() for _ in range(num_locations)]
countries = [fake.country() for _ in range(num_locations)]

location_df = pd.DataFrame({
    'location_id': location_ids,
    'latitude': latitudes,
    'longitude': longitudes,
    'city': cities,
    'country': countries
})
location_df.to_csv("location_data.csv", index=False)
print("Location table saved as 'location_data.csv'.")

#BUILDING THE LOGININSTANCE TABLE
num_logins = 1500
login_ids = list(range(1, num_logins + 1))
party_sample = party_df['party_id'].sample(num_logins, replace=True).tolist()
device_sample = np.random.choice(device_ids, num_logins).tolist()
location_sample = np.random.choice(location_ids, num_logins).tolist()
login_times = [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(num_logins)]
success_flags = np.random.choice([True, False], size=num_logins, p=[0.95, 0.05])

login_df = pd.DataFrame({
    'login_id': login_ids,
    'party_id': party_sample,
    'device_id': device_sample,
    'location_id': location_sample,
    'timestamp': login_times,
    'successful': success_flags
})
login_df.to_csv("login_instance_data.csv", index=False)
print("LoginInstance table saved as 'login_instance_data.csv'.")

#BUILDING THE ANALYST TABLE
num_analysts = 50
analyst_ids = list(range(1, num_analysts + 1))
analyst_names = [fake.name() for _ in range(num_analysts)]
analyst_emails = [fake.email() for _ in range(num_analysts)]
teams = ['Red', 'Blue', 'Green', 'Yellow']
analyst_df = pd.DataFrame({
    'analyst_id': analyst_ids,
    'name': analyst_names,
    'email': analyst_emails,
    'team': np.random.choice(teams, num_analysts)
})
analyst_df.to_csv("analyst_data.csv", index=False)
print("Analyst table saved as 'analyst_data.csv'.")

#BUILDING THE FRAUDALERT TABLE
num_alerts = 300
alert_ids = list(range(1, num_alerts + 1))
sample_transactions = transaction_df[transaction_df['is_fraud'] == True].sample(num_alerts, replace=True)
generated_by = np.random.choice(['analyst', 'system', 'model'], num_alerts)
alert_levels = np.random.choice(['low', 'medium', 'high'], num_alerts)
status_options = ['open', 'dismissed', 'investigated']
created_ats = [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_alerts)]

fraud_alert_df = pd.DataFrame({
    'alert_id': alert_ids,
    'transaction_id': sample_transactions['transaction_id'].tolist(),
    'generated_by': generated_by,
    'alert_level': alert_levels,
    'status': np.random.choice(status_options, num_alerts),
    'created_at': created_ats
})
fraud_alert_df.to_csv("fraud_alert_data.csv", index=False)
print("FraudAlert table saved as 'fraud_alert_data.csv'.")

#BUILDING THE INVESTIGATION TABLE
num_investigations = 200
investigation_ids = list(range(1, num_investigations + 1))
sample_alerts = fraud_alert_df.sample(num_investigations, replace=False)
analyst_ids_sample = np.random.choice(analyst_ids, size=num_investigations)
start_dates = [fake.date_between(start_date='-1y', end_date='-30d') for _ in range(num_investigations)]
end_dates = [start + timedelta(days=random.randint(1, 60)) for start in start_dates]
conclusions = np.random.choice(['fraudulent', 'legitimate', 'inconclusive'], size=num_investigations)

investigation_df = pd.DataFrame({
    'investigation_id': investigation_ids,
    'alert_id': sample_alerts['alert_id'].tolist(),
    'analyst_id': analyst_ids_sample,
    'start_date': start_dates,
    'end_date': end_dates,
    'conclusion': conclusions
})
investigation_df.to_csv("investigation_data.csv", index=False)
print("Investigation table saved as 'investigation_data.csv'.")
