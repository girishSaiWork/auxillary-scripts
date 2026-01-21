import json
import uuid
import random
import time
import os
from datetime import datetime, timezone

# Configuration
OUTPUT_DIR = "stream_data"
GENERATION_INTERVAL = 2  # Seconds between file generation
RECORDS_PER_FILE = 5     # Number of records per JSON file

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

banking_pii_fields = [
    # Personal Identification
    "full_name",
    "date_of_birth",
    "gender",
    "nationality",
    "photograph",
    "fingerprint",
    "face_scan",

    # Contact Information
    "residential_address",
    "mailing_address",
    "email_address",
    "mobile_number",
    "landline_number",

    # Government / KYC Identifiers
    "passport_number",
    "national_id_number",
    "aadhaar_number",
    "ssn",
    "drivers_license_number",

    # Account Information
    "bank_account_number",
    "iban",
    "customer_id",
    "cif_number",

    # Card & Payment Instruments
    "debit_card_number",
    "credit_card_number",
    "card_expiry_date",
    "cvv",
    "virtual_card_number",

    # Authentication & Security
    "pin",
    "password",
    "otp",
    "security_question",
    "security_answer",
    "cryptographic_key",

    # Transaction PII
    "sender_account_number",
    "receiver_account_number",
    "upi_id",
    "wallet_id",
    "merchant_id",
    "transaction_reference_number",

    # Transaction Metadata
    "transaction_amount",
    "transaction_timestamp",
    "transaction_location",
    "ip_address",
    "device_id",
    "transaction_channel",

    # Derived / Behavioral PII
    "spending_pattern",
    "transaction_frequency",
    "merchant_preference",
    "geolocation_pattern",
    "credit_score",
    "fraud_risk_score",
]

application_ids = [
    "App-Mobile-iOS",
    "App-Mobile-Android",
    "Web-Banking-Portal",
    "ATM-Interface",
    "Branch-CRM",
    "Partner-API"
]

def generate_record():
    """Generates a single data record."""
    sign_in_id = str(uuid.uuid4())
    customer_id = f"CUSTOMERID{uuid.uuid4()}"
    application_id = random.choice(application_ids)
    event_date_timestamp = datetime.now(timezone.utc).isoformat()
    
    # Randomly pick 1 to 5 fields for the viewedField list
    num_fields = random.randint(1, 5)
    viewed_field = random.sample(banking_pii_fields, num_fields)

    return {
        "signInID": sign_in_id,
        "customerID": customer_id,
        "applicationID": application_id,
        "eventDateTimestamp": event_date_timestamp,
        "viewedField": viewed_field
    }

def main():
    print(f"Starting data generation stream to '{OUTPUT_DIR}'...")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            records = [generate_record() for _ in range(RECORDS_PER_FILE)]
            
            # Get current time
            now = datetime.now()
            
            # Create directory structure: year/month/date/hour
            year = now.strftime("%Y")
            month = now.strftime("%m")
            day = now.strftime("%d")
            hour = now.strftime("%H")
            
            current_output_dir = os.path.join(OUTPUT_DIR, year, month, day, hour)
            os.makedirs(current_output_dir, exist_ok=True)
            
            # Create a unique filename based on timestamp
            timestamp_str = now.strftime("%Y%m%d_%H%M%S_%f")
            filename = f"data_stream_{timestamp_str}.json"
            filepath = os.path.join(current_output_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(records, f, indent=4)
            
            print(f"Generated {filepath} with {len(records)} records.")
            time.sleep(GENERATION_INTERVAL)
            
    except KeyboardInterrupt:
        print("\nData generation stopped.")

if __name__ == "__main__":
    main()
