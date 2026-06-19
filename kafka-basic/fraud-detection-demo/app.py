import json
from kafka import KafkaConsumer

def monitor_all_fraud_alerts():
    print("=== Global Fraud Alert Monitor ===")
    print("Listening for real-time alerts across all user accounts...\n")
    
    # Initialize Kafka Consumer listening to the notification topic
    consumer = KafkaConsumer(
        'fraud-notification',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest'
    )
    
    for message in consumer:
        try:
            # Decode bytes and parse the JSON string manually
            alert_data = json.loads(message.value.decode('utf-8'))
        except Exception:
            continue  # Skip messages that fail to parse
        
        # Display alerts for ALL users indiscriminately
        print("🚨 [CRITICAL ALERT] 🚨")
        print(f"User ID:                    {alert_data.get('userId')}")
        print(f"User Name:                  {alert_data.get('name')}")
        print(f"Suspicious Transaction ID:  {alert_data.get('tx_id')}")
        print(f"Amount Flagged:             ${alert_data.get('amount'):.2f}")
        print("-" * 40)

if __name__ == "__main__":
    monitor_all_fraud_alerts()
