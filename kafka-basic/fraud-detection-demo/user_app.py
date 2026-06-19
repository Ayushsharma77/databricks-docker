import json
from kafka import KafkaConsumer

def user_login_and_listen():
    print("=== Fraud Alert System ===")
    user_id_input = input("Enter your userId to login (No password required): ")
    
    try:
        user_id = int(user_id_input)
    except ValueError:
        print("Invalid ID. Exiting.")
        return
        
    print(f"Logged in successfully as User {user_id}. Listening for real-time alerts...")
    
    # Initialize Kafka Consumer (removed value_deserializer to eliminate the warning)
    consumer = KafkaConsumer(
        'fraud-notification',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest'
    )
    
    for message in consumer:
        try:
            # Decode the bytes and parse the JSON manually inside the loop
            alert_data = json.loads(message.value.decode('utf-8'))
        except Exception:
            continue  # Skip messages that fail to parse
        
        # Filter messages for the logged-in user
        if alert_data.get('userId') == user_id:
            print("\n🚨 [CRITICAL ALERT] 🚨")
            print(f"User Name: {alert_data.get('name')}")
            print(f"Suspicious Transaction ID: {alert_data.get('tx_id')}")
            print(f"Amount: ${alert_data.get('amount'):.2f}")
            print("Please verify this transaction immediately.\n")

if __name__ == "__main__":
    user_login_and_listen()
