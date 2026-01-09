import json
import os
import signal
import sys
from confluent_kafka import Consumer, Producer


KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
INPUT_TOPIC = 'input-stream'
VALID_TOPIC = 'validated-transactions'
DLQ_TOPIC = 'dead-letter-queue'

GROUP_ID = 'fraud-detector-group-v1'

running = True

def signal_handler(sig, frame):
    
    global running
    print('\nArrêt demandé, fermeture...')
    running = False

def create_producer():
    return Producer({'bootstrap.servers': KAFKA_BROKER})

def create_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest', 
        'enable.auto.commit': False      
    }
    return Consumer(conf)

def process_transaction(transaction):
   
    # Règle 1 : le montant doit etre positif
    if transaction.get('amount') <= 0:
        return False, "NEGATIVE_AMOUNT"
    
    # Règle 2 : Les Champs obligatoires
    if not transaction.get('user_id'):
        return False, "MISSING_USER_ID"

    # Règle 3 : supporte les dévises 
    if transaction.get('currency') != 'EUR':
        return False, "INVALID_CURRENCY"

    return True, "OK"

def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    consumer = create_consumer()
    producer = create_producer()
    
    consumer.subscribe([INPUT_TOPIC])
    print(f" Écoute du topic '{INPUT_TOPIC}'...")

    try:
        while running:
            msg = consumer.poll(1.0) 

            if msg is None: continue
            if msg.error():
                print(f"Erreur Kafka: {msg.error()}")
                continue

            # Décodage
            try:
                raw_data = msg.value().decode('utf-8')
                transaction = json.loads(raw_data)
                
                is_valid, reason = process_transaction(transaction)

                if is_valid:
                    producer.produce(VALID_TOPIC, value=raw_data)
                else:
                    # On enrichit la donnée avec la raison de l'échec
                    transaction['error_reason'] = reason
                    producer.produce(DLQ_TOPIC, value=json.dumps(transaction))
                    print(f"Rejet: {reason} pour ID {transaction.get('event_id')}")

                producer.poll(0) 

                consumer.commit(asynchronous=True)

            except Exception as e:
                print(f"Erreur critique de traitement : {e}")

    finally:
        consumer.close()
        producer.flush()

if __name__ == '__main__':
    main()