import time
import json
import random
import os
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
TOPIC_NAME = 'input-stream'

fake = Faker('fr_FR')

def delivery_report(err, msg):
    """Fonction de callback pour confirmer que le message est bien arrivé."""
    if err is not None:
        print(f'Échec de l\'envoi du message: {err}')
    else:
        print(f'Message livré à {msg.topic()} [{msg.partition()}]')

def generate_event():
    """Génère un dictionnaire représentant une transaction ou un événement."""
    return {
        "event_id": fake.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": fake.random_int(min=1000, max=9999),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": "EUR",
        "status": random.choice(["INITIATED", "PENDING", "COMPLETED", "FAILED"]),
        "origin_ip": fake.ipv4()
    }

def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'python-producer-1'
    }

    print(f"Connexion à Kafka sur {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = Producer(conf)

    print(f" Démarrage de l'envoi des événements vers le topic '{TOPIC_NAME}'")
    
    try:
        while True:
            event = generate_event()
            
            value_json = json.dumps(event).encode('utf-8')
            
            # On utilise user_id comme clé pour garantir l'ordre 
            key = str(event['user_id']).encode('utf-8')
            
            producer.produce(
                TOPIC_NAME, 
                key=key, 
                value=value_json, 
                callback=delivery_report
            )
            
            producer.poll(0)
            
            time.sleep(0.01) 

    except KeyboardInterrupt:
        print("Arrêt par l'utilisateur.")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()