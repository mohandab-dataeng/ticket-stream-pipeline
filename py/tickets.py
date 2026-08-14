"""
Producteur de tickets clients pour Redpanda.

Génère des tickets aléatoires (ID ticket, ID client, date/heure, demande,
type de demande, priorité) et les envoie en continu dans le topic
`client_tickets`, pour simuler un flux temps réel.
"""

import json
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

# --- Configuration ---
BOOTSTRAP_SERVERS = "localhost:19092"  # port Redpanda exposé par docker-compose
TOPIC_NAME = "client_tickets"
SEND_INTERVAL_SECONDS = 1  # délai entre deux tickets, simule un flux temps réel

fake = Faker("fr_FR")

# Types de demande et priorités possibles
TICKET_TYPES = ["Support technique", "Facturation", "Livraison", "Réclamation", "Information"]
PRIORITIES = ["basse", "moyenne", "haute", "urgente"]


def build_producer() -> KafkaProducer:
    """Crée et retourne un KafkaProducer configuré pour Redpanda."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

def generate_ticket() -> dict:
    """Génère un ticket client aléatoire respectant le format minimum demandé."""
    return {
        "ticket_id": str(uuid.uuid4()),
        "client_id": str(fake.random_int(min=1000, max=9999)),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": fake.sentence(nb_words=12),
        "request_type": fake.random_element(elements=TICKET_TYPES),
        "priority": fake.random_element(elements=PRIORITIES),
    }

def main() -> None:
    producer = build_producer()
    print(f"Connecté à Redpanda ({BOOTSTRAP_SERVERS}). Envoi vers le topic '{TOPIC_NAME}'...")

    try:
        while True:
            ticket = generate_ticket()
            producer.send(TOPIC_NAME, key=ticket["client_id"], value=ticket)
            print(f"Envoyé : {ticket}")
            time.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nArrêt du producteur (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()