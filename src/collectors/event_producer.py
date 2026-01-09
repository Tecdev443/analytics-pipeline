import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = 'localhost:9092'
EVENTS_TOPIC = 'clickstream_events'

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

PAGES = ['/', '/products', '/product-detail', '/cart', '/checkout', '/search', '/profile']
EVENTS = ['pageview', 'click', 'scroll', 'search', 'add_to_cart', 'purchase', 'signup', 'login']
SOURCES = ['organic', 'paid', 'direct', 'referral', 'social', 'email']

def generate_event():
    user_id = f'user_{random.randint(1, 10000)}'
    session_id = f'session_{random.randint(1, 5000)}'
    
    return {
        'event_id': f'evt_{int(time.time() * 1000)}_{random.randint(0, 9999)}',
        'user_id': user_id,
        'session_id': session_id,
        'event_type': random.choice(EVENTS),
        'page': random.choice(PAGES),
        'source': random.choice(SOURCES),
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'revenue': round(random.uniform(0, 500), 2) if random.random() < 0.1 else 0,
        'country': random.choice(['US', 'UK', 'CA', 'AU', 'DE'])
    }

if __name__ == '__main__':
    logger.info('Starting clickstream event producer')
    count = 0
    try:
        while True:
            event = generate_event()
            producer.produce(
                EVENTS_TOPIC,
                key=event['user_id'].encode(),
                value=json.dumps(event).encode()
            )
            producer.poll(0)
            count += 1
            if count % 100 == 0:
                logger.info(f'Produced {count} events')
            time.sleep(0.01)
    except KeyboardInterrupt:
        logger.info('Shutting down')
    finally:
        producer.flush()
