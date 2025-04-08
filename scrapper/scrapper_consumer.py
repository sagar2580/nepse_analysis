import json
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2 import sql
import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
class NepseKafkaConsumer:
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': 'localhost:29092',  # Change to your Kafka broker
            'group.id': 'nepse-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        self.pg_config = {
            'host': 'localhost',  # Your container name
            'database': 'nepse',
            'user': 'admin',
            'password': 'admin',
            'port': '5432'
        }
        
        self.topic = 'test-topic'
        self.consumer = Consumer(self.kafka_config)
        self.conn = None

    def create_db_connection(self):
        """Establish connection to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.pg_config)
            print("Connected to PostgreSQL database")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise

    def process_message(self, msg):
        """Process a single Kafka message"""
        try:
            data = json.loads(msg.value())
            
            with self.conn.cursor() as cur:
                # Insert into nepse1 table
                insert_query = sql.SQL("""
                    INSERT INTO nepse1 (
                        transaction_no, symbol, buyer, seller, 
                        quantity, rate, amount, scraped_at, 
                        trade_date, page_number, kafka_offset
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (transaction_no) DO NOTHING
                """)
                
                cur.execute(insert_query, (
                    data.get('Transact. No.'),
                    data.get('Symbol'),
                    data.get('Buyer'),
                    data.get('Seller'),
                    data.get('Quantity'),
                    data.get('Rate'),
                    data.get('Amount'),
                    data.get('scraped_at'),
                    data.get('trade_date'),
                    data.get('page_number'),
                    data.get('kafka_offset')
                ))
                
                # Update kafka_offsets table
                offset_query = sql.SQL("""
                    INSERT INTO kafka_offsets (topic, partition, kafka_offset)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (topic, partition) 
                    DO UPDATE SET kafka_offset = EXCLUDED.kafka_offset
                """)
                
                cur.execute(offset_query, (
                    msg.topic(),
                    msg.partition(),
                    msg.offset()
                ))
                
                self.conn.commit()
                print(data)
                print(f"Processed message: {data.get('transaction_no')}")
                
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")
            self.conn.rollback()


    def get_stored_offsets(self):
        """Retrieve previously committed offsets"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT partition, kafka_offset 
                FROM kafka_offsets 
                WHERE topic = %s
            """, (self.topic,))
            return {p: o for p, o in cur.fetchall()}
    
    def consume_messages(self):
        from confluent_kafka import TopicPartition
        """Main consumption loop"""
        try:
            self.create_db_connection()
            
            self.consumer.subscribe([self.topic])
            
            logger.info(f"Started consuming {self.topic}")
            
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.error(f"Topic {msg.topic()} doesn't exist")
                        break
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                self.process_message(msg)
                
        except KeyboardInterrupt:
            logger.info("Gracefully shutting down...")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            logger.info("Closing connections...")
            self.consumer.close()
            if self.conn and not self.conn.closed:
                self.conn.close()

if __name__ == '__main__':
    consumer = NepseKafkaConsumer()
    consumer.consume_messages()

