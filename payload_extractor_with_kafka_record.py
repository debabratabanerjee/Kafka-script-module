from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
import json

def get_latest_messages(bootstrap_servers, output_file):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()
    results = []

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=False
    )

    for topic_name in topics:
        if topic_name.startswith('_'):
            continue

        # Get partitions for topic
        partitions = consumer.partitions_for_topic(topic_name)
        if not partitions:
            continue

        message_found = False
        for partition in partitions:
            if message_found:
                break

            tp = TopicPartition(topic_name, partition)
            consumer.assign([tp])
            
            end_offset = consumer.end_offsets([tp])[tp]
            if end_offset == 0:
                continue
            
            consumer.seek(tp, end_offset - 1)
            records = consumer.poll(timeout_ms=5000)
            
            if records:
                for partition_records in records.values():
                    for record in partition_records:
                        try:
                            key = record.key.decode('utf-8') if record.key else '{}'
                            value = record.value.decode('utf-8') if record.value else '{}'
                            
                            key_json = json.loads(key)
                            value_json = json.loads(value)
                            
                            result = f"{topic_name}|{json.dumps(key_json, separators=(',',':'))}|{json.dumps(value_json, separators=(',',':'))}"
                            results.append(result)
                            message_found = True
                            break
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON for topic {topic_name}, partition {partition}: {str(e)}")
                        except Exception as e:
                            print(f"Error processing topic {topic_name}, partition {partition}: {str(e)}")
                    if message_found:
                        break

    consumer.close()
    admin_client.close()

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(results))

bootstrap_servers = ['localhost:9092']
output_file = 'kafka_messages.txt'

get_latest_messages(bootstrap_servers, output_file)