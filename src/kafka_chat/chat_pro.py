from kafka import KafkaProducer
import time
from tqdm import tqdm
import json

def chatpro():
    bootstrap_servers = input("Kafka bootstrap 서버 주소 입력 (예: 34.64.106.237:9092): ").strip()
    topic = input("Kafka 토픽 이름 입력 (예: quickstart-events): ").strip()
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
    except Exception as e:
        return

    print("메시지를 입력하세요 (종료하려면 'exit' 입력)")

    while True:
        msg_input = input("You: ").strip()
        if msg_input.lower() == 'exit':
            print("종료합니다.")
            producer.flush()
            break

        msg = f"Friend: {msg_input}"
        try:
            producer.send(topic, msg)
            producer.flush()
        except Exception as e:
            print(f" 전송 오류: {e}")

    print("모든 메시지 전송 완료. 프로그램 종료.")

if __name__ == "__main__":
    chatpro()

