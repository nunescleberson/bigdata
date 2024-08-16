from kafka import KafkaProducer
import json
import time
import random
from faker import Faker

# Configuração do produtor Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

fake = Faker('pt_BR')

# Enviar dados simulados para o tópico Kafka
i = 1
while True:
    person_cod = i
    country_cod = random.randint(1, 121)
    name = fake.name()
    date_birth = fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d')
    salary = round(random.uniform(3000, 6000), 2)
    
    data = {
        "person_cod": person_cod,
        "country_cod": country_cod,
        "name": name,
        "date_birth": date_birth,
        "salary": salary
    }
    
    producer.send('real_time_data', value=data)
    print(f"Mensagem enviada: {data}")
    time.sleep(5)
    
    i += 1
