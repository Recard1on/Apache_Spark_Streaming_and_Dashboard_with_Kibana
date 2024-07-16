
import random
import json
import time
from datetime import datetime
from kafka import KafkaProducer

btype = ['TL', 'USD', 'EURO']
banks = [f'{chr(i)}_Bank' for i in range(65, 65 + 15)]
cities = ["Adana", "Adıyaman", "Afyonkarahisar", "Ağrı", "Aksaray", "Amasya", "Ankara", "Antalya", "Ardahan", "Artvin",
           "Aydın", "Balıkesir", "Bartın", "Batman", "Bayburt", "Bilecik", "Bingöl", "Bitlis", "Bolu", "Burdur",
           "Bursa", "Çanakkale", "Çankırı", "Çorum", "Denizli", "Diyarbakır", "Düzce", "Edirne", "Elazığ", "Erzincan",
           "Erzurum", "Eskişehir", "Gaziantep", "Giresun", "Gümüşhane", "Hakkari", "Hatay", "Iğdır", "Isparta", "İstanbul",
           "İzmir", "Kahramanmaraş", "Karabük", "Karaman", "Kars", "Kastamonu", "Kayseri", "Kırıkkale", "Kırklareli", "Kırşehir",
           "Kilis", "Kocaeli", "Konya", "Kütahya", "Malatya", "Manisa", "Mardin", "Mersin", "Muğla", "Muş", "Nevşehir",
           "Niğde", "Ordu", "Osmaniye", "Rize", "Sakarya", "Samsun", "Şanlıurfa", "Siirt", "Sinop", "Şırnak",
           "Sivas", "Tekirdağ", "Tokat", "Trabzon", "Tunceli", "Uşak", "Van", "Yalova", "Yozgat", "Zonguldak"]

def read_file():
    with open("isimler.txt", encoding='utf-8') as f:
        name_list = [line.strip() for line in f.readlines()]

    with open("soyisimler.txt", encoding='utf-8') as f:
        surname_list = [line.strip() for line in f.readlines()]

    return name_list, surname_list

def generate_oid():
    return random.randint(10000000000, 99999999999)

def generate_iban():
    return 'TR' + ''.join([str(random.randint(0, 9)) for _ in range(24)])

def generate_name(name_list, surname_list):
    name = random.choice(name_list)
    surname = random.choice(surname_list)
    return f"{name} {surname}"

def generate_city():
    return random.choice(cities)

def create_dict(name_list, surname_list):
    sender_info = {
        'bank': random.choice(banks),
        'iban': generate_iban(),
        'name': generate_name(name_list, surname_list),
        'city': generate_city()
    }

    receiver_info = {
        'bank': random.choice(banks),
        'iban': generate_iban(),
        'name': generate_name(name_list, surname_list),
        'city': generate_city()
    }

    temp_dict = {
        'pid': generate_oid(),
        'ptype': 'H',
        'sender': sender_info,
        'receiver': receiver_info,
        'balance': random.randint(0, 10000),
        'btype': random.choice(btype),
        'timestamp': str(datetime.now())
    }

    return temp_dict

def create_kafka_producer(bootstrap_servers=None):
    if bootstrap_servers is None:
        bootstrap_servers = ['164.92.196.12:9092']
    return KafkaProducer(bootstrap_servers=bootstrap_servers)

def start_streaming(duration=120):
    producer = create_kafka_producer()
    name_list, surname_list = read_file()
    end_time = time.time() + duration

    while True:
        if time.time() > end_time:
            break

        kafka_data = create_dict(name_list, surname_list)
        producer.send("Finans", json.dumps(kafka_data).encode('utf-8'))
        time.sleep(2)

if __name__ == "__main__":
    start_streaming()