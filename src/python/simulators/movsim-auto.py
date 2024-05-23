import json
import paho.mqtt.client as mqtt
import tkinter as tk
from datetime import datetime
import time
import random
import threading

allow_room_limit = True
allow_invalid_reading = False
allow_idle_limit = False

room_limit = 30
idle_limit = 15
invalid_reading_chance = 0.05
simultaneous_moves = 4

adjacency_mask = [  # rows: from, cols: to
    [0, 1, 1, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 1, 1, 0, 0, 0, 0, 0],
    [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
    [0, 0, 1, 0, 0, 1, 1, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 1, 1],
    [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
]

room_mouses = {
    1: 30,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0,
}

correct_rooms = [
    {"salaa": 1, "salab": 2},
    {"salaa": 1, "salab": 3},
    {"salaa": 2, "salab": 4},
    {"salaa": 2, "salab": 5},
    {"salaa": 3, "salab": 2},
    {"salaa": 4, "salab": 5},
    {"salaa": 5, "salab": 3},
    {"salaa": 5, "salab": 6},
    {"salaa": 5, "salab": 7},
    {"salaa": 6, "salab": 8},
    {"salaa": 7, "salab": 5},
    {"salaa": 8, "salab": 9},
    {"salaa": 8, "salab": 10},
    {"salaa": 9, "salab": 7}
]

incorrect_rooms = [
    {"salaa": 1, "salab": 6},
    {"salaa": 2, "salab": 9},
    {"salaa": 3, "salab": 4},
    {"salaa": 3, "salab": 5},
    {"salaa": 3, "salab": 8},
    {"salaa": 3, "salab": 10},
    {"salaa": 5, "salab": 8},
    {"salaa": 6, "salab": 9},
    {"salaa": 7, "salab": 10},
    {"salaa": 8, "salab": 3},
    {"salaa": 4, "salab": 7},
    {"salaa": 6, "salab": 10}
]

self_paired_rooms = [
    {"salaa": 1, "salab": 1},
    {"salaa": 2, "salab": 2},
    {"salaa": 3, "salab": 3},
    {"salaa": 4, "salab": 4},
    {"salaa": 5, "salab": 5},
    {"salaa": 6, "salab": 6},
    {"salaa": 7, "salab": 7},
    {"salaa": 8, "salab": 8},
    {"salaa": 9, "salab": 9},
    {"salaa": 10, "salab": 10}
]

host = "broker.mqtt-dashboard.com"
port = 1883
topic = "pisid_mazemov_14"


def on_connectMqttTemp(client, userdata, flags, rc):
    print("MQTT Movements Connected with result code " + str(rc))


clientMqttMovements = mqtt.Client()
clientMqttMovements.on_connect = on_connectMqttTemp
clientMqttMovements.connect(host, port)


def send_reading(data):
    a = data['salaa']
    b = data['salab']

    if adjacency_mask[a - 1][b - 1] == 1:
        room_mouses[a] -= 1
        room_mouses[b] += 1

    jsonString = f"{{Hora: \"{str(datetime.now())}\", SalaOrigem: {str(a)}, SalaDestino: {str(b)}}}"
    print("MESSAGE:", jsonString)
    clientMqttMovements.publish(topic, jsonString, qos=2)
    clientMqttMovements.loop()


def start():
    print("Started")
    print("----------")
    for key in sorted(room_mouses.keys()):
        print(f"Mouses in {key}: {room_mouses[key]}")
    print("----------")
    while True:
        moves = random.randint(1, simultaneous_moves)
        print("Simultaneous movements:", moves)
        for i in range(moves):
            print(f"Movement {i + 1}:")

            filtered_rooms = {key: val for key, val in room_mouses.items() if val > 0}

            while True:
                keys = list(filtered_rooms.keys())
                weights = list(filtered_rooms.values())
                random_room = random.choices(keys, weights, k=1)[0]
                print("Random room:", random_room)
                correct_filtered_rooms = [room for room in correct_rooms if room["salaa"] == random_room]

                if len(correct_filtered_rooms) == 0:
                    continue

                do_movement = random.choice(correct_filtered_rooms)
                print("Movement:", do_movement)

                send_reading(do_movement)
                break

            time.sleep(0.25)

        print("----------")
        for key in sorted(room_mouses.keys()):
            print(f"Mouses in {key}: {room_mouses[key]}")
        print("----------")
        time.sleep(random.uniform(0, idle_limit / 3))



start()
time.sleep(5)
