import json
import paho.mqtt.client as mqtt
import tkinter as tk
from datetime import datetime

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
    1: 20,
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
    {"salaa": 2, "salab": 3},
    {"salaa": 2, "salab": 4},
    {"salaa": 2, "salab": 5},
    {"salaa": 3, "salab": 2},
    {"salaa": 4, "salab": 5},
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
    {"salaa": 8, "salab": 10},
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


def on_button_click(data):
    # json_string = json.dumps(data, indent=4)
    # print(f'Button clicked. JSON object: \n{json_string}')
    a = data['salaa']
    b = data['salab']

    if adjacency_mask[a - 1][b - 1] == 1:
        room_mouses[b] += 1
        room_mouses[a] -= 1
    
    for key in sorted(room_mouses.keys()):
        print(f"Mouses in {key}: {room_mouses[key]}")

    jsonString = f"{{Hora: \"{str(datetime.now())}\", SalaOrigem: {str(a)}, SalaDestino: {str(b)}}}"
    print(jsonString)
    clientMqttMovements.publish(topic, jsonString, qos=2)
    clientMqttMovements.loop()

def send_string_message(txt):
    message = txt.get()
    print("Sending message:", message)
    clientMqttMovements.publish(topic, message, qos=2)
    clientMqttMovements.loop()
    txt.delete(0, 'end')


root = tk.Tk()

frame1 = tk.Frame(root, padx=20, pady=20)
frame1.pack()

frame2 = tk.Frame(root, padx=20, pady=0)
frame2.pack()

frame3 = tk.Frame(root, padx=20, pady=20)
frame3.pack()

frame4 = tk.Frame(root, padx=20, pady=0)
frame4.pack()

frame5 = tk.Frame(root, padx=20, pady=20)
frame5.pack()

frame6 = tk.Frame(root, padx=20, pady=0)
frame6.pack()

columns = 4

label1 = tk.Label(frame1, text="Correct Rooms")
label1.grid(row=0, column=0, columnspan=columns)

for i, json_object in enumerate(correct_rooms):
    button_text = f"{json_object['salaa']} -> {json_object['salab']}"
    button = tk.Button(frame2, text=button_text, command=lambda obj=json_object: on_button_click(obj))
    button.grid(row=i // columns, column=i % columns, padx=5, pady=5)

label2 = tk.Label(frame3, text="Incorrect Rooms")
label2.grid(row=0, column=0, columnspan=columns)

for i, json_object in enumerate(incorrect_rooms):
    button_text = f"{json_object['salaa']} -> {json_object['salab']}"
    button = tk.Button(frame4, text=button_text, command=lambda obj=json_object: on_button_click(obj))
    button.grid(row=i // columns, column=i % columns, padx=5, pady=5)

label3 = tk.Label(frame5, text="Room to Itself")
label3.grid(row=0, column=0, columnspan=columns)

for i, json_object in enumerate(self_paired_rooms):
    button_text = f"{json_object['salaa']} -> {json_object['salab']}"
    button = tk.Button(frame6, text=button_text, command=lambda obj=json_object: on_button_click(obj))
    button.grid(row=i // columns, column=i % columns, padx=5, pady=5)


label_message = tk.Label(root, text='Message')
label_message.pack()
message_entry = tk.Entry(root)
message_entry.pack(fill=tk.X, expand=True)  # stretches to fill the window width
message_button = tk.Button(root, text="Send Message", command=lambda: send_string_message(message_entry))
message_button.pack()

root.mainloop()
mqtt_thread = threading.Thread(target=run_mqtt)
mqtt_thread.start()