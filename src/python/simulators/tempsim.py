# py -m pip install paho-mqtt
import paho.mqtt.client as mqtt
import time
from datetime import datetime
import random
import threading
from tkinter import Tk, Label, Entry, Button


host = "broker.mqtt-dashboard.com"
port = 1883
topic = "pisid_mazetemp_14"


def on_connectMqttTemp(client, userdata, flags, rc):
    print("MQTT Temperature Connected with result code " + str(rc))


clientMqttMovements = mqtt.Client()
clientMqttMovements.on_connect = on_connectMqttTemp
clientMqttMovements.connect(host, port)

prc_outlier = 0.05
prc_rampage = 0.00

regular_temp_variation = 0.6

rampage_value = 2.5
rampage_variation = 1.0

outlier_value = 20.0
outlier_variation = 10.0

output_temp1 = random.uniform(15.0, 35.0)
output_temp2 = random.uniform(15.0, 35.0)

sensor1_rampage = False
sensor1_rampage_direction = 1
sensor2_rampage = False
sensor2_rampage_direction = 1


def run_mqtt():
    global sensor1_rampage
    global sensor1_rampage_direction
    global sensor2_rampage
    global sensor2_rampage_direction
    global output_temp1
    global output_temp2

    def rampage_scenario(temp, direction):
        return temp + ((rampage_value + random.uniform(-rampage_variation, rampage_value)) * direction)

    while True:

        if sensor1_rampage:
            output_temp1 = rampage_scenario(output_temp1, sensor1_rampage_direction)
        else:
            output_temp1 += random.uniform(-regular_temp_variation, regular_temp_variation)

        if sensor2_rampage:
            output_temp2 = rampage_scenario(output_temp2, sensor2_rampage_direction)
        else:
            output_temp2 += random.uniform(-regular_temp_variation, regular_temp_variation)

        outlier1 = None
        if not sensor1_rampage:

            rand1 = random.random()
            if rand1 < prc_outlier:
                outlier_temp1 = outlier_value + random.uniform(-outlier_variation, outlier_variation)
                if random.random() > 0.5:
                    outlier_temp1 *= -1
                outlier1 = output_temp1 + outlier_temp1
                print("    ###    Sensor 1 produced an outlier.")
            elif rand1 < prc_outlier + prc_rampage:
                sensor1_rampage = True
                sensor1_rampage_direction = 1 if random.random() > 0.5 else -1
                print(f"    ###    Sensor 1 entered rampage state. Temperature will "
                      f"{'rise' if sensor1_rampage_direction > 0 else 'fall'} quickly.")

        outlier2 = None
        if not sensor2_rampage:

            rand2 = random.random()
            if rand2 < prc_outlier:
                outlier_temp2 = outlier_value + random.uniform(-outlier_variation, outlier_variation)
                if random.random() > 0.5:
                    outlier_temp2 *= -1
                outlier2 = output_temp2 + outlier_temp2
                print("    ###    Sensor 2 produced an outlier.")
            elif rand2 < prc_outlier + prc_rampage:
                sensor2_rampage = True
                sensor2_rampage_direction = 1 if random.random() > 0.5 else -1
                print(f"    ###    Sensor 2 entered rampage state. Temperature will "
                      f"{'rise' if sensor2_rampage_direction > 0 else 'fall'} quickly.")

        temp1 = output_temp1 if outlier1 is None else outlier1
        temp2 = output_temp2 if outlier2 is None else outlier2
        try:
            mensagem = f"{{Hora: '{str(datetime.now())}', Leitura: {temp1:.2f}, Sensor: 1}}"
            print("Generated message Sensor 1:", mensagem)
            clientMqttMovements.publish(topic, mensagem, qos=2)
            clientMqttMovements.loop()
            mensagem = f"{{Hora: '{str(datetime.now())}', Leitura: {temp2:.2f}, Sensor: 2}}"
            print("Generated message Sensor 2:", mensagem)
            clientMqttMovements.publish(topic, mensagem, qos=2)
            clientMqttMovements.loop()
            time.sleep(1)
        except Exception:
            print("Error sendMqtt")
            clientMqttMovements.disconnect()
            clientMqttMovements.loop_stop()


mqtt_thread = threading.Thread(target=run_mqtt, daemon=True)
mqtt_thread.start()


def click(txt):
    print("Button clicked: Entered text: ", txt)


def change_outlier_percentage(txt):
    global prc_outlier
    prc_outlier = float(txt.get())
    print("-- Outlier Percentage Changed to:", prc_outlier)


def change_rampage_percentage(txt):
    global prc_rampage
    prc_rampage = float(txt.get())
    print("-- Rampage Percentage Changed to:", prc_rampage)


def change_out_temp_1(txt):
    global output_temp1
    output_temp1 = float(txt.get())
    print("-- Output Temp 1 Changed to:", output_temp1)


def change_out_temp_2(txt):
    global output_temp2
    output_temp2 = float(txt.get())
    print("-- Output Temp 2 Changed to:", output_temp2)


def change_dir_1(txt):
    global sensor1_rampage_direction
    sensor1_rampage_direction = int(txt.get())
    print("-- Rampage Direction 1 Changed to:", sensor1_rampage_direction)


def change_dir_2(txt):
    global sensor2_rampage_direction
    sensor2_rampage_direction = int(txt.get())
    print("-- Rampage Direction 2 Changed to:", sensor2_rampage_direction)


def rampage_1():
    global sensor1_rampage
    if not sensor1_rampage:
        sensor1_rampage = True
        b1.config(text="Stop Sensor 1 Rampage")
        print(f"-- Triggered Sensor 1 Rampage. Temperature will "
              f"{'rise' if sensor1_rampage_direction > 0 else 'fall'} quickly.")
    else:
        sensor1_rampage = False
        b1.config(text="Start Sensor 1 Rampage")
        print(f"-- Sensor 1 Rampage is off. Consider manually setting Temperature 1.")


def rampage_2():
    global sensor2_rampage
    if not sensor2_rampage:
        sensor2_rampage = True
        b2.config(text="Stop Sensor 2 Rampage")
        print(f"-- Triggered Sensor 2 Rampage. Temperature will "
              f"{'rise' if sensor2_rampage_direction > 0 else 'fall'} quickly.")
    else:
        sensor2_rampage = False
        b2.config(text="Start Sensor 2 Rampage")
        print(f"-- Sensor 2 Rampage is off. Consider manually setting Temperature 2.")


def send_string_message(txt):
    message = txt.get()
    print("Sending message:", message)
    clientMqttMovements.publish(topic, message, qos=2)
    clientMqttMovements.loop()
    txt.delete(0, 'end')  # clear the input field after sending


root = Tk()
entry_pairs = 2  # Number of entry-button pairs in a row
fields_per_row = 3  # Number of rows

funcs = [change_outlier_percentage, change_rampage_percentage, change_out_temp_1, change_out_temp_2, change_dir_1,
         change_dir_2]
lbls = ['Outlier Chance %', 'Rampage Chance %', 'Temperature 1', 'Temperature 2', 'Rampage Dir 1 (-1 or 1)',
        'Rampage Dir 2 (-1 or 1)']
for row in range(fields_per_row):
    for i in range(entry_pairs):
        Label(root, text=lbls[row * 2 + i]).grid(row=row * 3, column=i * 2)
        entry = Entry(root)
        entry.grid(row=row * 3 + 1, column=i * 2)
        button = Button(root, text="Set",
                        command=lambda txt=entry, r=row, idx=i: funcs[r * 2 + idx](txt))
        print(funcs[row * 2 + i])
        button.grid(row=row * 3 + 2, column=i * 2, columnspan=2)
b1 = Button(root, text="Start Sensor 1 Rampage", command=rampage_1)
b1.grid(row=fields_per_row * 3, column=0)
b2 = Button(root, text="Start Sensor 2 Rampage", command=rampage_2)
b2.grid(row=fields_per_row * 3, column=2)

Label(root, text='Message').grid(row=fields_per_row * 3 + 1, column=0, sticky='w')
message_entry = Entry(root)
message_entry.grid(row=fields_per_row * 3 + 2, column=0, columnspan=50, sticky="ew")
root.grid_columnconfigure(0, weight=1)  # makes column 0 expandable
message_button = Button(root, text="Send Message", command=lambda: send_string_message(message_entry))
message_button.grid(row=fields_per_row * 3 + 3, column=0, columnspan=50, sticky="ew")

root.update()
window_width = root.winfo_width()
window_height = root.winfo_height()
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
position_top = int(screen_height / 2 - window_height / 2)
position_right = int(screen_width / 2 - window_width / 2)
root.geometry(f"{window_width}x{window_height}+{position_right}+{position_top}")
root.mainloop()


