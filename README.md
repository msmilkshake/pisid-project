## Install the MongoDB Replica Set:
- Extract the contents of [replicaimdb.zip](replicaimdb.zip) into C:/

### Commands to start the replica set:

```sh
mongod --config C:\replicaimdb\server1\server1.conf
mongod --config C:\replicaimdb\server2\server2.conf
mongod --config C:\replicaimdb\server3\server3.conf
```

## MQTT needs a Broker running:
So we install and run Mosquitto:
https://mosquitto.org/download/

## Quick Windows Terminal Tutorial
- To split a terminal to the right press:
`Shift + Alt + '+'` (Not the NUMPAD +)
- To split a terminal down press:
    `Shift + Alt + '-'` (Not the NUMPAD -)
- To adjust terminal window size:
  `Shift + Alt + Arrow Key`

# Python Instructions
- Install a fresh venv for the project scope (python 3.11 or below)
- run the command below to install the dependencies
```shell
pip install -r requirements.txt
```
- Mark the python folder as sources!!

# MySQL Instructions
- To Reset the db Password you can use the following command in phpmyadmin SQL console:
```SQL
SET PASSWORD FOR 'root'@'localhost' = PASSWORD('your_root_password');
```
**WARNING**
You will lose access to the phpmyadmin dashboard

To fix this look for the file `config.inc.php` located in `C:\xampp\phpMyAdmin` and edit
the line `$cfg['Servers'][$i]['password'] = 'your_root_password';`


- Verify the [WriteMysql.ini](WriteMysql.ini) for the db configuration

```shell
mongod.exe --config "C:\sensor-replica-set\server1\server1.conf"
mongod.exe --config "C:\sensor-replica-set\server2\server2.conf"
mongod.exe --config "C:\sensor-replica-set\server3\server3.conf"
```

