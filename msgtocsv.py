import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import json
import pandas as pd
import time
import os.path
import sys

class mqtttocsv:

    NONE = 0
    OBSERVATION = 100
    REQUEST = 200
    RESPONSE = 300
    STATUS = 400
    NOTICE = 500
    UNDEFINED = 600

    def __init__(self):
        self._temp = 0

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("connected OK")
        else:
            print("Bad connection Returned code=", rc)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("subscribed: " + str(mid))


    def on_message(self, client, userdata, msg):
        renew = {}
        listblk = []
        if json.loads(msg.payload)['type'] == mqtttocsv.OBSERVATION:
            print(str(msg.payload.decode("utf-8")))
            if json.loads(msg.payload)['type'] == 100 and json.loads(msg.payload)['nid'] > "50":
                blk = json.loads(msg.payload)['content']
                nid = json.loads(msg.payload)['nid']
                renew['time'] = blk['time']
                blk = {key : [value[0]] for key, value in blk.items() if key != nid and key != "time"}
                for key, value in blk.items():
                    renew[key] = value
                if len(renew)==1:
                    pass
                else:
                    self.tocsv(renew)
            else:
                pass

    def on_disconnect(self, client, userdata, flags, rc=0):
        print("disconnect " + str(rc))

    def tocsv(self, blk):
        data = blk
        col = []
        for k,v in data.items():
            col.append(k)

        df = pd.DataFrame(data)
        if os.path.isfile('./obs.csv'):
            df2 = pd.read_csv('./obs.csv', header=0)
            df3 = pd.merge(df2, df, how="outer")
            df3.to_csv('./obs.csv', mode='w', index=False)
        else:
            df.to_csv('./obs.csv', mode='w', index=False, header=True)

    def _loadfile(self):
        with open('./conf/config.json') as json_file:
            json_data = json.load(json_file)
            return json_data['host'], json_data['keepalive'], json_data['port']
    def process_run(self):
        client = mqtt.Client()
        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        client.on_subscribe = self.on_subscribe
        host, keepalive, port = self._loadfile()
        client.on_message = self.on_message

        client.connect(host, port, keepalive)
        client.subscribe("cvtgate/" + "#", 2)
        client.loop_forever()


if __name__ == "__main__":
    a = mqtttocsv()
    a.process_run()
