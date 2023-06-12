import mysql.connector
from start2 import subscriber
import threading


def createMySqlConnection():
    mydb = mysql.connector.connect(
        host="steve-db-cms.cqry44wn7lp3.ap-south-1.rds.amazonaws.com",
        user='cms_admin',
        password='s2VuUS34wxWO18yQtbkz',
        database="MqttWeb"
        )
    print("db connect inside start file")
    return mydb
def create_instances():
    # mydb = createMySqlConnection()
    # cursor = mydb.cursor()
    # sql = "select imei from subscriberOne"
    # cursor.execute(sql)
    # record = cursor.fetchall()
    # print(record)
    # for i in range (len(record)):
    #     print(record[i][0])
    #     str = record[i][0]
    #     thread = threading.Thread(target=create_instance, args=(str,))
    #     thread.start()
    #     # proc = Process(target = create_instance, args = (str,))
    #     # proc.start()
    str = "866907056709164"
    # str = "864394042600181"
    thread = threading.Thread(target=create_instance, args=(str,))
    thread.start()

def create_instance(imei):
    sub = subscriber()
    sub._start(imei)
         
# if __name__ == '__main__':
#     create_instances()
create_instances()

