from machine import UART
from machine import Pin
from machine import Timer
from umqtt import MQTTClient
import dataCall
import cellLocator
import _thread
import utime
import log
import net
import checkNet
import sim
import ustruct
import ujson

Work_Led = Pin(Pin.GPIO12, Pin.OUT, Pin.PULL_DISABLE, 1)

PROJECT_NAME = "QuecPython_EC600U"
PROJECT_VERSION = "1.0.2"

checknet = checkNet.CheckNetwork(PROJECT_NAME, PROJECT_VERSION)
TaskEnable = True  # 调用disconnect后会通过该状态回收线程资源
device_address = 0x01
msg_id = 0
state = 0
mqtt_sub_msg = {}
time_interval = 8 * 3600 * 1000

log.basicConfig(level=log.INFO)
app_log = log.getLogger("app_log")

t = Timer(Timer.Timer1)


def watch_dog_task():
    while True:
        if Work_Led.read():
            Work_Led.write(0)
        else:
            Work_Led.write(1)
        utime.sleep(10)


class MqttClient():
    # 说明：reconn该参数用于控制使用或关闭umqtt内部的重连机制，默认为True，使用内部重连机制。
    # 如需测试或使用外部重连机制可参考此示例代码，测试前需将reconn=False,否则默认会使用内部重连机制！
    def __init__(self, clientid, server, port, user=None, password=None, keepalive=0, ssl=False, ssl_params={},
                 reconn=True):
        self.__clientid = clientid
        self.__pw = password
        self.__server = server
        self.__port = port
        self.__uasename = user
        self.__keepalive = keepalive
        self.__ssl = ssl
        self.__ssl_params = ssl_params
        self.topic = None
        self.qos = None
        # 网络状态标志
        self.__nw_flag = True
        # 创建互斥锁
        self.mp_lock = _thread.allocate_lock()
        # 创建类的时候初始化出mqtt对象
        self.client = MQTTClient(self.__clientid, self.__server, self.__port, self.__uasename, self.__pw,
                                 keepalive=self.__keepalive, ssl=self.__ssl, ssl_params=self.__ssl_params,
                                 reconn=reconn)

    def connect(self):
        self.client.connect(clean_session=False)
        # 注册网络回调函数，网络状态发生变化时触发
        flag = dataCall.setCallback(self.nw_cb)
        if flag != 0:
            # 回调注册失败
            raise Exception("Network callback registration failed")

    def set_callback(self, sub_cb):
        self.client.set_callback(sub_cb)

    def error_register_cb(self, func):
        self.client.error_register_cb(func)

    def subscribe(self, topic, qos=0):
        self.topic = topic  # 保存topic ，多个topic可使用list保存
        self.qos = qos  # 保存qos
        self.client.subscribe(topic, qos)

    def publish(self, topic, msg, qos=0):
        self.client.publish(topic, msg, qos)

    def disconnect(self):
        global TaskEnable
        # 关闭wait_msg的监听线程
        TaskEnable = False
        # 关闭之前的连接，释放资源
        self.client.disconnect()

    def reconnect(self):
        # 判断锁是否已经被获取
        if self.mp_lock.locked():
            return
        self.mp_lock.acquire()
        # 重新连接前关闭之前的连接，释放资源(注意区别disconnect方法，close只释放socket资源，disconnect包含mqtt线程等资源)
        self.client.close()
        # 重新建立mqtt连接
        while True:
            net_sta = net.getState()  # 获取网络注册信息
            if net_sta != -1 and net_sta[1][0] == 1:
                call_state = dataCall.getInfo(1, 0)  # 获取拨号信息
                if (call_state != -1) and (call_state[2][0] == 1):
                    try:
                        # 网络正常，重新连接mqtt
                        self.connect()
                    except Exception as e:
                        # 重连mqtt失败, 5s继续尝试下一次
                        self.client.close()
                        utime.sleep(5)
                        continue
                else:
                    # 网络未恢复，等待恢复
                    utime.sleep(10)
                    continue
                # 重新连接mqtt成功，订阅Topic
                try:
                    # 多个topic采用list保存，遍历list重新订阅
                    if self.topic is not None:
                        self.client.subscribe(self.topic, self.qos)
                    self.mp_lock.release()
                except:
                    # 订阅失败，重新执行重连逻辑
                    self.client.close()
                    utime.sleep(5)
                    continue
            else:
                utime.sleep(5)
                continue
            break  # 结束循环
        # 退出重连
        return True

    def nw_cb(self, args):
        nw_sta = args[1]
        if nw_sta == 1:
            # 网络连接
            app_log.info("*** network connected! ***")
            self.__nw_flag = True
        else:
            # 网络断线
            app_log.info("*** network not connected! ***")
            self.__nw_flag = False

    def __listen(self):
        while True:
            try:
                if not TaskEnable:
                    break
                self.client.wait_msg()
            except OSError as e:
                # 判断网络是否断线
                if not self.__nw_flag:
                    # 网络断线等待恢复进行重连
                    self.reconnect()
                # 在socket状态异常情况下进行重连
                elif self.client.get_mqttsta() != 0 and TaskEnable:
                    self.reconnect()
                else:
                    # 这里可选择使用raise主动抛出异常或者返回-1
                    return -1

    def loop_forever(self):
        _thread.start_new_thread(self.__listen, ())


class Uart2(object):
    def __init__(self, no=UART.UART2, bate=115200, data_bits=8, parity=0, stop_bits=1, flow_control=0):
        self.uart = UART(no, bate, data_bits, parity, stop_bits, flow_control)
        self.uart.control_485(UART.GPIO28, 0)
        self.uart.set_callback(self.callback)
        self.modbus_rtu = None

    def set_modbus_rtu_instance(self, modbus_rtu_instance):
        self.modbus_rtu = modbus_rtu_instance

    def callback(self, para):
        app_log.debug("call para:{}".format(para))
        if (0 == para[0]):
            self.uartRead(para[2])

    def uartWrite(self, msg):
        hex_msg = [hex(x) for x in msg]
        app_log.debug("Write msg:{}".format(hex_msg))
        self.uart.write(msg)

    def uartRead(self, len):
        msg = self.uart.read(len)
        hex_msg = [hex(x) for x in msg]
        app_log.debug("Read msg: {}".format(hex_msg))
        if self.modbus_rtu:
            self.modbus_rtu.handle_response(msg)


class ModbusRTU:
    def __init__(self, device_address):
        self.device_address = device_address
        self.distance = 0

    def calculate_crc(self, data):
        crc = 0xFFFF
        for pos in data:
            crc ^= pos
            for _ in range(8):
                if crc & 0x0001:
                    crc >>= 1
                    crc ^= 0xA001
                else:
                    crc >>= 1
        return crc

    def reverse_crc(self, crc):
        return ((crc & 0xFF) << 8) | ((crc >> 8) & 0xFF)

    def build_message(self, device_address, function_code, reg_address, reg_number):
        # 构建 Modbus 请求消息
        message = ustruct.pack(
            '>BBHH', device_address, function_code, reg_address, reg_number)
        # 计算 CRC 校验码
        crc = self.calculate_crc(message)
        crc_bytes = ustruct.pack('<H', crc)
        # app_log.debug("build_message:{}".format(
        #     ['0x{:02X}'.format(b) for b in (message + crc_bytes)]))
        return message + crc_bytes

    def build_stop_message(self, device_address, function_code, reg_address, reg_number, reg_value1, reg_value2):
        # 构建 Modbus 请求消息
        message = ustruct.pack(
            '>BBHHBH', device_address, function_code, reg_address, reg_number, reg_value1, reg_value2)
        # 计算 CRC 校验码
        crc = self.calculate_crc(message)
        crc_bytes = ustruct.pack('<H', crc)
        # app_log.debug("build_message:{}".format(
        #     ['0x{:02X}'.format(b) for b in (message + crc_bytes)]))
        return message + crc_bytes

    def send_message(self, message, timeout=50):
        uart_inst.uartWrite(message)
        utime.sleep_ms(timeout)  # 等待响应的时间，可以根据需要调整

    def handle_response(self, data):
        global msg_id
        if len(data) < 5 or len(data) == 6 or len(data) == 7:
            app_log.error("Invalid response length")
            return
        elif len(data) == 5:
            address, function_code, value1, crc_received = ustruct.unpack(
                '>BBBH', data)
        elif len(data) == 8:
            address, function_code, value1, value2, crc_received = ustruct.unpack(
                '>BBHHH', data)
        elif len(data) == 9:
            address, function_code, bytes_num, value1, value2, crc_received = ustruct.unpack(
                '>BBBHHH', data)
        else:
            app_log.error("The data is linked together")
            return
        crc_calculated = self.calculate_crc(data[:-2])
        crc_calculated_swapped = self.reverse_crc(crc_calculated)
        if crc_received != crc_calculated_swapped:
            app_log.error("CRC mismatch: received=0x{:04X}, calculated=0x{:04X}".format(
                crc_received, crc_calculated))
        elif function_code == 0x83:
            app_log.error("measure error: value1=0x{:02X}".format(value1))
            modbus_rtu.query_single_measure()
        elif function_code == 0x03 and value1 == 0x0000 and value2 == 0x0000:
            app_log.error(
                "measure failure: value1=0x{:04X}, value2=0x{:04X}".format(value1, value2))
        elif function_code == 0x03 and (value1 != 0x0000 or value2 != 0x0000):
            app_log.debug(
                "measure successful: value1=0x{:04X}, value2=0x{:04X}".format(value1, value2))
            self.distance = ((value1 << 16) + value2)/1000
            app_log.info("distance: {}".format(self.distance))
            msg_id += 1
            mqtt_client.publish(property_publish_topic.encode(
                'utf-8'), msg_distance.format(msg_id, modbus_rtu.distance).encode('utf-8'))
        elif function_code == 0x10:
            app_log.debug("measure stop successful: value1=0x{:04X}, value2=0x{:04X}".format(
                value1, value2))

    def query_single_measure(self):
        message = self.build_message(
            self.device_address, 0x03, 0x000F, 0x0002)
        self.send_message(message)

    def query_auto_measure(self):
        message = self.build_message(
            self.device_address, 0x03, 0x0013, 0x0002)
        self.send_message(message)

    def query_stop_measure(self):
        message = self.build_stop_message(
            self.device_address, 0x10, 0x0031, 0x0001, 0x02, 0x0001)
        self.send_message(message)


def cell_location_task():
    global msg_id
    while True:
        utime.sleep(86400)
        cell_location = cellLocator.getLocation(
            "www.queclocator.com", 80, "qa6qTK91597826z6", 8, 1)
        msg_id += 1
        mqtt_client.publish(property_publish_topic.encode(
            'utf-8'), msg_cellLocator.format
            (msg_id, cell_location[0], cell_location[1], cell_location[2]).encode('utf-8'))


def sim_task():
    global msg_id
    while True:
        sim_imsi = sim.getImsi()
        sim_iccid = sim.getIccid()
        msg_id += 1
        mqtt_client.publish(property_publish_topic.encode(
            'utf-8'), msg_sim.format(msg_id, sim_imsi, sim_iccid).encode('utf-8'))
        utime.sleep(7200)


def timing_task(args):
    modbus_rtu.query_single_measure()
    app_log.debug("time_interval: {}".format(time_interval))


def update_time_interval(new_time_interval):
    global time_interval
    global t

    if new_time_interval != time_interval:
        app_log.info(
            "Updating interval to {} seconds".format(new_time_interval))
        time_interval = new_time_interval
        t.stop()
        t.start(period=int(time_interval),
                mode=t.PERIODIC, callback=timing_task)


def mqtt_sub_cb(topic, msg):
    global state, mqtt_sub_msg
    app_log.info("Subscribe Recv: Topic={},Msg={}".format(
        topic.decode(), msg.decode()))
    mqtt_sub_msg = ujson.loads(msg.decode())
    state = 1
    app_log.debug(mqtt_sub_msg['params'])


def process_relay_logic():
    global state, msg_id, mqtt_sub_msg, time_interval

    if 'method' not in mqtt_sub_msg:
        app_log.error('method is missing')
    elif not mqtt_sub_msg['method']:
        app_log.error('method is empty')
    elif 'thing.service.single_measure' in mqtt_sub_msg['method']:
        modbus_rtu.query_single_measure()
        state = 0
        mqtt_sub_msg = {}
        return
    elif 'thing.service.auto_measure' in mqtt_sub_msg['method']:
        modbus_rtu.query_auto_measure()
        state = 0
        mqtt_sub_msg = {}
        return
    elif 'thing.service.stop_measure' in mqtt_sub_msg['method']:
        modbus_rtu.query_stop_measure()
        state = 0
        mqtt_sub_msg = {}
        return

    if 'params' not in mqtt_sub_msg:
        app_log.error('params is missing')
    elif not mqtt_sub_msg['params']:
        app_log.error('params is empty')
    elif 'set_time_interval' in mqtt_sub_msg['params']:
        update_time_interval(
            mqtt_sub_msg['params']['set_time_interval'] * 3600 * 1000)
        msg_id += 1
        mqtt_client.publish(property_publish_topic.encode(
            'utf-8'), msg_time_interval.format(msg_id, time_interval / 3600000).encode('utf-8'))

    state = 0
    mqtt_sub_msg = {}


if __name__ == '__main__':
    utime.sleep(5)
    checknet.poweron_print_once()
    stagecode, subcode = checknet.wait_network_connected(30)
    if stagecode == 3 and subcode == 1:
        app_log.info('Network connection successful!')

        uart_inst = Uart2()
        modbus_rtu = ModbusRTU(device_address=device_address)
        uart_inst.set_modbus_rtu_instance(modbus_rtu)

        _thread.start_new_thread(watch_dog_task, ())
        t.start(period=int(time_interval),
                mode=t.PERIODIC, callback=timing_task)

        msg_cellLocator = """{{
                        "id": "{0}",
                        "version": "1.0",
                        "params": {{
                            "cell_locator": {{
                                "longitude": {{
                                    "value": {1}
                                }},
                                "latitude": {{
                                    "value": {2}
                                }},
                                "accuracy": {{
                                "value": {3}
                                }}
                            }}
                        }},
                        "method": "thing.event.property.post"
                    }}"""

        msg_sim = """{{
                    "id": "{0}",
                    "version": "1.0",
                    "params": {{
                        "IMSI": {{
                            "value": "{1}"
                        }},
                        "ICCID": {{
                            "value": "{2}"
                        }}
                    }},
                    "method": "thing.event.property.post"
                }}"""

        msg_netStatus = """{{
                                        "id": "{0}",
                                        "version": "1.0",
                                        "params": {{
                                            "net_status": {{
                                                "stage_code": {{
                                                    "value": "{1}"
                                                }},
                                                "sub_code": {{
                                                    "value": "{2}"
                                                }}
                                            }}
                                        }},
                                        "method": "thing.event.property.post"
                                    }}"""

        msg_distance = """{{
                                "id": "{0}",
                                "version": "1.0",
                                "params": {{
                                    "distance": {{
                                        "value": {1}
                                    }}
                                }},
                                "method": "thing.event.property.post"
                             }}"""

        msg_time_interval = """{{
                                "id": "{0}",
                                "version": "1.0",
                                "params": {{
                                    "set_time_interval": {{
                                        "value": {1}
                                    }}
                                }},
                                "method": "thing.event.property.post"
                             }}"""

        ProductKey = "k12xgqCNomb"  # 产品标识
        DeviceName = "L2s-001"  # 设备名称

        property_subscribe_topic = "/sys" + "/" + ProductKey + "/" + \
            DeviceName + "/" + "thing/service/property/set"
        property_publish_topic = "/sys" + "/" + ProductKey + "/" + \
            DeviceName + "/" + "thing/event/property/post"

        # 创建一个mqtt实例
        mqtt_client = MqttClient(clientid="k12xgqCNomb.L2s-001|securemode=2,signmethod=hmacsha256,timestamp=1721701315215|",
                                 server="iot-06z00eu1sc0k51m.mqtt.iothub.aliyuncs.com",
                                 port=1883,
                                 user="L2s-001&k12xgqCNomb",
                                 password="658be3c854c2aa67a3cc29d6cc932bb01e0c4af5a553913fc5d38a47c938b020",
                                 keepalive=60, reconn=False)

        def mqtt_err_cb(err):
            app_log.error("thread err:%s" % err)
            mqtt_client.reconnect()  # 可根据异常进行重连

        # 设置消息回调
        mqtt_client.set_callback(mqtt_sub_cb)
        mqtt_client.error_register_cb(mqtt_err_cb)
        # 建立连接
        try:
            mqtt_client.connect()
        except Exception as e:
            app_log.error('e=%s' % e)

        # 订阅主题
        app_log.info(
            "Connected to aliyun, subscribed to: {}".format(property_subscribe_topic))
        mqtt_client.subscribe(property_subscribe_topic.encode('utf-8'), qos=0)

        msg_id += 1
        mqtt_client.publish(property_publish_topic.encode(
            'utf-8'), msg_netStatus.format(msg_id, stagecode, subcode).encode('utf-8'))

        mqtt_client.loop_forever()

        _thread.start_new_thread(cell_location_task, ())
        _thread.start_new_thread(sim_task, ())

        while True:
            if state == 1:
                process_relay_logic()
            utime.sleep_ms(50)
    else:
        app_log.error('Network connection failed! stagecode = {}, subcode = {}'.format(
            stagecode, subcode))
