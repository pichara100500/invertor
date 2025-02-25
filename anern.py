#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Протокол для работы с инверторами Anern по Modbus RTU/TCP
"""

import logging
import time
from pymodbus.client.sync import ModbusSerialClient, ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.exceptions import ModbusException

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("anern_protocol.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AnernProtocol")


class AnernProtocol:
    """Класс для работы с инверторами Anern по протоколу Modbus RTU/TCP"""
    
    # Карта регистров Anern
    REGISTER_MAP = {
        "status": {"address": 0x0000, "size": 1, "factor": 1, "unit": ""},
        "operation_mode": {"address": 0x0001, "size": 1, "factor": 1, "unit": ""},
        "grid_voltage": {"address": 0x0100, "size": 1, "factor": 0.1, "unit": "V"},
        "grid_frequency": {"address": 0x0101, "size": 1, "factor": 0.01, "unit": "Hz"},
        "output_voltage": {"address": 0x0102, "size": 1, "factor": 0.1, "unit": "V"},
        "output_frequency": {"address": 0x0103, "size": 1, "factor": 0.01, "unit": "Hz"},
        "output_power": {"address": 0x0104, "size": 1, "factor": 1, "unit": "W"},
        "output_current": {"address": 0x0105, "size": 1, "factor": 0.1, "unit": "A"},
        "load_percent": {"address": 0x0106, "size": 1, "factor": 1, "unit": "%"},
        "bus_voltage": {"address": 0x0110, "size": 1, "factor": 0.1, "unit": "V"},
        "pv_voltage": {"address": 0x0111, "size": 1, "factor": 0.1, "unit": "V"},
        "pv_charging_current": {"address": 0x0112, "size": 1, "factor": 0.1, "unit": "A"},
        "pv_power": {"address": 0x0113, "size": 1, "factor": 1, "unit": "W"},
        "battery_voltage": {"address": 0x0114, "size": 1, "factor": 0.1, "unit": "V"},
        "battery_current": {"address": 0x0115, "size": 1, "factor": 0.1, "unit": "A"},
        "battery_temperature": {"address": 0x0116, "size": 1, "factor": 1, "unit": "°C"},
        "inverter_temperature": {"address": 0x0117, "size": 1, "factor": 1, "unit": "°C"},
        "ambient_temperature": {"address": 0x0118, "size": 1, "factor": 1, "unit": "°C"},
        "battery_soc": {"address": 0x0120, "size": 1, "factor": 1, "unit": "%"},
        "error_flags_1": {"address": 0x0200, "size": 1, "factor": 1, "unit": ""},
        "error_flags_2": {"address": 0x0201, "size": 1, "factor": 1, "unit": ""},
        "max_charging_current": {"address": 0x0300, "size": 1, "factor": 1, "unit": "A"},
        "max_ac_charging_current": {"address": 0x0301, "size": 1, "factor": 1, "unit": "A"},
        "max_pv_charging_current": {"address": 0x0302, "size": 1, "factor": 1, "unit": "A"},
        "charge_source_priority": {"address": 0x0303, "size": 1, "factor": 1, "unit": ""},
        "output_source_priority": {"address": 0x0304, "size": 1, "factor": 1, "unit": ""},
        "battery_cutoff_voltage": {"address": 0x0305, "size": 1, "factor": 0.1, "unit": "V"},
        "battery_reconnect_voltage": {"address": 0x0306, "size": 1, "factor": 0.1, "unit": "V"},
    }
    
    # Режимы работы
    OPERATION_MODES = {
        "STANDBY": 0,
        "LINE_MODE": 1,
        "BATTERY_MODE": 2,
        "FAULT_MODE": 3,
        "OFF_GRID_MODE": 4,
        "BYPASS_MODE": 5,
        "SELF_TEST_MODE": 6
    }
    
    # Приоритеты источника заряда
    CHARGE_PRIORITIES = {
        "PV_FIRST": 0,
        "PV_AND_UTILITY": 1,
        "ONLY_PV": 2
    }
    
    # Приоритеты источника выхода
    OUTPUT_PRIORITIES = {
        "UTILITY_FIRST": 0,
        "SOLAR_FIRST": 1,
        "SOLAR_AND_UTILITY": 2,
        "ONLY_SOLAR": 3
    }
    
    def __init__(self, config):
        """
        Инициализация адаптера для инвертора Anern
        
        Args:
            config (dict): Словарь с конфигурацией подключения
                {
                    "connection_type": "serial" | "tcp",
                    "port": "/dev/ttyUSB0" | IP адрес,
                    "baudrate": 9600 (для serial),
                    "port": 502 (для tcp),
                    "unit_id": 1
                }
        """
        self.config = config
        self.client = None
        self.is_connected = False
        self.unit_id = config.get("unit_id", 1)
        self.connection_type = config.get("connection_type", "serial")
        self.timeout = config.get("timeout", 1)
        self.retries = config.get("retries", 3)
      def connect(self):
        """Установка соединения с инвертором"""
        try:
            if self.connection_type.lower() == "serial":
                # Соединение по Serial/RS485
                self.client = ModbusSerialClient(
                    method='rtu',
                    port=self.config.get("port", "/dev/ttyUSB0"),
                    baudrate=self.config.get("baudrate", 9600),
                    bytesize=self.config.get("bytesize", 8),
                    parity=self.config.get("parity", 'N'),
                    stopbits=self.config.get("stopbits", 1),
                    timeout=self.timeout
                )
            else:
                # Соединение по Modbus TCP
                self.client = ModbusTcpClient(
                    host=self.config.get("host", "192.168.1.100"),
                    port=self.config.get("port", 502),
                    timeout=self.timeout
                )
                
            if self.client.connect():
                logger.info("Successfully connected to Anern inverter")
                self.is_connected = True
                return True
            else:
                logger.error("Failed to connect to Anern inverter")
                self.is_connected = False
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Anern inverter: {str(e)}")
            self.is_connected = False
            return False
            
    def disconnect(self):
        """Закрытие соединения с инвертором"""
        if self.client and self.is_connected:
            self.client.close()
            self.is_connected = False
            logger.info("Disconnected from Anern inverter")
            return True
        return False
        
    def read_register(self, address, count=1):
        """
        Чтение регистра из инвертора
        
        Args:
            address (int): Адрес регистра
            count (int): Количество регистров для чтения
            
        Returns:
            list: Список значений регистров или None в случае ошибки
        """
        if not self.is_connected and not self.connect():
            return None
            
        for attempt in range(self.retries):
            try:
                response = self.client.read_holding_registers(
                    address=address,
                    count=count,
                    unit=self.unit_id
                )
                
                if response.isError():
                    logger.error(f"Error reading register {address}: {response}")
                    continue
                    
                return response.registers
                
            except ModbusException as e:
                logger.error(f"Modbus error reading register {address}: {str(e)}")
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error reading register {address}: {str(e)}")
                time.sleep(0.1)
                
        return None
        
    def write_register(self, address, value):
        """
        Запись значения в регистр инвертора
        
        Args:
            address (int): Адрес регистра
            value (int): Значение для записи
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        if not self.is_connected and not self.connect():
            return False
            
        for attempt in range(self.retries):
            try:
                response = self.client.write_register(
                    address=address,
                    value=value,
                    unit=self.unit_id
                )
                
                if response.isError():
                    logger.error(f"Error writing register {address}: {response}")
                    continue
                    
                return True
                
            except ModbusException as e:
                logger.error(f"Modbus error writing register {address}: {str(e)}")
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error writing register {address}: {str(e)}")
                time.sleep(0.1)
                
        return False
      def read_parameter(self, parameter_name):
        """
        Чтение параметра из инвертора по имени параметра
        
        Args:
            parameter_name (str): Имя параметра из REGISTER_MAP
            
        Returns:
            dict: Словарь с значением и единицей измерения или None в случае ошибки
        """
        if parameter_name not in self.REGISTER_MAP:
            logger.error(f"Unknown parameter: {parameter_name}")
            return None
            
        register_info = self.REGISTER_MAP[parameter_name]
        address = register_info["address"]
        size = register_info["size"]
        factor = register_info["factor"]
        unit = register_info["unit"]
        
        registers = self.read_register(address, size)
        if registers is None:
            return None
            
        try:
            if size == 1:
                value = registers[0] * factor
            elif size == 2:
                value = (registers[0] << 16 | registers[1]) * factor
            else:
                value = registers[0] * factor
                
            return {"value": value, "unit": unit}
            
        except Exception as e:
            logger.error(f"Error processing parameter {parameter_name}: {str(e)}")
            return None
            
    def write_parameter(self, parameter_name, value):
        """
        Запись параметра в инвертор по имени параметра
        
        Args:
            parameter_name (str): Имя параметра из REGISTER_MAP
            value (float): Значение для записи
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        if parameter_name not in self.REGISTER_MAP:
            logger.error(f"Unknown parameter: {parameter_name}")
            return False
            
        register_info = self.REGISTER_MAP[parameter_name]
        address = register_info["address"]
        factor = register_info["factor"]
        
        # Преобразование значения с учетом множителя
        register_value = int(value / factor)
        
        return self.write_register(address, register_value)
        
    def get_status(self):
        """
        Получение общего статуса инвертора
        
        Returns:
            dict: Словарь с основными параметрами инвертора
        """
        status = {}
        
        # Основные параметры для статуса
        parameters = [
            "operation_mode", "grid_voltage", "output_voltage", "output_power",
            "pv_voltage", "pv_power", "battery_voltage", "battery_current",
            "battery_soc", "inverter_temperature"
        ]
        
        for param in parameters:
            result = self.read_parameter(param)
            if result:
                status[param] = result
                
        # Добавление кодов ошибок, если они есть
        error_flags_1 = self.read_parameter("error_flags_1")
        error_flags_2 = self.read_parameter("error_flags_2")
        
        if error_flags_1 and error_flags_1["value"] > 0:
            status["has_errors"] = True
            status["error_flags_1"] = error_flags_1
        
        if error_flags_2 and error_flags_2["value"] > 0:
            status["has_errors"] = True
            status["error_flags_2"] = error_flags_2
            
        return status
        
    def set_mode(self, mode):
        """
        Установка режима работы инвертора
        
        Args:
            mode (str): Режим работы из OPERATION_MODES
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        if mode in self.OPERATION_MODES:
            mode_value = self.OPERATION_MODES[mode]
            return self.write_parameter("operation_mode", mode_value)
        else:
            logger.error(f"Unknown operation mode: {mode}")
            return False
            
    def set_charge_priority(self, priority):
        """
        Установка приоритета источника заряда
        
        Args:
            priority (str): Приоритет из CHARGE_PRIORITIES
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        if priority in self.CHARGE_PRIORITIES:
            priority_value = self.CHARGE_PRIORITIES[priority]
            return self.write_parameter("charge_source_priority", priority_value)
        else:
            logger.error(f"Unknown charge priority: {priority}")
            return False
            
    def set_output_priority(self, priority):
        """
        Установка приоритета источника выхода
        
        Args:
            priority (str): Приоритет из OUTPUT_PRIORITIES
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        if priority in self.OUTPUT_PRIORITIES:
            priority_value = self.OUTPUT_PRIORITIES[priority]
            return self.write_parameter("output_source_priority", priority_value)
        else:
            logger.error(f"Unknown output priority: {priority}")
            return False
            
    def set_max_charging_current(self, current):
        """
        Установка максимального тока заряда
        
        Args:
            current (int): Максимальный ток заряда в амперах
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        return self.write_parameter("max_charging_current", current)
        
    def set_battery_cutoff_voltage(self, voltage):
        """
        Установка напряжения отсечки батареи
        
        Args:
            voltage (float): Напряжение отсечки батареи в вольтах
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        return self.write_parameter("battery_cutoff_voltage", voltage)


# Пример использования
if __name__ == "__main__":
    # Пример конфигурации для инвертора Anern
    config = {
        "connection_type": "serial",
        "port": "/dev/ttyUSB0",
        "baudrate": 9600,
        "unit_id": 1
    }
    
    # Создание экземпляра адаптера
    anern = AnernProtocol(config)
    
    # Подключение к инвертору
    if anern.connect():
        # Чтение статуса инвертора
        status = anern.get_status()
        print("Anern Inverter Status:")
        for key, value in status.items():
            print(f"  {key}: {value}")
            
        # Установка приоритета источника заряда
        anern.set_charge_priority("PV_FIRST")
        
        # Отключение от инвертора
        anern.disconnect()
    else:
        print("Failed to connect to Anern inverter")
