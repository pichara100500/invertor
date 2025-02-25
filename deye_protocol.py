#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Протокол для работы с инверторами Deye/Dextrom по Modbus RTU/TCP
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
        logging.FileHandler("deye_protocol.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DeyeProtocol")


class DeyeModbusProtocol:
    """Класс для работы с инверторами Deye/Dextrom по Modbus RTU/TCP"""
    
    # Карта регистров Deye/Dextrom
    REGISTER_MAP = {
        "grid_voltage": {"address": 0x0004, "size": 1, "factor": 0.1, "unit": "V"},
        "grid_current": {"address": 0x0005, "size": 1, "factor": 0.1, "unit": "A"},
        "grid_power": {"address": 0x0006, "size": 1, "factor": 1, "unit": "W"},
        "pv1_voltage": {"address": 0x0007, "size": 1, "factor": 0.1, "unit": "V"},
        "pv1_current": {"address": 0x0008, "size": 1, "factor": 0.1, "unit": "A"},
        "pv1_power": {"address": 0x0009, "size": 1, "factor": 1, "unit": "W"},
        "pv2_voltage": {"address": 0x000A, "size": 1, "factor": 0.1, "unit": "V"},
        "pv2_current": {"address": 0x000B, "size": 1, "factor": 0.1, "unit": "A"},
        "pv2_power": {"address": 0x000C, "size": 1, "factor": 1, "unit": "W"},
        "battery_voltage": {"address": 0x0011, "size": 1, "factor": 0.1, "unit": "V"},
        "battery_current": {"address": 0x0012, "size": 1, "factor": 0.1, "unit": "A"},
        "battery_power": {"address": 0x0013, "size": 1, "factor": 1, "unit": "W"},
        "battery_soc": {"address": 0x0014, "size": 1, "factor": 1, "unit": "%"},
        "load_voltage": {"address": 0x0015, "size": 1, "factor": 0.1, "unit": "V"},
        "load_current": {"address": 0x0016, "size": 1, "factor": 0.1, "unit": "A"},
        "load_power": {"address": 0x0017, "size": 1, "factor": 1, "unit": "W"},
        "inverter_temperature": {"address": 0x0018, "size": 1, "factor": 1, "unit": "°C"},
        "operation_mode": {"address": 0x0100, "size": 1, "factor": 1, "unit": ""},
        "charge_source_priority": {"address": 0x0101, "size": 1, "factor": 1, "unit": ""},
        "output_source_priority": {"address": 0x0102, "size": 1, "factor": 1, "unit": ""},
        "max_charging_current": {"address": 0x0103, "size": 1, "factor": 1, "unit": "A"},
        "max_ac_charging_current": {"address": 0x0104, "size": 1, "factor": 1, "unit": "A"},
        "battery_type": {"address": 0x0105, "size": 1, "factor": 1, "unit": ""},
        "float_charging_voltage": {"address": 0x0106, "size": 1, "factor": 0.1, "unit": "V"},
        "bulk_charging_voltage": {"address": 0x0107, "size": 1, "factor": 0.1, "unit": "V"},
        "battery_cutoff_voltage": {"address": 0x0108, "size": 1, "factor": 0.1, "unit": "V"},
        "max_parallel_units": {"address": 0x0109, "size": 1, "factor": 1, "unit": ""},
        "machine_type": {"address": 0x010A, "size": 1, "factor": 1, "unit": ""},
        "topology": {"address": 0x010B, "size": 1, "factor": 1, "unit": ""},
        "output_model_setting": {"address": 0x010C, "size": 1, "factor": 1, "unit": ""},
        "solar_power_priority": {"address": 0x010D, "size": 1, "factor": 1, "unit": ""},
        "mppt_strings": {"address": 0x010E, "size": 1, "factor": 1, "unit": ""},
        "machine_model": {"address": 0x010F, "size": 1, "factor": 1, "unit": ""},
        "ac_input_voltage_range": {"address": 0x0110, "size": 1, "factor": 1, "unit": ""},
        "output_voltage": {"address": 0x0111, "size": 1, "factor": 0.1, "unit": "V"},
        "output_frequency": {"address": 0x0112, "size": 1, "factor": 0.1, "unit": "Hz"},
        "battery_reconnect_voltage": {"address": 0x0113, "size": 1, "factor": 0.1, "unit": "V"},
        "battery_under_voltage_alarm": {"address": 0x0114, "size": 1, "factor": 0.1, "unit": "V"},
        "discharge_limit_current": {"address": 0x0115, "size": 1, "factor": 1, "unit": "A"},
        "battery_equalization_enable": {"address": 0x0116, "size": 1, "factor": 1, "unit": ""},
        "battery_equalization_voltage": {"address": 0x0117, "size": 1, "factor": 0.1, "unit": "V"},
        "battery_equalization_time": {"address": 0x0118, "size": 1, "factor": 1, "unit": "min"},
        "battery_equalization_timeout": {"address": 0x0119, "size": 1, "factor": 1, "unit": "min"},
        "battery_equalization_interval": {"address": 0x011A, "size": 1, "factor": 1, "unit": "day"},
        "battery_equalization_boost": {"address": 0x011B, "size": 1, "factor": 1, "unit": ""},
        "pv_day_energy": {"address": 0x0200, "size": 1, "factor": 0.1, "unit": "kWh"},
        "pv_month_energy": {"address": 0x0201, "size": 1, "factor": 0.1, "unit": "kWh"},
        "pv_year_energy": {"address": 0x0202, "size": 1, "factor": 0.1, "unit": "kWh"},
        "pv_total_energy": {"address": 0x0203, "size": 2, "factor": 0.1, "unit": "kWh"},
        "today_energy": {"address": 0x0205, "size": 1, "factor": 0.1, "unit": "kWh"},
        "month_energy": {"address": 0x0206, "size": 1, "factor": 0.1, "unit": "kWh"},
        "year_energy": {"address": 0x0207, "size": 1, "factor": 0.1, "unit": "kWh"},
        "total_energy": {"address": 0x0208, "size": 2, "factor": 0.1, "unit": "kWh"},
        "co2_reduction": {"address": 0x020A, "size": 2, "factor": 0.1, "unit": "t"},
        "net_current": {"address": 0x020C, "size": 1, "factor": 0.1, "unit": "A"},
        "time_now": {"address": 0x020D, "size": 3, "factor": 1, "unit": ""},
        "error_codes": {"address": 0x0300, "size": 4, "factor": 1, "unit": ""},
        "warning_codes": {"address": 0x0304, "size": 4, "factor": 1, "unit": ""},
    }
    
    # Режимы работы
    OPERATION_MODES = {
        "POWER_ON": 0,
        "STANDBY": 1,
        "BYPASS": 2,
        "BATTERY": 3,
        "FAULT": 4,
        "HYBRID": 5,
        "CHARGE": 6
    }
    
    # Приоритеты источников заряда
    CHARGE_PRIORITIES = {
        "SOLAR_FIRST": 0,
        "GRID_FIRST": 1,
        "SOLAR_AND_GRID": 2,
        "ONLY_SOLAR": 3
    }
    
    # Приоритеты источников выхода
    OUTPUT_PRIORITIES = {
        "GRID_FIRST": 0,
        "SOLAR_FIRST": 1,
        "SBU_PRIORITY": 2
    }
    
    def __init__(self, config):
        """
        Инициализация адаптера для инвертора Deye/Dextrom
        
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
                logger.info("Successfully connected to Deye inverter")
                self.is_connected = True
                return True
            else:
                logger.error("Failed to connect to Deye inverter")
                self.is_connected = False
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Deye inverter: {str(e)}")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """Закрытие соединения с инвертором"""
        if self.client and self.is_connected:
            self.client.close()
            self.is_connected = False
            logger.info("Disconnected from Deye inverter")
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
            # Обработка специальных параметров
            if parameter_name == "time_now":
                # Формат времени: часы, минуты, секунды
                hours = registers[0]
                minutes = registers[1]
                seconds = registers[2]
                return {"value": f"{hours:02d}:{minutes:02d}:{seconds:02d}", "unit": ""}
                
            elif parameter_name == "error_codes" or parameter_name == "warning_codes":
                # Коды ошибок и предупреждений хранятся как битовые маски
                value = 0
                for i, reg in enumerate(registers):
                    value |= reg << (i * 16)
                return {"value": value, "unit": unit}
                
            # Стандартные числовые параметры
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
            "grid_voltage", "grid_power", 
            "pv1_voltage", "pv1_power",
            "pv2_voltage", "pv2_power",
            "battery_voltage", "battery_soc", "battery_power",
            "load_voltage", "load_power", 
            "inverter_temperature", "operation_mode"
        ]
        
        for param in parameters:
            result = self.read_parameter(param)
            if result:
                status[param] = result
        
        # Добавление ошибок и предупреждений, если они есть
        error_codes = self.read_parameter("error_codes")
        warning_codes = self.read_parameter("warning_codes")
        
        if error_codes and error_codes["value"] > 0:
            status["errors"] = error_codes
            
        if warning_codes and warning_codes["value"] > 0:
            status["warnings"] = warning_codes
        
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
    # Пример конфигурации для инвертора Deye/Dextrom
    config = {
        "connection_type": "serial",
        "port": "/dev/ttyUSB0",
        "baudrate": 9600,
        "unit_id": 1
    }
    
    # Создание экземпляра адаптера
    deye = DeyeModbusProtocol(config)
    
    # Подключение к инвертору
    if deye.connect():
        # Чтение статуса инвертора
        status = deye.get_status()
        print("Deye/Dextrom Inverter Status:")
        for key, value in status.items():
            print(f"  {key}: {value}")
            
        # Установка режима работы
        deye.set_mode("HYBRID")
        
        # Установка приоритета источника заряда
        deye.set_charge_priority("SOLAR_FIRST")
        
        # Установка приоритета источника выхода
        deye.set_output_priority("SBU_PRIORITY")
        
        # Отключение от инвертора
        deye.disconnect()
    else:
        print("Failed to connect to Deye/Dextrom inverter")
