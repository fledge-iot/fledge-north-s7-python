# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge.readthedocs.io/
# FLEDGE_END


# ***********************************************************************
# * DISCLAIMER:
# *
# * All sample code is provided by ACDP for illustrative purposes only.
# * These examples have not been thoroughly tested under all conditions.
# * ACDP provides no guarantee nor implies any reliability,
# * serviceability, or function of these programs.
# * ALL PROGRAMS CONTAINED HEREIN ARE PROVIDED TO YOU "AS IS"
# * WITHOUT ANY WARRANTIES OF ANY KIND. ALL WARRANTIES INCLUDING
# * THE IMPLIED WARRANTIES OF NON-INFRINGEMENT, MERCHANTABILITY
# * AND FITNESS FOR A PARTICULAR PURPOSE ARE EXPRESSLY DISCLAIMED.
# ************************************************************************

""" S7 North plugin"""
import snap7
from snap7.util import *
from snap7.types import *

import asyncio
import json
import re

from fledge.common import logger
from fledge.plugins.north.common.common import *

""" Plugin for writing data to a S7 PLC

    This plugin uses the snap7 library, to install this perform the following steps:

        pip install python-snap7

    You can learn more about this library here:
        https://pypi.org/project/python-snap7/
    The library is licensed under the BSD License (BSD).

    As an example of how to use this library:

        import snap7

        client = snap7.client.Client()
        client.connect("127.0.0.1", 0, 0, 1012)
        client.get_connected()

        data = client.db_read(1, 0, 4)

        print(data)
        ???client.close()

"""

__author__ = "Sebastian Kropatschek"
__copyright__ = "Copyright (c) 2021 Austrian Center for Digital Production (ACDP)"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__, level=logging.INFO)
""" Setup the access to the logging system of Fledge """

_CONFIG_CATEGORY_NAME = "S7"
_CONFIG_CATEGORY_DESCRIPTION = "S7 Python North Plugin"
_DEFAULT_CONFIG = {
    'plugin': {
         'description': 'S7 North Plugin',
         'type': 'string',
         'default': 's7_python',
         'readonly': 'true'
    },
    'host': {
        'description': 'Host IP address of the PLC',
        'type': 'string',
        'default': '127.0.0.1',
        'order': '2',
        'displayName': 'Host TCP Address'
    },
    'port': {
        'description': 'Port of the PLC',
        'type': 'integer',
        'default': '102',
        'order': '5',
        'displayName': 'Port'
    },
    'rack': {
        'description': 'Rack number where the PLC is located',
        'type': 'integer',
        'default': '0',
        'order': '3',
        'displayName': 'Rack'
    },
    'slot': {
        'description': 'Slot number where the CPU is located.',
        'type': 'integer',
        'default': '0',
        'order': '4',
        'displayName': 'Slot'
    },
    'map': {
        'description': 'S7 register map',
        'type': 'JSON',
        # 'default': json.dumps({
        #     "DB": {
        #         "788": {
        #             "0.0":   {"asset": "S7", "datapoint": "Job",             "type": "String[254]"},
        #             "256.0": {"asset": "S7", "datapoint": "Count",           "type": "UINT"},
        #             "258.0": {"asset": "S7", "datapoint": "Active",          "type": "BOOL"},
        #             "258.1": {"asset": "S7", "datapoint": "TESTVAR_Bits",    "type": "BOOL"},
        #             "260.0": {"asset": "S7", "datapoint": "TESTVAR_Word",    "type": "WORD"},
        #             "262.0": {"asset": "S7", "datapoint": "TESTVAR_Int",     "type": "INT"},
        #             "264.0": {"asset": "S7", "datapoint": "TESTVAR_DWord",   "type": "DWORD"},
        #             "268.0": {"asset": "S7", "datapoint": "TESTVAR_DInt",    "type": "DINT"},
        #             "272.0": {"asset": "S7", "datapoint": "TESTVAR_Real",    "type": "REAL"}#,
        #             #"276.0": {"name": "TESTVAR_String",  "type": "STRING"}#,
        #             #"532.0": {"name": "TESTVAR_ChArray", "type": "Char[11]"}
        #         },
        #         "789": {
        #             "1371.3": {"asset": "S7", "datapoint": "Test_Bool_4", "type": "Bool"},
        #             "1371.5": {"asset": "S7", "datapoint": "Test_Bool_6", "type": "Bool"}
        #         }
        #     }
        # }),
        'default': json.dumps({
            "sinusoid": {
                "sinusoid": {"DB": "788", "index": "272.0", "type": "Real"},
                "static-1": {"DB": "788", "index": "262.0", "type": "Int", "value": 1234},
            }
        }),
        'order': '6',
        'displayName': 'Register Map'
    },
    'supportBool': {
        'type': 'boolean',
        'description': 'Activation of write support for the type boolean. This setting is not recommended because only whole bytes can be written. Procedure: The byte is read, then a bit is changed and finally the whole byte with the changed bit is written again. In the meantime, however, a bit may have changed, which can be very dangerous)',
        'default': 'False',
        'order': '7',
        'displayName': 'boolean write support (dangerous)'
    },
    'verify': {
        'type': 'boolean',
        'description': '',
        'default': 'False',
        'order': '8',
        'displayName': 'verify'
    },
    'supportStaticValues': {
        'type': 'boolean',
        'description': 'Activation of write support for the type boolean. This setting is not recommended because only whole bytes can be written. Procedure: The byte is read, then a bit is changed and finally the whole byte with the changed bit is written again. In the meantime, however, a bit may have changed, which can be very dangerous)',
        'default': 'False',
        'order': '9',
        'displayName': 'Static value support'
    }
}


def plugin_info():
    return {
        'name': 's7_north_python',
        'version': '2.1.0',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(data):
    _LOGGER.info('Initializing S7 North Python Plugin')
    global s7_north, config
    s7_north = S7NorthPlugin()

    config = data
    _LOGGER.warn(config)
    _LOGGER.info(
        f'Initializing plugin with host: {config["host"]["value"]}, port: {config["port"]["value"]}, rack: {config["rack"]["value"]}  and slot: {config["slot"]["value"]}')
    return config


async def plugin_send(data, payload, stream_id):
    try:
        _LOGGER.info(f'S7 North Python - plugin_send: {stream_id}')
        is_data_sent, new_last_object_id, num_sent = await s7_north.send_payloads(payload)
    except asyncio.CancelledError as ex:
        _LOGGER.exception(f'Exception occurred in plugin_send: {ex}')
    else:
        _LOGGER.info('payload sent successfully')
        return is_data_sent, new_last_object_id, num_sent


def plugin_shutdown(data):
    pass

# TODO: North plugin can not be reconfigured? (per callback mechanism)


def plugin_reconfigure():
    pass


class S7NorthPlugin(object):
    """ North S7 Plugin """

    def __init__(self):
        self.event_loop = asyncio.get_event_loop()

    def s7_error(self, error):
        _LOGGER.error(f'S7 error: {error}')

    async def send_payloads(self, payloads):
        is_data_sent = False
        last_object_id = 0
        num_sent = 0

        size_payload_block = 0

        map = json.loads(config['map']['value'])

        try:
            _LOGGER.info('processing payloads')
            payload_block = list()

            for p in payloads:
                last_object_id = p["id"]

                if p['asset_code'] in map:
                    for datapoint, item in map[p['asset_code']].items():
                        if not (item.get('index') is None) and not (item.get('DB') is None) and not (item.get('type') is None):
                            matches = re.search(
                                r"^static-\d+", datapoint, re.IGNORECASE)
                            if datapoint in p['reading'] or (bool_(config["supportStaticValues"]["value"]) is True and matches):
                                read = dict()

                                read["asset"] = p['asset_code']
                                read["datapoint"] = datapoint

                                if bool_(config["supportStaticValues"]["value"]) is True and matches:
                                    if not (item.get('value') is None):
                                        read["value"] = item.get('value')
                                    else:
                                        _LOGGER.error(
                                            "JSON is not valid - the JSON key: value is missing")
                                else:
                                    read["value"] = p['reading'][datapoint]

                                read["type"] = item.get('type')
                                read["dbnumber"] = int(item.get('DB'))
                                index_split = str(item.get('index')).split('.')
                                read["byte_index"] = int(index_split[0])
                                bool_index = 0
                                if len(index_split) == 2:
                                    bool_index = int(index_split[1])
                                read["bool_index"] = bool_index
                                read["timestamp"] = p['user_ts']

                                await self._send_payload(read)
                        else:
                            _LOGGER.error(
                                "JSON is not valid - one of the following keys is missing: index, DB, type")
                num_sent += 1
            _LOGGER.info(f'payloads sent: {num_sent}')
            is_data_sent = True
        except Exception as ex:
            _LOGGER.exception("Data could not be sent, %s", str(ex))

        return is_data_sent, last_object_id, num_sent

    async def _send_payload(self, payload):
        """ send payloads"""
        num_count = 0
        try:
            host = config['host']['value']
            port = int(config['port']['value'])
            rack = int(config['rack']['value'])
            slot = int(config['slot']['value'])
        except Exception as ex:
            e_msg = 'Failed to parse S7 TCP host address and / or port configuration.'
            _LOGGER.error('%s %s', e_msg, str(ex))
            raise ValueError(e_msg)
        try:
            client = snap7.client.Client()
            client.connect(host, rack, slot, port)
            client_connected = client.get_connected()
            if client_connected:
                _LOGGER.info('S7 TCP Client is connected. %s:%d', host, port)
            else:
                raise RuntimeError("S7 TCP Connection failed!")
        except:
            _LOGGER.warn(
                'Failed to connect! S7 TCP host %s on port %d, rack %d and slot %d ', host, port, rack, slot)
            raise RuntimeError("S7 TCP Connection failed!")
            return

        try:
            bytearray_size = get_type_size(payload["type"])

            _LOGGER.debug("supportBool: %s type: %s", str(
                bool_(config["supportBool"]["value"])), str(type(bool_(config["supportBool"]["value"]))))

            if payload["type"].strip().lower() == 'bool' and bool_(config["supportBool"]["value"]) is True:
                _LOGGER.debug("BoolIfTrue")
                buffer = client.read_area(
                    snap7.types.Areas.DB, payload["dbnumber"], payload["byte_index"], bytearray_size)
                _LOGGER.warn("First read Bool Byte! Buffer: ", str(buffer))
            else:
                buffer = bytearray(bytearray_size)

            buffer = set_value(
                buffer, 0, payload["bool_index"], payload["value"], payload["type"])

            if buffer is None:
                _LOGGER.debug("Buffer is None! Asset: %s, Datapoint: %s, DB: %d, Index: %d.%d, Type: %s, Value: %s",
                              payload["asset"], payload["datapoint"], payload["dbnumber"],  payload["byte_index"], payload["bool_index"], payload["type"], str(payload["value"]))
                return

            _LOGGER.debug("Asset: %s, Datapoint: %s, DB: %d, Index: %d.%d, Type: %s, Buffer: %s",
                          payload["asset"], payload["datapoint"], payload["dbnumber"],  payload["byte_index"], payload["bool_index"], payload["type"], str(buffer))
            client.write_area(snap7.types.Areas.DB,
                              payload["dbnumber"], payload["byte_index"], buffer)

            _LOGGER.debug("verify: %s type: %s", str(
                bool_(config["verify"]["value"])), str(type(bool_(config["verify"]["value"]))))
            if bool_(config["verify"]["value"]) is True:
                read_buffer = client.read_area(
                    snap7.types.Areas.DB, payload["dbnumber"], payload["byte_index"], bytearray_size)
                _LOGGER.debug("Write Buffer: %s, Read Buffer: %s",
                              str(buffer), str(read_buffer))
                if buffer != read_buffer:
                    _LOGGER.error(
                        'Verification failed! Failed to write data to S7 TCP host %s on port %d, rack %d and slot %d ', host, port, rack, slot)
                    raise RuntimeError("S7 data writing failed!")

        except Exception as ex:
            #client.disconnect()
            _LOGGER.exception(f'Exception sending payloads: {ex}')
            raise ex


def set_value(bytearray_, byte_index, bool_index, value, type_):
    """ Sets the value for a specific type.
    Args:
        byte_index: byte index from where start reading.
        type_: type of data to write.
    Raises:
        :obj:`ValueError`: if the `type_` is not handled.
    Returns:
        Value read according to the `type_`
    """

    type_ = type_.strip().lower()

    if type_ == 'bool' and bool_(config["supportBool"]["value"]) is True:
        return set_bool_(bytearray_, byte_index, bool_index, bool_(value))

    elif type_.startswith('string'):
        max_size = re.search(r'\d+', type_)[0]
        # (\d+\.\.)?(\d+)    0..9
        # if max_size is None:
        #     max_size = 255
        _LOGGER.debug("string max_size %d", max_size)
        return set_string_(bytearray_, byte_index, str(value), int(max_size))

    elif type_ == 'real':
        #return value_to_type(set_real, bytearray_, byte_index, value)
        return set_real(bytearray_, byte_index, value)

    # elif type_ == 'lreal':
    #     return set_lreal(bytearray_, byte_index)

    elif type_ == 'word':
        #return value_to_type(set_word, bytearray_, byte_index, value)
        return set_word(bytearray_, byte_index, value)

    elif type_ == 'dword':
        #return value_to_type(set_dword, bytearray_, byte_index, value)
        return set_dword_(bytearray_, byte_index, value)

    # elif type_ == 'lword':
    #     return set_lword(bytearray_, byte_index)

    # elif type_ == 'sint':
    #     return set_sint(bytearray_, byte_index)

    elif type_ == 'int':
        #return value_to_type(set_int, bytearray_, byte_index, value)
        return set_int(bytearray_, byte_index, value)

    elif type_ == 'dint':
        #return value_to_type(set_dint bytearray_, byte_index, value)
        return set_dint_(bytearray_, byte_index, value)

    # elif type_ == 'lint':
    #     return set_lint(bytearray_, byte_index)
    #
    # elif type_ == 'usint':
    #     return set_usint(bytearray_, byte_index)
    #
    # elif type_ == 'uint':
    #     return set_uint(bytearray_, byte_index)
    #
    # elif type_ == 'udint':
    #     return set_udint(bytearray_, byte_index)
    #
    # elif type_ == 'ulint':
    #     return set_ulint(bytearray_, byte_index)

    elif type_ == 'byte':
        #return value_to_type(set_byte_, bytearray_, byte_index, value)
        return set_byte(bytearray_, byte_index, value)

    # elif type_ == 'char':
    #     return chr(set_usint(bytearray_, byte_index))
    #
    # elif type_ == 's5time':
    #     data_s5time = set_s5time(bytearray_, byte_index)
    #     return data_s5time
    #
    # elif type_ == 'date_and_time':
    #     data_dt = set_dt(bytearray_, byte_index)
    #     return data_dt

    # add these three not implemented data typ to avoid error
    # elif type_ == 'time':
    #     _LOGGER.warn("Data Type 'Time' not implemented")
    #     return None
    #
    # elif type_ == 'date':
    #     _LOGGER.warn("Data Type 'Date' not implemented")
    #     return None
    #
    # elif type_ == 'time_of_day':
    #     _LOGGER.warn("Data Type 'Time of Day' not implemented")
    #     return None

    _LOGGER.warn('Unknown Data Type %s not implemented', str(type_))
    return None


# def value_to_type(ptype, bytearray_, byte_index, value):
#     #FIXME
#     # if isinstance(value, (list, tuple)
#     if isinstance(value, list):
#         value = [ptype(i) for i in value]
#     else:
#         value = ptype(value)
#
#     return value


def get_type_size(type_name):

    type_name = type_name.strip().lower()

    type_size = {"bool": 1, "byte": 1, "char": 1, "word": 2, "dword": 4, "usint": 1,  "uint": 2, "udint": 4,
                 "ulint": 8, "sint": 1, "int": 2, "dint": 4, "lint": 8,  "real": 4, "lreal": 8, "string": 256, "date_and_time": 8}

    if type_name in type_size.keys():
        return type_size[type_name]

    type_split = type_name.split('[')
    if len(type_split) == 2 and "]" == type_name[-1]:
        array_size = int(type_split[1][:-1])  # +1 because array start with 0

        if type_split[0] == 'string':
            return array_size + 2

        if type_split[0] in type_size.keys():
            return type_size[type_split[0]] * array_size

    if type_split[0] == 'string' and len(type_split) == 3 and "]" == type_name[-1]:
        # +1 because array start with 0
        string_size = int(type_split[1][:-1]) + 2
        array_size = int(type_split[2][:-1])  # +1 because array start with 0
        return array_size * string_size

    raise ValueError


def set_bool_(bytearray_: bytearray, byte_index: int, bool_index: int, value: bool):
    """Set boolean value on location in bytearray.
    Args:
        bytearray_: buffer to write to.
        byte_index: byte index to write to.
        bool_index: bit index to write to.
        value: value to write.
    Examples:
        >>> buffer = bytearray([0b00000000])
        >>> set_bool(buffer, 0, 0, True)
        >>> buffer
            bytearray(b"\\x01")
    """
    if value not in {0, 1, True, False}:
        raise TypeError(f"Value value:{value} is not a boolean expression.")

    current_value = get_bool(bytearray_, byte_index, bool_index)
    index_value = 1 << bool_index

    # check if bool already has correct value
    if current_value == value:
        return

    if value:
        # make sure index_v is IN current byte
        bytearray_[byte_index] += index_value
    else:
        # make sure index_v is NOT in current byte
        bytearray_[byte_index] -= index_value

    return bytearray_


def set_string_(bytearray_: bytearray, byte_index: int, value: str, max_size: int):
    """Set string value
    Args:
        bytearray_: buffer to write to.
        byte_index: byte index to start writing from.
        value: string to write.
        max_size: maximum possible string size.
    Raises:
        :obj:`TypeError`: if the `value` is not a :obj:`str`.
        :obj:`ValueError`: if the length of the  `value` is larger than the `max_size`.
    Examples:
        >>> data = bytearray(20)
        >>> snap7.util.set_string(data, 0, "hello world", 255)
        >>> data
            bytearray(b'\\x00\\x0bhello world\\x00\\x00\\x00\\x00\\x00\\x00\\x00')
    """
    if not isinstance(value, str):
        raise TypeError(f"Value value:{value} is not from Type string")

    size = len(value)
    # FAIL HARD WHEN trying to write too much data into PLC
    if size > max_size:
        raise ValueError(f'size {size} > max_size {max_size} {value}')
    # set len count on first position
    bytearray_[byte_index + 1] = len(value)

    i = 0
    # fill array which chr integers
    for i, c in enumerate(value):
        bytearray_[byte_index + 2 + i] = ord(c)

    # fill the rest with empty space
    for r in range(i + 1, bytearray_[byte_index]):
        bytearray_[byte_index + 2 + r] = ord(' ')

    return bytearray_


def set_dword_(bytearray_: bytearray, byte_index: int, dword: int):
    """Set a DWORD to the buffer.
    Notes:
        Datatype `dword` consists in 8 bytes in the PLC.
        The maximum value posible is `4294967295`
    Args:
        bytearray_: buffer to write to.
        byte_index: byte index from where to writing reading.
        dword: value to write.
    Examples:
        >>> data = bytearray(4)
        >>> snap7.util.set_dword(data,0, 4294967295)
        >>> data
            bytearray(b'\\xff\\xff\\xff\\xff')
    """
    dword = int(dword)
    _bytes = struct.unpack('4B', struct.pack('>I', dword))
    for i, b in enumerate(_bytes):
        bytearray_[byte_index + i] = b

    return bytearray_


def set_dint_(bytearray_: bytearray, byte_index: int, dint: int):
    """Set value in bytearray to dint
    Notes:
        Datatype `dint` consists in 4 bytes in the PLC.
        Maximum possible value is 2147483647.
        Lower posible value is -2147483648.
    Args:
        bytearray_: buffer to write.
        byte_index: byte index from where to start writing.
    Examples:
        >>> data = bytearray(4)
        >>> snap7.util.set_dint(data, 0, 2147483647)
        >>> data
            bytearray(b'\\x7f\\xff\\xff\\xff')
    """
    dint = int(dint)
    _bytes = struct.unpack('4B', struct.pack('>i', dint))
    for i, b in enumerate(_bytes):
        bytearray_[byte_index + i] = b

    return bytearray_


def bool_(value):
    if value in (True, "True", "true", 1, "1"):
        return True
    if value in (False, "False", "false", 0, "0"):
        return False
    else:
        return bool(value)
