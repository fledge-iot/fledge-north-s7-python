""" Unit tests for the S7 plugin """


__author__ = "Thorsten Steuer"
__copyright__ = "ACDP"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


import sys
import pytest
import json
import logging

from unittest.mock import patch, MagicMock, ANY
from python.fledge.plugins.north.s7_python import s7_python as s7


import asyncio
import time
import ast
import aiohttp
#from fledge.tasks.north.sending_process import SendingProcess
#import fledge.tasks.north.sending_process as module_sp
from fledge.common.storage_client import payload_builder
from fledge.common.storage_client.storage_client import StorageClientAsync

_STREAM_ID = 1


async def mock_async_call(p1=ANY):
    """Mocks a generic async function

    Parameters
    ----------
    p1 : type
        Description of parameter `p1`.

    Returns
    -------
    def
        Description of returned object.

    """
    return p1


@pytest.fixture
def fixture_s7(event_loop):
    """Short summary.

    Parameters
    ----------
    event_loop : type
        Description of parameter `event_loop`.

    Returns
    -------
    type
        Description of returned object.

    """

    _omf = MagicMock()

    s7._logger = MagicMock(spec=logging)

    # fixme
    s7._config_omf_types = {"type-id": {"value": "0001"}}

    return s7


@pytest.fixture
def fixture_s7_north(event_loop):
    """Short summary.

    Parameters
    ----------
    event_loop : type
        Description of parameter `event_loop`.

    Returns
    -------
    type
        Description of returned object.

    """

    sending_process_instance = MagicMock()

    _logger = MagicMock(spec=logging)

    s7_north = s7.S7NorthPlugin()

    # fixme: check if required
    #s7_north._sending_process_instance._storage_async = MagicMock(spec=StorageClientAsync)

    return s7_north


@pytest.mark.usefixtures('fixture_s7_north')
class TestS7:
    """Short summary."""

    def test_plugin_info(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """

        plugin_info = s7.plugin_info()

        assert plugin_info == {
            'name': 's7_north_python',
            'version': '3.0.0',
            'type': 'north',
            'interface': '1.0',
            'config': s7._DEFAULT_CONFIG
        }

    def test_plugin_init_good(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        data = s7._DEFAULT_CONFIG
        data['host']['value'] = '127.0.0.1'
        data['port']['value'] = data['port']['default']
        data['rack']['value'] = data['rack']['default']
        data['slot']['value'] = data['slot']['default']
        data['map']['value'] = data['map']['default']

        s7._logger = MagicMock()

        assert data == s7.plugin_init(data)

    @pytest.mark.parametrize(
        "send_payloads_data, "
        "payload_raw_data",
        [
            (
                # send_payloads_data
                # is_data_available - new_last_object_id - num_sent
                [True,                20,            10],
                # raw_data
                [
                    {
                        "id": 10,
                        "asset_code": "test_asset_code",
                        "reading": {"humidity": 100, "temperature": 1001},
                        "user_ts": '2018-04-20 09:38:50.163164+00'
                    }
                ]
             )
        ]
    )
    @pytest.mark.asyncio
    async def test_plugin_send(self,
                               send_payloads_data,
                               payload_raw_data,
                               monkeypatch
                               ):
        data = MagicMock()

        config = s7._DEFAULT_CONFIG
        config['host']['value'] = '127.0.0.1'
        config['port']['value'] = config['port']['default']
        config['rack']['value'] = config['rack']['default']
        config['slot']['value'] = config['slot']['default']
        config['map']['value'] = config['map']['default']

        monkeypatch.setattr(s7, "config", config)
        is_data_sent, last_object_id, num_sent = await s7.plugin_send(data, payload_raw_data, _STREAM_ID)

        assert [True, 10, 1] == [is_data_sent, last_object_id, num_sent]
#
#         # Used to check the conversions
#         data = {
#                 "stream_id": {"value": 1},
#
#                 "_CONFIG_CATEGORY_NAME":  module_sp.SendingProcess._CONFIG_CATEGORY_NAME,
#                 "URL": {"value": "test_URL"},
#                 "producerToken": {"value": "test_producerToken"},
#                 "OMFMaxRetry": {"value": "100"},
#                 "OMFRetrySleepTime": {"value": "100"},
#                 "OMFHttpTimeout": {"value": "100"},
#                 "StaticData": {
#                     "value": json.dumps(
#                         {
#                             "Location": "Palo Alto",
#                             "Company": "Dianomic"
#                         }
#                     )
#                 },
#                 "destination_type": {"value": "3"},
#                 'sending_process_instance': MagicMock(spec=SendingProcess),
#                 "formatNumber": {"value": "float64"},
#                 "formatInteger": {"value": "int64"},
#                 "notBlockingErrors": {"value": "{'id': 400, 'message': 'none'}"},
#                 "compression": {"value": "true"},
#                 "namespace": {"value": "ocs_namespace_0001"},
#                 "tenant_id": {"value": "ocs_tenant_id"},
#                 "client_id": {"value": "ocs_client_id"},
#                 "client_secret": {"value": "ocs_client_secret"},
#
#         }
#
#         config_default_omf_types = s7._CONFIG_DEFAULT_OMF_TYPES
#         config_default_omf_types["type-id"]["value"] = "0001"
#         data["debug_level"] = None
#         data["log_performance"] = None
#         data["destination_id"] = 1
#         data["stream_id"] = 1
#
#         with patch.object(data['sending_process_instance'], '_fetch_configuration',
#                           return_value=config_default_omf_types):
#             config = ocs.plugin_init(data)
#
#         assert config['_CONFIG_CATEGORY_NAME'] == module_sp.SendingProcess._CONFIG_CATEGORY_NAME
#         assert config['URL'] == "test_URL"
#         assert config['producerToken'] == "test_producerToken"
#         assert config['OMFMaxRetry'] == 100
#         assert config['OMFRetrySleepTime'] == 100
#         assert config['OMFHttpTimeout'] == 100
#
#         # Check conversion from String to Dict
#         assert isinstance(config['StaticData'], dict)
#
#     @pytest.mark.parametrize("data", [
#             # Bad case 1 - StaticData is a python dict instead of a string containing a dict
#             {
#                 "stream_id": {"value": 1},
#                 "_CONFIG_CATEGORY_NAME":  module_sp.SendingProcess._CONFIG_CATEGORY_NAME,
#                 "URL": {"value": "test_URL"},
#                 "producerToken": {"value": "test_producerToken"},
#                 "OMFMaxRetry": {"value": "100"},
#                 "OMFRetrySleepTime": {"value": "100"},
#                 "OMFHttpTimeout": {"value": "100"},
#                 "StaticData": {
#                     "value":
#                         {
#                             "Location": "Palo Alto",
#                             "Company": "Dianomic"
#                         }
#                 },
#                 'sending_process_instance': MagicMock(spec=SendingProcess),
#                 "formatNumber": {"value": "float64"},
#                 "formatInteger": {"value": "int64"},
#             },
#             # Bad case 2 - OMFMaxRetry, bad value expected an int it is a string
#             {
#                 "stream_id": {"value": 1},
#                 "_CONFIG_CATEGORY_NAME": module_sp.SendingProcess._CONFIG_CATEGORY_NAME,
#                 "URL": {"value": "test_URL"},
#                 "producerToken": {"value": "test_producerToken"},
#                 "OMFMaxRetry": {"value": "xxx"},
#                 "OMFRetrySleepTime": {"value": "100"},
#                 "OMFHttpTimeout": {"value": "100"},
#                 "StaticData": {
#                     "value": json.dumps(
#                         {
#                             "Location": "Palo Alto",
#                             "Company": "Dianomic"
#                         }
#                     )
#                 },
#                 'sending_process_instance': MagicMock(spec=SendingProcess),
#                 "formatNumber": {"value": "float64"},
#                 "formatInteger": {"value": "int64"},
#             },
#             # Bad case 3- formatNumber not defined
#             {
#                 "stream_id": {"value": 1},
#
#                 "_CONFIG_CATEGORY_NAME": module_sp.SendingProcess._CONFIG_CATEGORY_NAME,
#                 "URL": {"value": "test_URL"},
#                 "producerToken": {"value": "test_producerToken"},
#                 "OMFMaxRetry": {"value": "100"},
#                 "OMFRetrySleepTime": {"value": "100"},
#                 "OMFHttpTimeout": {"value": "100"},
#                 "StaticData": {
#                     "value": json.dumps(
#                         {
#                             "Location": "Palo Alto",
#                             "Company": "Dianomic"
#                         }
#                     )
#                 },
#
#                 'sending_process_instance': MagicMock(spec=SendingProcess),
#
#                 "formatInteger": {"value": "int64"}
#             },
#
#             # Bad case 4 - formatInteger not defined
#             {
#                 "stream_id": {"value": 1},
#
#                 "_CONFIG_CATEGORY_NAME": module_sp.SendingProcess._CONFIG_CATEGORY_NAME,
#                 "URL": {"value": "test_URL"},
#                 "producerToken": {"value": "test_producerToken"},
#                 "OMFMaxRetry": {"value": "100"},
#                 "OMFRetrySleepTime": {"value": "100"},
#                 "OMFHttpTimeout": {"value": "100"},
#                 "StaticData": {
#                     "value": json.dumps(
#                         {
#                             "Location": "Palo Alto",
#                             "Company": "Dianomic"
#                         }
#                     )
#                 },
#
#                 'sending_process_instance': MagicMock(spec=SendingProcess),
#
#                 "formatNumber": {"value": "float64"}
#             }
#     ])
#     def test_plugin_init_bad(self, data):
#         """Tests plugin_init using an invalid set of values"""
#
#         ocs._logger = MagicMock()
#
#         with pytest.raises(Exception):
#             ocs.plugin_init(data)
#
#     @pytest.mark.parametrize(
#         "ret_transform_in_memory_data, "
#         "p_raw_data, ",
#         [
#             (
#                 # ret_transform_in_memory_data
#                 # is_data_available - new_position - num_sent
#                 [True,                20,            10],
#                 # raw_data
#                 [
#                     {
#                         "id": 10,
#                         "asset_code": "test_asset_code",
#                         "reading": {"humidity": 100, "temperature": 1001},
#                         "user_ts": '2018-04-20 09:38:50.163164+00'
#                     }
#                 ]
#              )
#         ]
#     )
#     @pytest.mark.asyncio
#     async def test_plugin_send_success(self,
#                                        ret_transform_in_memory_data,
#                                        p_raw_data,
#                                        fixture_ocs
#                                        ):
#
#         data = MagicMock()
#
#         with patch.object(fixture_ocs.OCSNorthPlugin,
#                           'transform_in_memory_data',
#                           return_value=ret_transform_in_memory_data) as patched_transform_in_memory_data:
#             with patch.object(fixture_ocs.OCSNorthPlugin,
#                               'create_omf_objects',
#                               return_value=mock_async_call()) as patched_create_omf_objects:
#                 with patch.object(fixture_ocs.OCSNorthPlugin,
#                                   'send_in_memory_data_to_picromf',
#                                   return_value=mock_async_call()) as patched_send_in_memory_data_to_picromf:
#                     await fixture_ocs.plugin_send(data, p_raw_data, _STREAM_ID)
#
#         assert patched_transform_in_memory_data.called
#         assert patched_create_omf_objects.called
#         assert patched_send_in_memory_data_to_picromf.called
#
#     @pytest.mark.parametrize(
#         "ret_transform_in_memory_data, "
#         "p_raw_data, ",
#         [
#             (
#                 # ret_transform_in_memory_data
#                 # is_data_available - new_position - num_sent
#                 [True,                20,            10],
#                 # raw_data
#                 {
#                     "id": 10,
#                     "asset_code": "test_asset_code",
#                     "reading": {"humidity": 100, "temperature": 1001},
#                     "user_ts": '2018-04-20 09:38:50.163164+00'
#                 }
#              )
#         ]
#     )
#     @pytest.mark.asyncio
#     async def test_plugin_send_error(
#                                     self,
#                                     fixture_ocs,
#                                     ret_transform_in_memory_data,
#                                     p_raw_data
#                                      ):
#         """ Unit test for - plugin_send - error handling case
#            it tests especially if the ocs objects are created again in case of a communication error
#
#            NOTE : the stderr is redirected to avoid the print of an error message that could be ignored.
#         """
#
#         data = MagicMock()
#
#         with patch.object(fixture_ocs.OCSNorthPlugin,
#                           'transform_in_memory_data',
#                           return_value=ret_transform_in_memory_data
#                           ) as patched_transform_in_memory_data:
#
#             with patch.object(fixture_ocs.OCSNorthPlugin,
#                               'create_omf_objects',
#                               return_value=mock_async_call()
#                               ) as patched_create_omf_objects:
#
#                 with patch.object(fixture_ocs.OCSNorthPlugin,
#                                   'send_in_memory_data_to_picromf',
#                                   side_effect=KeyError('mocked object generated an exception')
#                                   ) as patched_send_in_memory_data_to_picromf:
#
#                     with patch.object(fixture_ocs.OCSNorthPlugin,
#                                       'deleted_omf_types_already_created',
#                                       return_value=mock_async_call()
#                                       ) as patched_deleted_omf_types_already_created:
#
#                         with pytest.raises(Exception):
#                             await fixture_ocs.plugin_send(data, p_raw_data,
#                                                                                               _STREAM_ID)
#         assert patched_transform_in_memory_data.called
#         assert patched_create_omf_objects.called
#         assert patched_send_in_memory_data_to_picromf.called
#         assert patched_deleted_omf_types_already_created.called
#
#     def test_plugin_shutdown(self):
#
#         ocs._logger = MagicMock()
#         data = []
#         ocs.plugin_shutdown([data])
#
#     def test_plugin_reconfigure(self):
#
#         ocs._logger = MagicMock()
#         ocs.plugin_reconfigure()
#
#
# class TestOCSNorthPlugin:
#     """Unit tests related to OCSNorthPlugin, methods used internally to the plugin"""
#
#     @pytest.mark.parametrize(
#         "p_test_data, "
#         "p_type_id, "
#         "p_static_data, "
#         "expected_typename,"
#         "expected_omf_type",
#         [
#             # Case 1 - pressure / Number
#             (
#                 # Origin - Sensor data
#                 {"asset_code": "pressure", "asset_data": {"pressure": 921.6}},
#                 # type_id
#                 "0001",
#                 # Static Data
#                 {
#                     "Location": "Palo Alto",
#                     "Company": "Dianomic"
#                 },
#                 # Expected
#                 'pressure_typename',
#                 {
#                     'pressure_typename':
#                     [
#                         {
#                             'classification': 'static',
#                             'id': '0001_pressure_typename_sensor',
#                             'properties': {
#                                             'Company': {'type': 'string'},
#                                             'Name': {'isindex': True, 'type': 'string'},
#                                             'Location': {'type': 'string'}
#                             },
#                             'type': 'object'
#                         },
#                         {
#                             'classification': 'dynamic',
#                             'id': '0001_pressure_typename_measurement',
#                             'properties': {
#                                 'Time': {
#                                         'isindex': True,
#                                         'format': 'date-time',
#                                         'type': 'string'
#                                 },
#                                 'pressure': {
#                                         'type': 'number',
#                                         'format': 'float64'
#                                 }
#                             },
#                             'type': 'object'
#                          }
#                     ]
#                 }
#             ),
#             # Case 2 - luxometer / Integer
#             (
#                     # Origin - Sensor data
#                     {"asset_code": "luxometer", "asset_data": {"lux": 20}},
#                     # type_id
#                     "0002",
#                     # Static Data
#                     {
#                         "Location": "Palo Alto",
#                         "Company": "Dianomic"
#                     },
#                     # Expected
#                     'luxometer_typename',
#                     {
#                         'luxometer_typename':
#                             [
#                                 {
#                                     'classification': 'static',
#                                     'id': '0002_luxometer_typename_sensor',
#                                     'properties': {
#                                         'Company': {'type': 'string'},
#                                         'Name': {'isindex': True, 'type': 'string'},
#                                         'Location': {'type': 'string'}
#                                     },
#                                     'type': 'object'
#                                 },
#                                 {
#                                     'classification': 'dynamic',
#                                     'id': '0002_luxometer_typename_measurement',
#                                     'properties': {
#                                         'Time': {'isindex': True, 'format': 'date-time', 'type': 'string'},
#                                         'lux': {
#                                                 'type': 'number',
#                                                 'format': 'float64'
#                                         }
#                                     },
#                                     'type': 'object'
#                                 }
#                             ]
#                     }
#             )
#         ]
#     )
#     @pytest.mark.asyncio
#     async def test_create_omf_type_automatic(
#                                         self,
#                                         p_test_data,
#                                         p_type_id,
#                                         p_static_data,
#                                         expected_typename,
#                                         expected_omf_type,
#                                         fixture_ocs_north):
#         """ Unit test for - _create_omf_type_automatic - successful case
#             Tests the generation of the OMF messages starting from Asset name and data
#             using Automatic OMF Type Mapping"""
#
#         fixture_ocs_north._config_omf_types = {"type-id": {"value": p_type_id}}
#
#         fixture_ocs_north._config = {}
#         fixture_ocs_north._config["StaticData"] = p_static_data
#         fixture_ocs_north._config["formatNumber"] = "float64"
#         fixture_ocs_north._config["formatInteger"] = "int64"
#
#         with patch.object(fixture_ocs_north,
#                           'send_in_memory_data_to_picromf',
#                           return_value=mock_async_call()
#                           ) as patched_send_in_memory_data_to_picromf:
#
#             typename, omf_type = await fixture_ocs_north._create_omf_type_automatic(p_test_data)
#
#         assert typename == expected_typename
#         assert omf_type == expected_omf_type
#
#         assert patched_send_in_memory_data_to_picromf.called
#         patched_send_in_memory_data_to_picromf.assert_any_call("Type", expected_omf_type[expected_typename])
