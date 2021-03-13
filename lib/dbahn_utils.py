import os
import re
from collections import defaultdict
from pathlib import Path
from tokenize import String
from typing import List, Dict


class BahnUtils:

    def __init__(self, path):
        assert os.path.exists(path)
        self.path = path

    def get_path(self, folder : Path):
        return os.path.expanduser(self.path / f'paket_1_1_{folder}_push_nachrichten' / f'{folder}_push_nachrichten.csv')

    def read_data_folder(self, folder : Path):
        """
        Reads a data folder and outputs for this data_folder the header of the csv, the notification infomation on id level,
        and the results on channel_address type
        :param folder:
        :type folder:
        :return:
        :rtype:
        """
        assert isinstance(self.path, Path)
        i = 0
        header = None
        data_uuids_set = defaultdict(lambda: defaultdict(int))
        filter_by_type = defaultdict(int)
        _fpath = self.get_path(folder)
        with open(_fpath) as f:
            for line in f:
                i += 1
                if i == 1:
                    header = line.split(';')
                    continue
                app_uuid_index = header.index('app_uuid')
                channel_address_index = header.index('channel_address')

                data_split = line.split(';')
                date_str = data_split[0][:10]
                data_uuids_set[data_split[app_uuid_index]]['notifications'] += 1
                data_uuids_set[data_split[app_uuid_index]][date_str] += 1
                if 'type' not in data_uuids_set[data_split[2]]:
                    data_uuids_set[data_split[app_uuid_index]]['type'] = defaultdict(int)
                data_uuids_set[data_split[app_uuid_index]]['type'][data_split[channel_address_index]] += 1


                filter_by_type[data_split[header.index('channel_address')]] += 1
        return header, data_uuids_set, filter_by_type



    # Handle the filtering
    def get_additional_parameters(self, rt_connection_field : str):
        """
        Reads the RtStop from the connectionstring
        :param rt_connection_field:
        :type rt_connection_field:
        :return:
        :rtype:
        """
        rt_connection_arrivalstop = re.search('ArrivalStop=RtStop{([^}]*)', rt_connection_field).group(1)

        # we split the resulting regex group up. We have now comma separated strings. Most of them have a = in them
        # specifying an individual key=val pair. Some are just string
        data_splits = rt_connection_arrivalstop.split(',')
        details = dict()
        details['string'] = []
        for d in data_splits:
            if '=' in d:
                _s = d.split('=')
                details[_s[0]] = _s[1]
            else:
                print(f'{d} does not contain a "="')
        for k,v in details.items():
            details[k] = ','.join(details[k])
        return details


    def filter_by_id(self, _fpath : Path, ids_to_filter : List[String], filter_channels : List[String], additional_detail_fields : List[String]) -> Dict:
        """
        function filters based on app_uuid and filter_channels. It appends the 'additional_detail_fields'

        :param _fpath: filepath to read
        :param ids_to_filter: list of ids as string to
        :param filter_channels: list of channels
        :param additional_detail_fields: list of additional_detail_fields
        :return:
        :rtype:
        """
        lines_by_id = defaultdict(list)
        i = 0
        header = None
        with open(_fpath) as f:
            for line in f:
                i+=1
                if i == 1:
                    for id in ids_to_filter:
                        lines_by_id[id].append(line + ';' + ';'.join(additional_detail_fields))
                        header = line.split(';')
                    continue

                line = line.strip()

                data_split = line.split(';')
                if data_split[header.index('app_uuid')] not in ids_to_filter:
                    continue
                if data_split[header.index('channel_address')] not in filter_channels:
                    continue

                # read the rtconnectionevent fields to some degree
                rt_connection = data_split[header.index('RtConnectionEvent')]
                details = self.get_additional_parameters(rt_connection)

                # make them csv exportable
                v = []
                for add_field in additional_detail_fields:
                    # escape potential semicolons
                    details[add_field].replace(';', '%%%')
                    v.append(details.get(add_field, ''))

                new_line = line + ';' + ';'.join(v) + '\n'
                lines_by_id[data_split[header.index('app_uuid')]].append(new_line)

        return lines_by_id