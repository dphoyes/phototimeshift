#!/usr/bin/env python3

import sys
import os
import datetime
import json


class MplFileException(Exception):
    pass


class MtsInfo:
    def __init__(self, seek_pos, info):
        self.seek_pos = seek_pos
        self.__info = info
        self.dirty = False

    def __getitem__(self, key):
        return self.__info[key]

    def __setitem__(self, key, value):
        self.__info[key] = value
        self.dirty = True


class MplFile:
    SIGNATURE = b"MPLS0100"

    def __init__(self, filepath):
        self.filepath = filepath
        self.timezone_filepath = filepath + ".timezone.json"
        self.db = {}
        self.tz_dict = {}
        self.read()

    def get_mts(self, mts_name):
        return self.db[mts_name]

    def read(self):
        try:
            with open(self.timezone_filepath, "r") as f:
                self.tz_dict = json.load(f)
        except FileNotFoundError:
            pass

        with open(self.filepath, "rb") as f:

            if f.read(len(self.SIGNATURE)) != self.SIGNATURE:
                raise MplFileException("Could not read file signature. Wrong filetype?")

            # find out how many mts files are described
            # the 66th byte contains this number
            f.seek(66 - len(self.SIGNATURE) - 1, os.SEEK_CUR)
            ret = f.read(1)
            if len(ret) != 1:
                raise MplFileException("Could not read contents")
            num_desc = ord(ret)

            # jump to the first occurance of a time stamp and print it
            # iterate till all num_desc time stamps are shown
            # trailer = 50 bytes
            # mts description = 66 bytes
            # actual info starts at 9th byte of mts description
            f.seek(-50 - 66*num_desc - 36 + 2, os.SEEK_END)

            while num_desc > 0:
                time_stamp_sig = b'\x01\x03\x05\x01\x00\x00\x00\x02'

                num_desc -= 1
                f.seek(36, os.SEEK_CUR)

                # scan for time stamp signature
                assert f.read(len(time_stamp_sig)) == time_stamp_sig

                timestamp_seek_pos = f.tell()

                # scan time stamp
                mts_filename = "{:05d}.MTS".format(int.from_bytes(f.read(2), byteorder='big'))
                assert f.read(1) == b'\x1E'
                year = int("{:x}".format(int.from_bytes(f.read(2), byteorder='big')))

                datetime_values = (
                    int("{:x}".format(ord(f.read(1)))) for _ in range(5)
                )

                dt = datetime.datetime(year, *datetime_values)
                tzoffset_seconds = self.tz_dict.get(mts_filename)
                if tzoffset_seconds is not None:
                    dt = dt.replace(tzinfo=datetime.timezone(datetime.timedelta(seconds=tzoffset_seconds)))

                assert f.read(2) in {b'\x90\x0A', b'\x90\x0C'}
                assert f.read(10) == f"{dt.year:4d}.{dt.month:2d}.{dt.day:2d}".encode('ascii')

                self.db[mts_filename] = MtsInfo(seek_pos=timestamp_seek_pos, info={
                    "datetime": dt,
                })

    def write(self):
        if not any(info.dirty for info in self.db.values()):
            return

        with open(self.filepath, "r+b") as f:
            for mts_filename, info in self.db.items():
                if not info.dirty:
                    continue

                f.seek(info.seek_pos)
                dt = info["datetime"]

                assert f.read(2) == int(mts_filename[:-4]).to_bytes(2, byteorder='big')
                assert f.read(1) == b'\x1E'
                f.write(int(f"{dt.year}", 16).to_bytes(2, byteorder='big'))

                for val in (dt.month, dt.day, dt.hour, dt.minute, dt.second):
                    f.write(bytes((int(f"{val}", 16),)))

                assert f.read(2) in {b'\x90\x0A', b'\x90\x0C'}

                f.write(f"{dt.year:4d}.{dt.month:2d}.{dt.day:2d}".encode('ascii'))

                if dt.tzinfo is None:
                    self.tz_dict.pop(mts_filename, None)
                else:
                    self.tz_dict[mts_filename] = dt.tzinfo.utcoffset(None).total_seconds()

                info.dirty = False

        with open(self.timezone_filepath, "w") as f:
            json.dump(self.tz_dict, f)



class MplDirectory:
    def __init__(self, path):
        self.path = path
        self.db = {}
        self.mpl_files = {}

    def get_mts(self, mts_name):
        return self.db[mts_name]

    def print(self):
        for mts_name, info in self.db.items():
            print("{}: {} {}".format(mts_name, info['datetime'], info['datetime'].tzinfo))

    def read(self):
        for root, dirs, files in os.walk(self.path):
            for file in files:
                if file.endswith(".MPL"):
                    filepath = os.path.join(root, file)
                    self.mpl_files[filepath] = mpl = MplFile(filepath)
                    self.db.update(mpl.db)

    def write(self):
        for mpl in self.mpl_files.values():
            mpl.write()


def main():
    mpl = MplDirectory(path=sys.argv[1])
    mpl.read()

    dt = mpl.get_mts("00127.MTS")["datetime"]
    # mpl.get_mts("00127.MTS")["datetime"] = dt.replace(year=2520, month=6, day=2, hour=23, minute=50, second=51, tzinfo=datetime.timezone(datetime.timedelta(hours=3)))

    mpl.print()
    mpl.write()


if __name__ == "__main__":
    main()
