#! /usr/bin/python3

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, GObject, GdkPixbuf
import gbulb
gbulb.install(gtk=True)

import os
import io
import cv2
import sys
import re
import asyncio
import contextlib
import subprocess
import aiofiles
import pyexiv2
import datetime
import pytimeparse
import tempfile
import functools
import sortedcontainers
import mpl_extract

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))

DEFAULT_TIMEZONE_OFFSET = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo.utcoffset(datetime.datetime.now())


def make_null_task():
    async def coro():
        pass
    return asyncio.create_task(coro())


def store_set_pyobject_sort_func(store, column):
    def sort_func(treemodel, it1, it2, user_data):
        val1 = treemodel.get_value(it1, column)
        val2 = treemodel.get_value(it2, column)
        if val1 is None:
            if val2 is None:
                return 0
            else:
                return -1
        elif val2 is None:
            return 1
        return (val1 > val2) - (val1 < val2)

    store.set_sort_func(column, sort_func)


def make_pyobject_column(caption, idx, **kwargs):
    renderer = Gtk.CellRendererText()
    col = Gtk.TreeViewColumn(caption, renderer, **kwargs)
    def set_text(column, cell, model, it, _):
        obj = model.get_value(it, idx)
        if isinstance(obj, datetime.timedelta):
            text = datetime_delta_to_str(obj)
        else:
            text = str(obj)
        cell.set_property('text', text)
    col.set_cell_data_func(renderer, set_text)
    col.set_sort_column_id(idx)
    return col


def datetime_delta_to_str(delta):
    if delta >= datetime.timedelta():
        return str(delta)
    else:
        return '-' + str(-delta)


def str_to_datetime_delta(text):
    text = text.lstrip()
    has_minus_sign = text[0:1] == '-'
    text = (text[1:] if has_minus_sign else text).strip()
    if text == '0':
        return datetime.timedelta()
    seconds = pytimeparse.parse(text)
    if seconds is None:
        raise ValueError("Cannot parse datetime delta")
    if has_minus_sign:
        seconds *= -1
    return datetime.timedelta(seconds=seconds)


def round_datetime_to_second(ts):
    if ts.microsecond >= 500000:
        return ts.replace(microsecond=0) + datetime.timedelta(seconds=1)
    else:
        return ts.replace(microsecond=0)


class AsyncBytesIO:
    def __init__(self, *args):
        self.inner = io.BytesIO(*args)

    async def read(self, *args):
        return await asyncio.get_running_loop().run_in_executor(None, self.inner.read, *args)


class RowSelection:
    def __init__(self, files, selection):
        self.files = files
        self.selection = selection

    def __len__(self):
        return self.selection.count_selected_rows()

    def __iter__(self):
        _, selected = self.selection.get_selected_rows()
        for s in selected:
            yield self.files[s]


EXIF_TIMESTAMP_TAGS = (
    "Exif.Image.DateTime",
    "Exif.Photo.DateTimeOriginal",
    "Exif.Photo.DateTimeDigitized",
    "Exif.Image.DateTimeOriginal",
    "Exif.Image.PreviewDateTime",
)
EXIF_TAGS = (
    "Exif.Image.Model",
    "Exif.Image.TimeZoneOffset",
) + EXIF_TIMESTAMP_TAGS

XMP_TIMESTAMP_TAGS = (
    "Xmp.xmp.CreateDate",
    "Xmp.photoshop.DateCreated",
)
XMP_TAGS = XMP_TIMESTAMP_TAGS

IPTC_TIMESTAMP_TAGS = (
    ("Iptc.Application2.DateCreated", "Iptc.Application2.TimeCreated"),
    ("Iptc.Application2.DigitizationDate", "Iptc.Application2.DigitizationTime"),
)
IPTC_TAGS = IPTC_TIMESTAMP_TAGS

QUICKTIME_TIMESTAMP_TAGS = (
    "Quicktime:CreateDate",
    "Quicktime:ModifyDate",
    "Quicktime:TrackCreateDate",
    "Quicktime:TrackModifyDate",
    "Quicktime:MediaCreateDate",
    "Quicktime:MediaModifyDate",
)
QUICKTIME_TAGS = QUICKTIME_TIMESTAMP_TAGS

NIKON_TAGS = (
    "Nikon:TimeZone",
    "Nikon:DaylightSavings",
)

AVCHD_TAGS = (
    "AVCHD:Timestamp",
)

ALL_TAGS = EXIF_TAGS + XMP_TAGS + IPTC_TAGS + QUICKTIME_TAGS + NIKON_TAGS + AVCHD_TAGS


class FileStore:
    class _Row:
        INDICES = {}
        INDICES["full_path"] = (len(INDICES), GObject.TYPE_STRING)
        INDICES["shortened_path"] = (len(INDICES), GObject.TYPE_STRING)
        INDICES["timestamp"] = (len(INDICES), GObject.TYPE_PYOBJECT)
        INDICES["delta"] = (len(INDICES), GObject.TYPE_PYOBJECT)
        INDICES["mtime"] = (len(INDICES), GObject.TYPE_PYOBJECT)
        INDICES["row-bg-colour"] = (len(INDICES), GObject.TYPE_STRING)
        for t in ALL_TAGS:
            INDICES[t] = (len(INDICES), GObject.TYPE_PYOBJECT)

        def __init__(self, store, it):
            self.store = store
            self.it = it

        def get_value(self, column_index):
            return self.store.get_value(self.it, column_index)

        def set_value(self, column_index, value):
            return self.store.set_value(self.it, column_index, value)

        def __getitem__(self, field):
            i = self.INDICES[field][0]
            return self.get_value(i)

        def __setitem__(self, field, value):
            i = self.INDICES[field][0]
            return self.set_value(i, value)

        def get_exif_aware_timestamp(self, tag_name):
            ts = self[tag_name]
            if ts is None:
                return None
            tz_offset = self["Exif.Image.TimeZoneOffset"]
            return ts.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=tz_offset)))

        def iter_exiv_timestamps(self):
            for tag_name in XMP_TIMESTAMP_TAGS + IPTC_TIMESTAMP_TAGS:
                yield tag_name, self[tag_name]
            for tag_name in EXIF_TIMESTAMP_TAGS:
                yield tag_name, self.get_exif_aware_timestamp(tag_name)

    def __init__(self):
        self.store = Gtk.ListStore(*(t for i, t in self._Row.INDICES.values()))
        self.avchd_dirs = []

        for idx, typ in self._Row.INDICES.values():
            if typ is GObject.TYPE_PYOBJECT:
                store_set_pyobject_sort_func(self.store, idx)

    def Row(self, it):
        return self._Row(self.store, it)

    @classmethod
    def idx(cls, name):
        return cls._Row.INDICES[name][0]

    @staticmethod
    def get_exiv_metadata(f):
        metadata = pyexiv2.ImageMetadata(f)
        try:
            metadata.read()
        except:
            return None
        else:
            return metadata

    async def reload(self, app, selected_rows=None):
        SENTINEL = object()
        loop = asyncio.get_running_loop()

        if selected_rows is None:
            selected_rows = self

        def get_mtime(f):
            return datetime.datetime.fromtimestamp(os.path.getmtime(f), datetime.timezone(DEFAULT_TIMEZONE_OFFSET))

        def get_quicktime_tags(f):
            result = []
            proc = subprocess.run(
                ['exiftool', '-s', file] + ['-'+t for t in QUICKTIME_TAGS + NIKON_TAGS],
                capture_output=True,
            )
            if proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    line = line.decode('utf8')
                    tag_suffix, value = (s.strip() for s in line.split(':', 1))
                    for prefix, group in [
                        ("Quicktime", QUICKTIME_TAGS),
                        ("Nikon", NIKON_TAGS),
                    ]:
                        tag_name = f"{prefix}:{tag_suffix}"
                        if tag_name in group:
                            break
                    else:
                        raise AssertionError(f"Unknown tag: {tag_name}")
                    if tag_name in QUICKTIME_TIMESTAMP_TAGS:
                        value = datetime.datetime.strptime(value, "%Y:%m:%d %H:%M:%S").replace(
                            tzinfo=datetime.timezone(datetime.timedelta())
                        )
                    result.append((tag_name, value))
            return result

        def guess_timezone_offset(row, tag_name):
            for reference_tag in ("Xmp.xmp.CreateDate",):
                reference_dt = row[reference_tag]
                if reference_dt is not None:
                    break
            else:
                return 0
            naive_dt = row[tag_name].replace(tzinfo=datetime.timezone(datetime.timedelta()))
            diff = naive_dt - reference_dt
            tzoffset = guess_timezone_offset.result = round(diff.total_seconds() / 60**2)
            # new_diff_seconds = (naive_dt.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=tzoffset))) - reference_dt).total_seconds()
            # app.notify(
            #     f"Inferred capture timezone of {tzoffset:+d} hours",
            #     f"This synchronizes the timestamp with the {reference_tag} with a difference of {new_diff_seconds} seconds. "
            #     "If this difference is more than a few seconds, the results might be unexpected. "
            #     "If any updates are saved, the new timezone will be written into the tags."
            # )
            return tzoffset

        async def try_other_tag_sources(row, f):
            if f.endswith(".MTS"):
                for avchd_db in self.avchd_dirs:
                    if f.startswith(avchd_db.path):
                        row["AVCHD:Timestamp"] = ts = avchd_db.get_mts(os.path.basename(f))["datetime"]
                        if ts.tzinfo is None:
                            offset = guess_timezone_offset(row, "AVCHD:Timestamp")
                            row["AVCHD:Timestamp"] = ts.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=offset)))
                        return

            for tag_name, timestamp in await loop.run_in_executor(None, get_quicktime_tags, f):
                row[tag_name] = timestamp

        def get_timestamp(r):
            def tries():
                yield r.get_exif_aware_timestamp("Exif.Photo.DateTimeOriginal")
                yield r["AVCHD:Timestamp"]
                yield r["Quicktime:CreateDate"]
                yield r["mtime"]

            return next((t for t in tries() if t is not None), None)

        for avchd_db in self.avchd_dirs:
            await loop.run_in_executor(None, avchd_db.read)

        for i, row in enumerate(selected_rows):
            yield i / len(selected_rows)
            file = row["full_path"]
            row["delta"] = app.current_timestamp_delta
            row["mtime"] = await loop.run_in_executor(None, get_mtime, file)
            row["row-bg-colour"] = "#ffffff"

            metadata = await loop.run_in_executor(None, self.get_exiv_metadata, file)

            if metadata is None:
                for tag_name in XMP_TAGS + EXIF_TAGS + IPTC_TAGS:
                    row[tag_name] = None
            else:
                for tag_name in XMP_TAGS:
                    tag = metadata.get(tag_name, SENTINEL)
                    if tag is not SENTINEL:
                        try:
                            row[tag_name] = tag.value
                        except pyexiv2.xmp.XmpValueError:
                            match = re.fullmatch(r'(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+\.?\d*)((?:[+-]\d+:\d+)?)', tag.raw_value)
                            if match:
                                year, month, day, hours, minutes, seconds, offset = match.groups()
                                if not offset:
                                    offset = "+00:00"
                                tag.raw_value = f"{year}-{month}-{day}T{hours}:{minutes}:{float(seconds):09.6f}{offset}"
                                row[tag_name] = tag.value
                            else:
                                raise

                for tag_name in EXIF_TAGS:
                    tag = metadata.get(tag_name, SENTINEL)
                    if tag is not SENTINEL:
                        row[tag_name] = tag.value
                assert SENTINEL is metadata.get("Exif.Photo.SubSecTime", SENTINEL)
                assert SENTINEL is metadata.get("Exif.Photo.SubSecTimeOriginal", SENTINEL)
                assert SENTINEL is metadata.get("Exif.Photo.SubSecTimeDigitized", SENTINEL)

                for tag_name in IPTC_TAGS:
                    if isinstance(tag_name, tuple):
                         date_tag_name, time_tag_name = tag_name
                         date_tag = metadata.get(date_tag_name, SENTINEL)
                         time_tag = metadata.get(time_tag_name, SENTINEL)
                         if date_tag is not SENTINEL and time_tag is not SENTINEL:
                            assert len(date_tag.value) == 1
                            assert len(time_tag.value) == 1
                            row[tag_name] = datetime.datetime.combine(date_tag.value[0], time_tag.value[0])
                    else:
                        tag = metadata.get(tag_name, SENTINEL)
                        if tag is not SENTINEL:
                            row[tag_name] = tag.value

            if row["Exif.Photo.DateTimeOriginal"] is not None and row["Exif.Image.TimeZoneOffset"] is None:
                row["Exif.Image.TimeZoneOffset"] = guess_timezone_offset(row, "Exif.Photo.DateTimeOriginal")

            if row["Exif.Photo.DateTimeOriginal"] is None:
                await try_other_tag_sources(row, file)

            row["timestamp"] = get_timestamp(row)

            # for tag in (
            #     "Xmp.photoshop.DateCreated",
            #     ("Iptc.Application2.DateCreated", "Iptc.Application2.TimeCreated"),
            #     ("Iptc.Application2.DigitizationDate", "Iptc.Application2.DigitizationTime"),
            # ):
            #     if row[tag] is not None:
            #         row[tag] = row["Xmp.xmp.CreateDate"]

        yield 1

    async def write_back(self, app, selected_rows=None):
        loop = asyncio.get_running_loop()
        timezone_offset_hours = round(app.current_timezone_offset.total_seconds()/(60**2))
        timezone = datetime.timezone(datetime.timedelta(hours=timezone_offset_hours))

        if selected_rows is None:
            selected_rows = self

        for i, row in enumerate(selected_rows):
            yield i / len(selected_rows)
            file = row["full_path"]
            ts_delta = row["delta"]
            ts_original = row["timestamp"].astimezone(timezone)
            ts_new = ts_original + ts_delta
            metadata = await loop.run_in_executor(None, self.get_exiv_metadata, file)

            if metadata is not None:
                for tag_name, ts in row.iter_exiv_timestamps():
                    if ts is not None:
                        ts = round_datetime_to_second(ts.astimezone(timezone) + ts_delta)
                        row[tag_name] = ts
                        if isinstance(tag_name, tuple):
                            date_tag_name, time_tag_name = tag_name
                            metadata[date_tag_name].value = [ts]
                            metadata[time_tag_name].value = [ts]
                        else:
                            metadata[tag_name].value = ts

                if row["Exif.Image.TimeZoneOffset"] is not None:
                    row["Exif.Image.TimeZoneOffset"] = timezone_offset_hours
                    metadata["Exif.Image.TimeZoneOffset"] = timezone_offset_hours

                await loop.run_in_executor(None, metadata.write)

            quicktime_tags = [t for t in QUICKTIME_TIMESTAMP_TAGS if row[t] is not None]
            if quicktime_tags:
                def format_arg(tag_name):
                    ts = row[tag_name].astimezone(datetime.timezone(datetime.timedelta())) + ts_delta
                    return "-{}={}".format(tag_name, ts.strftime("%Y:%m:%d %H:%M:%S"))
                def run():
                    subprocess.run(['exiftool', '-overwrite_original', file] + [
                        format_arg(t) for t in QUICKTIME_TIMESTAMP_TAGS
                    ], capture_output=True, check=True)
                await loop.run_in_executor(None, run)

            avchd_ts = row["AVCHD:Timestamp"]
            if avchd_ts is not None:
                for avchd_db in self.avchd_dirs:
                    if file.startswith(avchd_db.path):
                        avchd_ts = round_datetime_to_second(avchd_ts.astimezone(timezone) + ts_delta)
                        avchd_db.get_mts(os.path.basename(file))["datetime"] = avchd_ts
                        break

            await loop.run_in_executor(None, os.utime, file, (ts_new.timestamp(), ts_new.timestamp()))

        for avchd_db in self.avchd_dirs:
            await loop.run_in_executor(None, avchd_db.write)

        yield 1

    def __iter__(self):
        it = self.store.get_iter_first()
        while it is not None:
            yield self.Row(it)
            it = self.store.iter_next(it)

    def __len__(self):
        return len(self.store)

    def __getitem__(self, pos):
        if isinstance(pos, Gtk.TreePath):
            pos = self.store.get_iter(pos)
        return self.Row(pos)

    @staticmethod
    def _iter_given_files(all_given_files):
        for f in all_given_files:
            if os.path.isdir(f):
                for root, dirs, files in os.walk(f):
                    if root.endswith("AVCHD/BDMV"):
                        yield mpl_extract.MplDirectory(os.path.abspath(root))
                    for f in files:
                        yield os.path.abspath(os.path.join(root, f))
            elif os.path.isfile(f):
                yield os.path.abspath(f)
            else:
                raise Exception("File {} does not exist".format(f))

    async def populate(self, all_given_files):
        loop = asyncio.get_running_loop()
        yield 0.1

        file_list = await loop.run_in_executor(None, lambda: list(self._iter_given_files(all_given_files)))

        for f in file_list:
            if isinstance(f, mpl_extract.MplDirectory):
                self.avchd_dirs.append(f)
        file_list = [f for f in file_list if not isinstance(f, mpl_extract.MplDirectory)]
        n_files = len(file_list)
        file_list.sort()
        yield 0.4

        def get_prefix_length():
            if n_files == 0:
                return 0
            elif n_files == 1:
                path = file_list[0]
                return len(path) - len(os.path.basename(path))
            else:
                length = len(os.path.commonpath(file_list))
                if length == 1:
                    length = 0
                else:
                    length += 1
                return length
        prefix_len = get_prefix_length()
        yield 0.7

        for f in file_list:
            self.store.append([f, f[prefix_len:]] + [None]*(len(self._Row.INDICES) - 2))
        yield 1


class Widgets:
    def __init__(self, app):
        builder = Gtk.Builder()
        builder.add_from_file(os.path.join(PROJECT_ROOT, "layout.glade"))

        self.window = builder.get_object("main_window")
        self.toolbar = builder.get_object("tool_bar")
        self.reload_button = builder.get_object("reload_button")
        self.save_button = builder.get_object("save_button")
        self.status_label = builder.get_object("status_label")
        self.progress_bar = builder.get_object("progress_bar")
        self.treeview = builder.get_object("main_file_list")
        self.image_preview = builder.get_object("image_preview")
        self.timestamp_entry = builder.get_object("timestamp_entry")
        self.timestamp_delta_entry = builder.get_object("timestamp_delta_entry")
        self.timezone_offset_entry = builder.get_object("timezone_offset_entry")
        self.tag_edit_form = builder.get_object("tag_edit_form")
        self.lock_image_toggle = builder.get_object("lock_image_toggle")
        self.treeview_selection = self.treeview.get_selection()

        builder.connect_signals(app)
        self.treeview_selection.connect('changed', app.on_change_selected_image)

        self.treeview_selection.set_mode(Gtk.SelectionMode.MULTIPLE)

    def parse_timestamp_entry(self):
        try:
            return datetime.datetime.fromisoformat(self.timestamp_entry.get_text())
        except ValueError:
            return None

    def parse_timestamp_delta_entry(self):
        try:
            return str_to_datetime_delta(self.timestamp_delta_entry.get_text())
        except ValueError:
            return None

    def parse_timezone_offset_entry(self):
        try:
            return datetime.timedelta(hours=float(self.timezone_offset_entry.get_text()))
        except ValueError:
            return None


class Application:
    image_preview: Gtk.Image

    def __init__(self, given_files):
        self.given_files = given_files

        self.widgets = Widgets(self)

        self.ui_is_active = False
        self.current_original_timestamp = None
        self.current_timestamp_delta = datetime.timedelta()
        self.current_timezone_offset = DEFAULT_TIMEZONE_OFFSET
        self.current_image_is_locked = False
        self.locked_timestamp_deltas = sortedcontainers.SortedDict()

        self.changing_selected_image_task = make_null_task()
        self.updating_delta_db_task = make_null_task()

        self.loaded_files = FileStore()
        self.widgets.treeview.set_model(self.loaded_files.store)
        self.setup_treeview()

        self.update_ui()
        self.widgets.window.show_all()

    @property
    def current_new_timestamp(self):
        if self.current_original_timestamp is None:
            return None
        return self.current_original_timestamp + self.current_timestamp_delta

    @classmethod
    async def start(cls, given_files):
        self = cls(given_files)
        self.widgets.status_label.set_text("Listing files...")
        self.widgets.progress_bar.set_fraction(0)
        async for frac in self.loaded_files.populate(self.given_files):
            self.widgets.progress_bar.set_fraction(frac)
            await gbulb.wait_signal(self.widgets.progress_bar, "draw")
        self.widgets.status_label.set_text("Reading files...")
        self.widgets.progress_bar.set_fraction(0)
        async for frac in self.loaded_files.reload(self):
            self.widgets.progress_bar.set_fraction(frac)
        self.widgets.status_label.set_text("")
        self.ui_is_active = True
        self.update_ui()

    def notify(self, title, message):
        dialog = Gtk.MessageDialog(
            parent=self.widgets.window,
            flags=0,
            message_type=Gtk.MessageType.INFO,
            buttons=Gtk.ButtonsType.OK,
            text=title
        )
        dialog.format_secondary_text(message)
        dialog.run()
        dialog.destroy()

    def setup_treeview(self):
        loaded_files = self.loaded_files

        col = Gtk.TreeViewColumn('File', Gtk.CellRendererText(), text=loaded_files.idx("shortened_path"))
        col.set_sort_column_id(loaded_files.idx("shortened_path"))
        self.widgets.treeview.append_column(col)

        self.widgets.treeview.append_column(make_pyobject_column("Inferred Timestamp", loaded_files.idx("timestamp")))
        self.widgets.treeview.append_column(make_pyobject_column("Delta to apply", loaded_files.idx("delta"), background=loaded_files.idx("row-bg-colour")))
        self.widgets.treeview.append_column(make_pyobject_column("mtime", loaded_files.idx("mtime")))

        for t in ALL_TAGS:
            if isinstance(t, tuple):
                prefix = os.path.commonprefix(t)
                heading = prefix + '{{{}}}'.format(','.join(ti[len(prefix):] for ti in t))
            else:
                heading = t
            self.widgets.treeview.append_column(make_pyobject_column(heading, loaded_files.idx(t)))

        for col in self.widgets.treeview.get_columns():
            col.set_resizable(True)

    def update_ui(self):
        current_new_timestamp = self.current_new_timestamp
        if current_new_timestamp is None:
            self.widgets.timestamp_entry.set_text("")
            self.widgets.timestamp_entry.set_sensitive(False)
            self.widgets.timestamp_entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, None)
            self.widgets.timestamp_delta_entry.set_text("")
            self.widgets.timestamp_delta_entry.set_sensitive(False)
            self.widgets.timestamp_delta_entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, None)
            self.widgets.lock_image_toggle.set_active(False)
            self.widgets.lock_image_toggle.set_sensitive(False)
        else:
            self.widgets.timestamp_entry.set_sensitive(True)
            self.widgets.timestamp_delta_entry.set_sensitive(True)
            self.widgets.lock_image_toggle.set_sensitive(True)

            parsed = self.widgets.parse_timestamp_entry()
            if parsed != current_new_timestamp or parsed.tzinfo != current_new_timestamp.tzinfo:
                self.widgets.timestamp_entry.set_text(current_new_timestamp.isoformat(sep=' '))

            if self.widgets.parse_timestamp_delta_entry() != self.current_timestamp_delta:
                self.widgets.timestamp_delta_entry.set_text(datetime_delta_to_str(self.current_timestamp_delta))

            if self.current_image_is_locked != self.widgets.lock_image_toggle.get_active():
                self.widgets.lock_image_toggle.set_active(self.current_image_is_locked)

        if self.widgets.parse_timezone_offset_entry() != self.current_timezone_offset:
            self.widgets.timezone_offset_entry.set_text(str(self.current_timezone_offset.total_seconds() / 60**2))

        any_selected = self.widgets.treeview.get_selection().count_selected_rows() > 0
        self.widgets.reload_button.set_sensitive(any_selected)
        self.widgets.save_button.set_sensitive(any_selected)

        self.widgets.toolbar.set_sensitive(self.ui_is_active)
        self.widgets.treeview.set_sensitive(self.ui_is_active)
        self.widgets.tag_edit_form.set_sensitive(self.ui_is_active)

    def on_destroy(self, *args):
        asyncio.get_event_loop().stop()

    @contextlib.asynccontextmanager
    async def open_thumbnail_image_of(self, filepath):
        file_ext = os.path.splitext(filepath)[1].upper()
        if file_ext in {".MOV", ".MTS"}:
            def extract():
                vid = cv2.VideoCapture(filepath)
                success, image = vid.read()
                assert success
                cv2.imwrite("/home/dphoyes/tmp/wibble.JPG", image)
                success, buf = cv2.imencode(".JPG", image)
                assert success
                return AsyncBytesIO(buf.tobytes())

            img_bytes = await asyncio.get_running_loop().run_in_executor(None, extract)
            yield img_bytes
        else:
            async with aiofiles.open(filepath, 'rb') as f:
                yield f

    def on_change_selected_image(self, *args):
        self.changing_selected_image_task.cancel()

        async def update_thumbnail(filepath):
            self.widgets.image_preview.set_opacity(0.5)

            try:
                pbl = GdkPixbuf.PixbufLoader()
                try:
                    async with self.open_thumbnail_image_of(filepath) as f:
                        while True:
                            data = await f.read(65536)
                            if len(data) == 0:
                                break
                            pbl.write(data)
                finally:
                    pbl.close()
                pixbuf = pbl.get_pixbuf()
            except Exception as e:
                self.widgets.image_preview.set_from_icon_name("gtk-missing-image", Gtk.IconSize.DIALOG)
            else:
                allocation = self.widgets.image_preview.get_parent().get_allocation()
                allocation_ratio = allocation.height/allocation.width
                image_ratio = pixbuf.props.height/pixbuf.props.width

                if image_ratio < allocation_ratio:
                    new_width = allocation.width
                    new_height = int(new_width * image_ratio)
                else:
                    new_height = allocation.height
                    new_width = int(new_height / image_ratio)

                pixbuf = pixbuf.scale_simple(new_width, new_height, GdkPixbuf.InterpType.BILINEAR)
                self.widgets.image_preview.set_from_pixbuf(pixbuf)
                self.widgets.image_preview.set_opacity(1)


        row = self.get_current_row()
        row_timestamp = row and row["timestamp"]

        if row_timestamp is not None:
            self.current_original_timestamp = row_timestamp.astimezone(datetime.timezone(self.current_timezone_offset))
            self.current_timestamp_delta = row["delta"]
            self.current_image_is_locked = self.current_original_timestamp in self.locked_timestamp_deltas
            self.changing_selected_image_task = asyncio.create_task(update_thumbnail(row["full_path"]))
        else:
            self.current_original_timestamp = None
            self.widgets.image_preview.clear()
        self.update_ui()
        return self.changing_selected_image_task

    def get_current_row(self):
        s = self.get_current_row_selection()
        if len(s) == 1:
            return next(iter(s))
        else:
            return None

    def get_current_row_selection(self):
        return RowSelection(files=self.loaded_files, selection=self.widgets.treeview.get_selection())

    def on_change_image_size(self, obj, rect):
        pass
        # print(obj, rect.width, rect.height)

    def change_current_timezone_offset(self, delta):
        self.current_timezone_offset = delta
        if self.current_original_timestamp is not None:
            self.current_original_timestamp = self.current_original_timestamp.astimezone(datetime.timezone(delta))

    async def any_ongoing_db_changes(self):
        await asyncio.shield(asyncio.wait((
            self.updating_delta_db_task,
        )))

    def update_timestamp_delta_state(self):
        self.updating_delta_db_task.cancel()

        def get_delta(row):
            if len(self.locked_timestamp_deltas) == 0:
                return self.current_timestamp_delta
            elif len(self.locked_timestamp_deltas) == 1:
                return self.locked_timestamp_deltas.values()[0]
            else:
                timestamp = row["timestamp"]
                upper_i = self.locked_timestamp_deltas.bisect_left(timestamp)
                lower_i = upper_i - 1
                if upper_i == len(self.locked_timestamp_deltas):
                    return self.locked_timestamp_deltas.values()[-1]
                if lower_i == -1:
                    return self.locked_timestamp_deltas.values()[0]
                upper = self.locked_timestamp_deltas.items()[upper_i]
                lower = self.locked_timestamp_deltas.items()[lower_i]
                ratio = (timestamp - lower[0]) / (upper[0] - lower[0])
                return lower[1] + ratio * (upper[1] - lower[1])

        async def coro():
            for i, row in enumerate(self.loaded_files):
                if i%40 == 0:
                    await asyncio.sleep(0.01)
                row["delta"] = get_delta(row)
                if row["timestamp"] in self.locked_timestamp_deltas:
                    row["row-bg-colour"] = "#ffff00"
                else:
                    row["row-bg-colour"] = "#ffffff"

        key = self.current_original_timestamp
        if key is not None:
            if self.current_image_is_locked:
                self.locked_timestamp_deltas[key] = self.current_timestamp_delta
            else:
                self.locked_timestamp_deltas.pop(key, None)
                self.current_timestamp_delta = get_delta(self.get_current_row())

        self.updating_delta_db_task = asyncio.create_task(coro())
        return self.updating_delta_db_task

    def change_current_timestamp_delta(self, delta):
        if self.current_timestamp_delta != delta:
            self.current_timestamp_delta = delta
            if len(self.locked_timestamp_deltas):
                self.current_image_is_locked = True
            self.update_timestamp_delta_state()

    async def reset_all_deltas(self):
        self.current_timestamp_delta = datetime.timedelta()
        self.locked_timestamp_deltas.clear()
        self.current_image_is_locked = False
        task = self.update_timestamp_delta_state()
        self.update_ui()
        await task

    def on_timestamp_entry_changed(self, entry):
        new_ts = self.widgets.parse_timestamp_entry()
        if new_ts is None:
            entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, 'gtk-dialog-error')
            return
        entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, None)
        self.change_current_timestamp_delta(new_ts - self.current_original_timestamp)
        self.change_current_timezone_offset(new_ts.tzinfo.utcoffset(None))
        self.update_ui()

    def on_timestamp_delta_entry_changed(self, entry):
        delta = self.widgets.parse_timestamp_delta_entry()
        if delta is None:
            entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, 'gtk-dialog-error')
            return
        entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, None)
        self.change_current_timestamp_delta(delta)
        self.update_ui()

    def on_timezone_offset_entry_changed(self, entry):
        offset = self.widgets.parse_timezone_offset_entry()
        if offset is None:
            entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, 'gtk-dialog-error')
            return
        entry.set_icon_from_icon_name(Gtk.EntryIconPosition.SECONDARY, None)
        self.change_current_timezone_offset(offset)
        self.update_ui()

    def on_toggle_lock_image(self, toggle):
        if self.current_image_is_locked != toggle.get_active():
            self.current_image_is_locked = toggle.get_active()
            self.update_timestamp_delta_state()
            self.update_ui()

    def on_toggle_select_all_files(self, button):
        pressed = button.get_active()
        if pressed:
            self.widgets.treeview_selection.select_all()
            self.widgets.treeview.set_sensitive(False)
        else:
            self.widgets.treeview_selection.unselect_all()
            self.widgets.treeview.set_sensitive(True)

    def on_click_reload(self, *args):
        async def coro():
            await self.any_ongoing_db_changes()

            self.ui_is_active = False
            self.update_ui()
            self.widgets.status_label.set_text("Reloading...")
            self.widgets.progress_bar.set_fraction(0)

            await self.reset_all_deltas()
            async for frac in self.loaded_files.reload(self, self.get_current_row_selection()):
                self.widgets.progress_bar.set_fraction(frac)
            self.widgets.status_label.set_text("")
            self.ui_is_active = True

            self.on_change_selected_image()

        return asyncio.create_task(coro())

    def on_click_save(self, *args):
        async def coro():
            await self.any_ongoing_db_changes()

            self.ui_is_active = False
            self.update_ui()
            self.widgets.status_label.set_text("Writing...")
            self.widgets.progress_bar.set_fraction(0)
            async for frac in self.loaded_files.write_back(self, self.get_current_row_selection()):
                self.widgets.progress_bar.set_fraction(frac)

            await self.on_click_reload()

        return asyncio.create_task(coro())


def main():
    given_files = sys.argv[1:]
    loop = asyncio.get_event_loop()
    loop.create_task(Application.start(given_files))
    loop.run_forever()


if __name__ == '__main__':
    main()
