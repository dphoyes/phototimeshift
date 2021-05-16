#!/usr/bin/env python3
import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk
import asyncio
import gbulb
gbulb.install(gtk=True)


class ProgressBarWindow(Gtk.Window):

    def __init__(self):
        Gtk.Window.__init__(self, title="ProgressBar Demo")
        self.set_border_width(10)

        vbox = Gtk.VBox()
        self.add(vbox)

        self.progressbar = Gtk.ProgressBar()
        vbox.pack_start(self.progressbar, True, True, 0)

        button = Gtk.Button(label="Magic button")
        button.connect("clicked", self.on_magic)
        vbox.pack_start(button, True, True, 0)
        self._magic_button = button

        button = Gtk.Button(label="Stop button")
        button.connect("clicked", self.on_stop)
        vbox.pack_start(button, True, True, 0)
        self._stop_button = button

        self._running = False

    def on_magic(self, button):
        async def coro():
            try:
                await gbulb.wait_signal(self._magic_button, "clicked")
                self.progressbar.set_text("blah blah!")
                self.progressbar.set_fraction(0.50)

                await asyncio.sleep(1)

                self.progressbar.set_fraction(0.75)
                self.progressbar.set_text("pouet pouet!")

                await gbulb.wait_signal(self._magic_button, "clicked")

                self.progressbar.set_fraction(1.0)
                self.progressbar.set_text("done!")

                await asyncio.sleep(1)

            finally:
                self.progressbar.set_fraction(0.0)
                self.progressbar.set_show_text(False)
                self._running = False

        if not self._running:
            self.progressbar.set_fraction(0.25)
            self.progressbar.set_text("do some magic!")
            self.progressbar.set_show_text(True)
            self._running = asyncio.create_task(coro())

    def on_stop(self, button):
        if self._running:
            self._running.cancel()


win = ProgressBarWindow()
win.connect("delete-event", lambda *args: loop.stop())
win.show_all()

loop = asyncio.get_event_loop()
loop.run_forever()