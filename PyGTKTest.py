__author__ = 'antoniofsanjuan'

import gtk

window = gtk.Window()
window.set_title("PyGTK Test Window")
window.connect("destroy", gtk.main_quit)
window.show_all()

gtk.main()
