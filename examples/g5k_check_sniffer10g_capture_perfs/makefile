prefix=/opt/snf

.PHONY: clean

snf-capture: snf-capture.c makefile
	gcc -I $(prefix)/include/ `pkg-config --cflags --libs glib-2.0` -Wl,-rpath=/usr/lib/wireshark/,-rpath=$(prefix)/lib/ /usr/lib/wireshark/libwiretap.so $(prefix)/lib/libsnf.so $< -o $@

clean:
	rm -f snf-capture