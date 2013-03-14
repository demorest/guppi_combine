PROGS = guppi_combine
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin
all: $(PROGS)
install: $(PROGS)
	cp $(PROGS) $(BINDIR)

guppi_combine: guppi_combine.C
	$(CXX) -g -O2 `psrchive --cflags` -o $@ $< `psrchive --libs`

clean:
	rm -f $(PROGS) *.o
