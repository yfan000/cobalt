CC=gcc
FTB_HOME=/home/hnaik/devel/ftb
CFLAGS=-I$(FTB_HOME)/include
LDFLAGS=-L$(FTB_HOME)/lib -lftb -lpthread

TARGETS=ftb_hybrid_event

all: $(TARGETS)

ftb_hybrid_event: ftb_hybrid_event.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS) *~

