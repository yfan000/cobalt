CC=gcc
FTB_HOME=/home/hnaik/devel/ftb/install
CFLAGS=-I$(FTB_HOME)/include
LDFLAGS=-L$(FTB_HOME)/lib -lftb -lpthread

TARGETS=ftb_hybrid_event \
	ftb_single_event \
	ftb_multiple_event \
	timed \
	hybrid \
	multiple_1 \
	multiple_2 \
	multiple_4 \
	multiple_8 \
	multiple_16


all: $(TARGETS)

%: %.c
	$(CC) -g $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS) *~

