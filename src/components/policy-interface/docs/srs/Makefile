TEX=pdflatex
SRC=srs
TARGET=$(SRC).pdf

all: $(TARGET)

$(TARGET):
	$(TEX) $(SRC)
	for i in 1 2 3; do $(TEX) $(SRC); done

.PHONY: clean

clean:
	find . \( -name '*.blg' -print \) -or \( -name '*.aux' -print \) -or \
		\( -name '*.bbl' -print \) -or \( -name '*~' -print \) -or \
		\( -name '*.lof' -print \) -or \( -name '*.lot' -print \) -or \
		\( -name '*.toc' -print \) | xargs rm -f; \
	rm -f $(SRC).dvi $(SRC).log $(SRC).ps $(SRC).pdf _region_* TAGS
