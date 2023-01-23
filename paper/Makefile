##########################
## LaTeX PDF Build File ##
##########################

NAME = Paper
SUMMARY_NAME = Zusammenfassung
BIB_LIT = lit.aux
BIB_INET = int.aux

default: bibtex pdf summary clean_internal

pdf:
	pdflatex $(NAME).tex
	pdflatex $(NAME).tex

summary:	
	pdflatex $(SUMMARY_NAME).tex
	pdflatex $(SUMMARY_NAME).tex

bibtex:
	pdflatex $(NAME).tex
	cd literature
	bibtex $(BIB_LIT)
	bibtex $(BIB_LIT)
	bibtex $(BIB_INET)
	bibtex $(BIB_INET)
	cd ..

clean_internal:
	-rm -f *~ *.out *.aux *.toc *.lof *.log *.lot *.flg *.bbl *.blg *.idx *.ind *.ilg *.snm *.nav *.fig.bak *.nlo *.nls

clean:	clean_internal
	-rm -f *.dvi *.ps *.pdf
