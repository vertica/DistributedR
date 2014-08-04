## twice seems enough for now -- could also use texi2dvi, not portably
../inst/doc/RODBC.pdf: RODBC.Rnw
	pdflatex RODBC.Rnw
	pdflatex RODBC.Rnw
	mv RODBC.pdf ../inst/doc
	@rm -f RODBC.aux RODBC.log RODBC.out
