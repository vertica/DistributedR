all:
	mkdir -p diagrams/
	python generate_diagrams.py logs/R_worker_jorgem_127.0.0.1.50000.log diagrams/a.js diagrams/a.msc
	./msc-generator-4.4/src/msc-gen --nocopyright -T png  -o diagrams/a.png -i diagrams/a.msc
