/*
* Copyright [2014] Hewlett-Packard Development Company, L.P.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* as published by the Free Software Foundation; either version 2
* of the License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with this program; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/
#include <Rcpp.h>
#include <fstream>

using namespace std;

const int MAXLINE=256;

/*
 * returns  0 when there is no weight
 * returns  1 when there are weights
 * returns -1 when there is bad format
*/
int gethasweights(ifstream &inFile)
{
  char line[MAXLINE];
  inFile.seekg (0, inFile.beg); // make sure it is at the begining of the file
  int scanner, s, t;
  float w;

  while (inFile) {
    inFile.getline(line, MAXLINE);
    scanner = sscanf(line, "%d %d %f", &s, &t, &w);
    if (scanner == EOF || scanner == 0)
        continue;
    if (scanner == 2) {
//        Rprintf("found NO weights\n");
        return 0;
    }
    else if (scanner == 3) {
//        Rprintf("found weights\n");
        return 1;
    }
    else {
        Rf_error("Error: found 1 vertex in a line, bad file format\n");
        return -1;
    }
  } //while

  Rf_error("Error: found no edge list, bad file format\n");
  return -1;
}

/*
 * inputfile: the name of the input file
 * rows:      the number of row splits
 * cols:      the number of col splits
 * outfile:   the prefix name of the output file
 * return:    TRUE when the file is split successfully; FALSE otherwise
*/
RcppExport SEXP hpdsplitter (SEXP inputfile, SEXP rows, SEXP cols, SEXP outfile)
{
  BEGIN_RCPP
  // converting input R objects to their C++ equivalents
  std::string inName = Rcpp::as<std::string>(inputfile);
  const char* inputName = inName.c_str();
  std::string outName = Rcpp::as<std::string>(outfile);
  const char *outname = outName.c_str();
  int desired_rowsplits = Rcpp::as<int>(rows);    
  int desired_colsplits = Rcpp::as<int>(cols);    


  ifstream inFile (inputName);
  if (! inFile) 
  {
    Rf_error("Error: Could not open the file. Make sure that the file is accessible in the specified path\n");
    return R_NilValue;
  }

  int d,e;
  int v=0;
//  bool zerobased = false; // it becomes true when a vertex with id==0 is found
  int hasweights = gethasweights(inFile); // determines existence of weights on the edges
  if (hasweights == -1) return R_NilValue;

  int fscanner;
  char aline[MAXLINE];

  // finding the max vertex-ID. The min assumed to be 0 (zerobased) or 1 (!zerobased).
  inFile.clear();
  inFile.seekg (0, inFile.beg); // going to the begining of the file
  while (inFile) {
    inFile.getline(aline, MAXLINE);
    fscanner = sscanf(aline, "%d %d", &d, &e);
    if (fscanner == EOF || fscanner == 0)
        continue; // ignore the line without vertex
    if (fscanner != 2) {
      inFile.close();
      Rf_error("Error: There is a line with only one vertex.\n");
      return R_NilValue;
    }
    if (d < 0 || e < 0) {
      inFile.close();
      Rf_error("Error: Found a negative number for a vertex ID. Vertex IDs should be non-negative integers.\n");
      return R_NilValue;
    }

    v = max(v, d);
//    zerobased |= d==0;
    v = max(v, e);
//    zerobased |= e==0;
  } //while
//  zerobased = true; // for now, we assume that it is always zerobased
//  if (zerobased)
    v++;

//  Rprintf("%d vertices\n", v);
  if (desired_rowsplits > v)
  {
    Rf_error("Error: requested number of row splits is bigger than the number of vertices\n");
    return R_NilValue;
  }
  if (desired_colsplits > v)
  {
    Rf_error("Error: requested number of col splits is bigger than the number of vertices\n");
    return R_NilValue;
  }

  int rowsplitsize = v%desired_rowsplits==0 ? v/desired_rowsplits : v/desired_rowsplits+1;
  int colsplitsize = v%desired_colsplits==0 ? v/desired_colsplits : v/desired_colsplits+1;
  int rowsplits = v%rowsplitsize==0 ? v/rowsplitsize : v/rowsplitsize+1;
  int colsplits = v%colsplitsize==0 ? v/colsplitsize : v/colsplitsize+1;
  if (rowsplits != desired_rowsplits)
  {
    Rf_warning("WARNING: number of vertices is not divisible by requested number of row splits\n         new number of row splits is %d\n", rowsplits);
  }
  if (colsplits != desired_colsplits)
  {
    Rf_warning("WARNING: number of vertices is not divisible by requested number of col splits\n         new number of col splits is %d\n", colsplits);
  }

  int splits = rowsplits*colsplits;
  FILE **outfiles = new FILE*[splits];
//  long *lines = new long[splits]; // counting the number o lines in each output file
//  memset(lines, 0, splits*sizeof(long));
  char fname[100];

  for (int i=0; i<splits; i++)
  {
    sprintf(fname, "%s%d", outname, i);
    outfiles[i] = fopen(fname, "w");
    if (outfiles[i] == NULL) {
        Rf_error("Error in creating the output files.\n");
        return R_NilValue;
    }
  }

  inFile.clear();
  inFile.seekg (0, inFile.beg); // going to the begining of the file
  while (inFile)
  {
    float weight;
    inFile.getline(aline, MAXLINE);
    fscanner = sscanf(aline, "%d %d %f", &d, &e, &weight);
    if (fscanner == EOF || fscanner == 0)
        continue;
    if (hasweights == 1 && fscanner == 2)
        weight = 1.0;
/*
    if (!zerobased)
    {
      d--;
      e--;
    }
*/
    int rowsplit = d/rowsplitsize;
    int colsplit = e/colsplitsize;
    int targetfile = rowsplit*colsplits+colsplit;
    fprintf(outfiles[targetfile], "%d %d", d, e);
    if (hasweights == 1)
      fprintf(outfiles[targetfile], " %f\n", weight);
    else
//      fprintf(outfiles[targetfile], " 1\n", weight);
      fprintf(outfiles[targetfile], "\n");

//    lines[targetfile]++;
  } //while 

  for (int i=0; i<splits; i++)
  {
    fclose(outfiles[i]);
/*      sprintf(fname, "%s%d_lines", outname, i);
      outfiles[i] = fopen(fname, "w");
      fprintf(outfiles[i], "%ld\n", lines[i]);
      fclose(outfiles[i]);
*/
  }
  delete[] outfiles;

  inFile.close();
  // returning the number of vertices and used rowsplits and colsplits
  Rcpp::List ret; 
  ret["nVertices"] = v; ret["rowsplits"] = rowsplits; ret["colsplits"] = colsplits;
  ret["rowsplitsize"] = rowsplitsize; ret["colsplitsize"] = colsplitsize;
  ret["isWeighted"] = (hasweights == 1) ? true : false;
  return ret;

  END_RCPP
}
