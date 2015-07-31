/********************************************************************
 *A scalable and high-performance platform for R.
 *Copyright (C) [2013] Hewlett-Packard Development Company, L.P.

 *This program is free software; you can redistribute it and/or modify
 *it under the terms of the GNU General Public License as published by
 *the Free Software Foundation; either version 2 of the License, or (at
 *your option) any later version.

 *This program is distributed in the hope that it will be useful, but
 *WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *General Public License for more details.  You should have received a
 *copy of the GNU General Public License along with this program; if
 *not, write to the Free Software Foundation, Inc., 59 Temple Place,
 *Suite 330, Boston, MA 02111-1307 USA
 ********************************************************************/

// Obtained from Rinternals.h, used to figure out the header sizes of
// R SEXP variables. Including Rinternals.h and RInside.h in the same file with
// #define USE_RINTERNALS introduces compilation errors

#ifndef __RINTERNALS_SNIPPET_H__
#define __RINTERNALS_SNIPPET_H__

typedef unsigned int SEXPTYPE;

struct sxpinfo_struct {
    SEXPTYPE type      :  5;
    /* ==> (FUNSXP == 99) %% 2^5 == 3 == CLOSXP
     * -> warning: `type' is narrower than values
     *              of its type
     * when SEXPTYPE was an enum */
    unsigned int obj   :  1;
    unsigned int named :  2;
    unsigned int gp    : 16;
    unsigned int mark  :  1;
    unsigned int debug :  1;
    unsigned int trace :  1;  /* functions and memory tracing */
    unsigned int spare :  1;  /* currently unused */
    unsigned int gcgen :  1;  /* old generation number */
    unsigned int gccls :  3;  /* node class */
}; /* Tot: 32 */

struct vecsxp_struct {
    R_len_t length;
    R_len_t truelength;
};

struct primsxp_struct {
    int offset;
};

struct symsxp_struct {
    struct SEXPREC *pname;
    struct SEXPREC *value;
    struct SEXPREC *internal;
};

struct listsxp_struct {
    struct SEXPREC *carval;
    struct SEXPREC *cdrval;
    struct SEXPREC *tagval;
};

struct envsxp_struct {
    struct SEXPREC *frame;
    struct SEXPREC *enclos;
    struct SEXPREC *hashtab;
};

struct closxp_struct {
    struct SEXPREC *formals;
    struct SEXPREC *body;
    struct SEXPREC *env;
};

struct promsxp_struct {
    struct SEXPREC *value;
    struct SEXPREC *expr;
    struct SEXPREC *env;
};

#define SEXPREC_HEADER \
    struct sxpinfo_struct sxpinfo; \
    struct SEXPREC *attrib; \
    struct SEXPREC *gengc_next_node, *gengc_prev_node

typedef struct SEXPREC {
    SEXPREC_HEADER;
    union {
      struct primsxp_struct primsxp;
      struct symsxp_struct symsxp;
      struct listsxp_struct listsxp;
      struct envsxp_struct envsxp;
      struct closxp_struct closxp;
      struct promsxp_struct promsxp;
    } u;
} SEXPREC, *SEXP;

typedef struct VECTOR_SEXPREC {
    SEXPREC_HEADER;
    struct vecsxp_struct vecsxp;
} VECTOR_SEXPREC, *VECSEXP;

typedef union {
    VECTOR_SEXPREC s;
    double align;
} SEXPREC_ALIGN;

#endif
