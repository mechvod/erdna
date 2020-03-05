#!/bin/sh

# Sorry, people, all those makefiles and configure scripts are too complex
# for me, so you need to manually set all paths and run this script to get
# the binaries.

CC="cc"

#SEDNA_DIR="/usr/home/test/sedna"
SEDNA_DIR="/home/test/sedna"

#LIB_ERLANG="/usr/local/lib/erlang"
#LIB_ERLANG="/usr/lib/erlang"
LIB_ERLANG="/usr/lib64/erlang"
ERL_INTERFACE_DIR=${LIB_ERLANG}/lib/erl_interface-3.10.1

# Location of header and library files vary in different Erlang versions (and,
# probably, distribution packages), so for each header and library file
# explicitly specify its location

# Directory where ei.h resides
EI_HDR_DIR=${ERL_INTERFACE_DIR}/include

# Directory where eiext.h resides
EIEXT_HDR_DIR=${ERL_INTERFACE_DIR}/src/misc

# Directory where libei.a resides
EI_LIB_DIR=${ERL_INTERFACE_DIR}/lib

# Directory where libei_st.a resides
EI_ST_LIB_DIR=${ERL_INTERFACE_DIR}/lib

# --library-path=${LIB_ERLANG}/usr/lib,
${CC} -v -O2 -pipe -DWITHDEBUG -I${SEDNA_DIR} -I${EI_HDR_DIR} -I${EIEXT_HDR_DIR} erdna.c \
-Wl,-v,--library-path=${EI_LIB_DIR},--library-path=${EI_ST_LIB_DIR},\
--library-path=${SEDNA_DIR}/driver/c,--library=ei,\
--library=ei_st,--library=sedna -static -o erdna && \
${CC} -v -O2 -pipe -I${SEDNA_DIR} -I${EI_HDR_DIR} -I${EIEXT_HDR_DIR} erdna.c \
-Wl,-v,--library-path=${EI_LIB_DIR},--library-path=${EI_ST_LIB_DIR},\
--library-path=${SEDNA_DIR}/driver/c,--library=ei,\
--library=ei_st,--library=sedna -static -o erdna_nodebug\
  && erlc erdna_lib.erl \
&& { cd examples ; erlc utf8_words.erl ; }
