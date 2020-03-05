/*
   This program is a mediator between Erlang VM ang Sedna XML DB. It works
   via erlang "port" - it receives XQuery statements from stdin, executes
   them and sends result back to Erlang VM via stdout.
   To do: make diagnostic messages more informative
   To do: this program, in it's current form, is prone to excessive memory
   consumption if too long packet is sent. If the data are erroneous,
   the whole packet is to be parsed before error is reported. At present
   the whole packet is received and before decoding command name (1'st atom
   in the tuple) it's length is examined and if it exceeds maximum length
   for command name - the packet is discarded. Having command name decoded,
   we check the tuple arity and lengths of all next fields. Consider adding
   one more check - if the overall length of packet does not exceed maximum
   for this command (e.g. for most commands tuple contains only the command
   itself, for SEsetConnectionAttr maximum length results from workdir path
   name, for SEexecute packet size limit results from maximum query length
   and only for SEloadData it can be any up to MAX_INPUT_PACKET_LENGTH.
   If the packet is too long for it's command - it should be dropped without
   any further processing.
   Another approach is to examine data while receiving them from stdin -
   i.e. receive up to command atom header (w/o the command atom itself),
   check it's length and receive the atom only if it does not exceed
   COMMANDNAME_MAXLENGTH, then examine the command, if it is correct - 
   examine total packet length, and so on. If anything is wrong - the rest
   of the packet is discarded (just read sequentially into the same buffer,
   next fragment over the previous).
   The risk can be mitigated when tuples with command and data are formed
   on Erlang side sufficiently carefully. However, there may by use cases
   where tuples may be formed based on data from remote source, so that
   incorrect tuple may be received from EVM and computational resources
   will be spent for vain activity.
   To do: consider using conn->isConnectionOk for checking if connection
   is on;
   To do: add check that sscanf returns 1;
*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <strings.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>

/*
  The following directives need sedna installation directory to be added to
  include search path (compiler's command-line option -I/path/to/sedna)
*/
#include <driver/c/libsedna.h>
#include <driver/c/sp_defs.h>

/*
  Location of the following header files vary in different installations of
  Erlang, so their containing directories need to be added to compiler
  command-line options with -I${EI_DIR} and -I${EIEXT_DIR}
*/
#include <ei.h>
#include <eiext.h>

/*
  Number of bytes set in {packet,N} erlang:open_port option.
  SE_MAX_QUERY_SIZE is defined as 2MB (sedna/driver/c/sp_defs.h), so setting
  this to 2 (max packet length 64k) might not be enough. Thus, 4 is the only
  option in general. However, you can change it if your data will not exceed
  65535 or 255 bytes.
  Also read comment about MAX_OUTPUT_PAYLOAD_LENGTH below.
*/
#define PACKET_HEADER_LENGTH 4

// These two are used to indicate what atom to add to tuple - 'ok' or 'error'
#define SE_OK 1
#define SE_ERROR 0

/*
 ei_encode_binary copies the data, provided by pointer and length, to the
 encoded buffer. I want to avoid it. Another thing I want to avoid is creating
 exactly the same sequence of bytes every time when tuple {'ok',
 <<"Some string">>} or {'error',<<"Reason">>} is to be sent to EVM port.
 Though I dislike messing about other people's software internals, I dislike
 vain copying stronger.
 So, we prepare 2 fixed patterns comprising control data, atom 'ok' or 'error'
 and header for the binary that will be sent right after it.
 Packing retrieved string to tuple {'ok',<<"Some string">>} will add
 13 bytes to the length of payload:
 131 - version header
 104 - tuple indicator
   2 - tuple arity
 100 - atom indicator
   0 -
   2 - atom length (2 bytes)
 111 - o
 107 - k
 109 - binary indicator
   n
   n
   n
   n - binary length (4 bytes)
 Similarly, packing a string to tuple {'error',<<"Reason">>} will add
 16 bytes to the length of payload.
 Trailing NULL not provided.
*/
#define OK_TUPLE_PART_LENGTH 13
static char ok_tuple_part[OK_TUPLE_PART_LENGTH]=\
                      {ERL_VERSION_MAGIC,
                       ERL_SMALL_TUPLE_EXT,2,
                       ERL_ATOM_EXT,0,2,'o','k',
                       ERL_BINARY_EXT,0,0,0,0};
#define ERROR_TUPLE_PART_LENGTH 16
static char error_tuple_part[ERROR_TUPLE_PART_LENGTH]=\
                      {ERL_VERSION_MAGIC,
                       ERL_SMALL_TUPLE_EXT,2,
                       ERL_ATOM_EXT,0,5,'e','r','r','o','r',
                       ERL_BINARY_EXT,0,0,0,0};

/*
  Sedna does not limit size of data sent by server in responce to query.
  However, there is a "feature" in Erlang erl_interface library that limits
  maximum packet length:
  ei_decode_* and ei_encode_* functions use "index into the buffer" which is
  a pointer to int. This limits length of data buffer they can decode/encode
  by total number of INT_MAX bytes. So, maximum size of payload data that can
  be sent to EVM port on successful retrieval is INT_MAX-OK_TUPLE_PART_LENGTH.
  Note, this value has nothing to do with SEDNA_ATTR_MAX_RESULT_SIZE session
  attribute - query result of any length can be read from Sedna as a sequence
  of chunks of moderate size, so limit on packet length sent to EVM does not
  put any limit on query result size.
*/
#define MAX_OUTPUT_PAYLOAD_LENGTH (INT_MAX-OK_TUPLE_PART_LENGTH)

/*
  Query results are retrieved one by one with explicit transition to next
  item when the current one is completely transferred from Sedna to the
  client. Transfer of current item may be done by chunks of arbitrary size,
  with the last one being recognized by its length being less than requested.
  So having fixed chunk size we have two ways:
  - define fixed maximum number of chunks and declare array of pointers
    of MAX_CHUNKS length with initial values set to NULL. They will be
    assigned addresses of newly allocated data chunks upon consequent
    reading data portions from Sedna.
    With INT_MAX=2G-1 and chunk size 256 bytes obvious condition
    DATA_CHUNK_SIZE*MAX_CHUNKS>MAX_OUTPUT_PAYLOAD_LENGTH gives array size of
    8M elements. This approach (with bigger chunk size) may be acceptable
    when either memory is not an issue and quick retrieval is required,
    or when maximum result size is known in advance and the whole item
    can be stored in one chunk.
  - use structure comprising of datachunk and pointer to next similar
    structure. The one with last data chunk will have NULL pointer.
  Note: we do not need to reserve space for trailing NULL when sending
  data to EVM as we rely on byte count. However, we request DATA_CHUNK_SIZE-1
  bytes so that last byte remains zero in order to be able to print the
  chunk to log when logging is on.
*/
// ToDo: Consider making data chunk size configurable at runtime
#define DATA_CHUNK_SIZE 256
struct data_chunk {
 char data[DATA_CHUNK_SIZE];
 void *next;
};

/*
  Formally, Sedna does not impose any general limitations on incoming data.
  There is limit for query size (SE_MAX_QUERY_SIZE=2MB), but there is no
  limit on size of data portion sent to Sedna with SEloadData function.
  (Programmer's guide says about "parts of any convenient size").
  So, the limit will be set to architecture's limit of INT_MAX due to 
  integer type of "index" in ei_encode/ei_decode functions (see explanation
  about MAX_OUTPUT_PAYLOAD_LENGTH above).
  However, this macro can be set to lower value to prevent possible overload
  on memory-constrained systems. This does not prohibit loading large amounts
  of data, they still can be loaded as a sequence of smaller pieces.
  Note, that SEloadData also requires document and collection names with
  max length of 511 bytes each, so actual portion of data loaded within single
  invocation of SEloadData will be less than packet size by up to 1053 bytes
  (encoded tuple layout is 1+(1+1)+(1+2+10)+(1+4+data)+(1+4+511)+(1+4+511))
  Also note that packet size should not be less than 2097171 bytes - this is
  SE_MAX_QUERY_SIZE (2MB) plus 1+(1+1)+(1+2+9)+(1+4)=19 bytes overhead to
  encode a tuple {'SEexecute',<<"query body">>}
  Also note, that MAX_INPUT_PACKET_LENGTH must be less than UINT32_MAX
  so that when read_bytes() returns UINT32_MAX it means that too big packet
  was discarded and main loop will proceed to next packet.
  Note that these figures do not include trailing NULL because ei_decode_*
  functions use chunk lengths encoded in the buffer and do not rely
  on trailing NULL.
*/
//#define MAX_INPUT_PACKET_LENGTH (SE_MAX_QUERY_SIZE+19) // 2097171

// We are sure that input length will not exceed 2kB (e.g. we are going to
// run simple read-only queries to some database that is not to change).
// #define MAX_INPUT_PACKET_LENGTH 2048
#define MAX_INPUT_PACKET_LENGTH INT_MAX

#define LOADDATA_TUPLE_OVERHEAD 1053

#define ATTRNAME_MAXLENGTH 46 // With trailing NULL
#define NUM_ATTRS 9
const char* sedna_attrs[NUM_ATTRS]=\
                     {"SEDNA_ATTR_AUTOCOMMIT",
                      "SEDNA_ATTR_SESSION_DIRECTORY",
                      "SEDNA_ATTR_DEBUG",
                      "SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD",
                      // This one is not mentioned in Programmer's
                      // Guide but exists in src. The Guide, however
                      // mentiones declaring "boundary-space preserve",
                      // so I'm not sure this attr is to be set from API.
                      // Anyway, it is present here so that this array
                      // reflects enum SEattr in libsedna.h
                      "SEDNA_ATTR_CONCURRENCY_TYPE",
                      "SEDNA_ATTR_QUERY_EXEC_TIMEOUT",
                      "SEDNA_ATTR_LOG_AMOUNT",
                      "SEDNA_ATTR_MAX_RESULT_SIZE",
                      "SEDNA_ATTR_CDATA_PRESERVE_WHILE_LOAD"};
                      // The last one is not mentioned in Programmer's
                      // Guide but exists in src. The Guide, however
                      // mentiones option "cdata-section-preserve", so
                      // I'm not sure this attr is to be set from API.

// The following two arrays should help process several attributes
// in a single "switch/case" entry - those that receive values ON or OFF.
const int attr_off[NUM_ATTRS]={     30,  // SEDNA_ATTR_AUTOCOMMIT
                               INT_MAX,  // SEDNA_ATTR_SESSION_DIRECTORY
                                     0,  // SEDNA_ATTR_DEBUG
                                    35,  // SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD
                                         // Not sure this attr is to be set from API.
                               INT_MAX,  // SEDNA_ATTR_CONCURRENCY_TYPE
                               INT_MAX,  // SEDNA_ATTR_QUERY_EXEC_TIMEOUT
                               INT_MAX,  // SEDNA_ATTR_LOG_AMOUNT
                               INT_MAX,  // SEDNA_ATTR_MAX_RESULT_SIZE
                                    37}; // SEDNA_ATTR_CDATA_PRESERVE_WHILE_LOAD
                                         // Not sure this attr is to be set from API.
const int attr_on[NUM_ATTRS]={      31,  // SEDNA_ATTR_AUTOCOMMIT
                               INT_MAX,  // SEDNA_ATTR_SESSION_DIRECTORY
                                     1,  // SEDNA_ATTR_DEBUG
                                    36,  // SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD
                                         // Not sure this attr is to be set from API.
                               INT_MAX,  // SEDNA_ATTR_CONCURRENCY_TYPE
                               INT_MAX,  // SEDNA_ATTR_QUERY_EXEC_TIMEOUT
                               INT_MAX,  // SEDNA_ATTR_LOG_AMOUNT
                               INT_MAX,  // SEDNA_ATTR_MAX_RESULT_SIZE
                                    38}; // SEDNA_ATTR_CDATA_PRESERVE_WHILE_LOAD
                                         // Not sure this attr is to be set from API.

// Use all-lower-case so that erlang module has functions with the same names.
//
#define COMMANDNAME_MAXLENGTH 25 // With trailing NULL
#define NUM_COMMANDS 19
static char* sedna_commands[NUM_COMMANDS]=\
                     {"segetlasterrormsg",
                      "segetlasterrorcode",
                      "seconnect",
                      "seclose",
                      "sesetconnectionattr",
                      "segetconnectionattr",
                      "seresetallconnectionattr",
                      "sebegin",
                      "secommit",
                      "serollback",
                      "seconnectionstatus",
                      "setransactionstatus",
                      "seexecute",
                      "seexecutelong",
                      "senext",
                      "segetdata",
                      "sesetdebughandler",
                      "seloaddata",
                      "seendloaddata"};

// Just to avoid invoking strlen()
static char sedna_cmd_lengths[NUM_COMMANDS]=\
                     {17, // SEgetLastErrorMsg
                      18, // SEgetLastErrorCode
                       9, // SEconnect
                       7, // SEclose
                      19, // SEsetConnectionAttr
                      19, // SEgetConnectionAttr
                      24, // SEresetAllConnectionAttr
                       7, // SEbegin
                       8, // SEcommit
                      10, // SErollback
                      18, // SEconnectionStatus
                      19, // SEtransactionStatus
                       9, // SEexecute
                      13, // SEexecuteLong
                       6, // SEnext
                       9, // SEgetData
                      17, // SEsetDebugHandler
                      10, // SEloadData
                      13};// SEendLoadData

// See comment about allocation of short_buffer within main()
#if ATTRNAME_MAXLENGTH > COMMANDNAME_MAXLENGTH
#define SHORTBUFFER_LENGTH ATTRNAME_MAXLENGTH
#else
#define SHORTBUFFER_LENGTH COMMANDNAME_MAXLENGTH
#endif

#define Num_SEgetLastErrorMsg 0
#define Num_SEgetLastErrorCode 1
#define Num_SEconnect 2
#define Num_SEclose 3
#define Num_SEsetConnectionAttr 4
#define Num_SEgetConnectionAttr 5
#define Num_SEresetAllConnectionAttr 6
#define Num_SEbegin 7
#define Num_SEcommit 8
#define Num_SErollback 9
#define Num_SEconnectionStatus 10
#define Num_SEtransactionStatus 11
#define Num_SEexecute 12
#define Num_SEexecuteLong 13
#define Num_SEnext 14
#define Num_SEgetData 15
#define Num_SEsetDebugHandler 16
#define Num_SEloadData 17
#define Num_SEendLoadData 18

/*
  Decoding incoming packet involves copying data from the input buffer to
  the new place. Without proper checks this can result in vain copying really
  big pieces of data in memory (Sedna does not limit size of data chunk to
  load with SEloadData or query file pathlength in SEexecuteLong, so maximum
  size of packet may be as big as INT_MAX bytes, as explained above, unless we
  set on it some limitation, reasonable for our use case).
  So, to avoid processing incorrect packet (whether erroneous or malicious),
  we check if its length not exceeds maximum value for the command encoded
  in it.
*/
static int max_packet_lengths[NUM_COMMANDS]={1+(1+1)+(1+2+17), // SEgetLastErrorMsg 
                                      1+(1+1)+(1+2+18), // SEgetLastErrorCode
                                      1+(1+1)+(1+2+9)\
                                      +(1+4+SE_HOSTNAMELENGTH)\
                                      +(1+4+SE_MAX_DB_NAME_LENGTH)\
                                      +(1+4+SE_MAX_LOGIN_LENGTH)\
                                      +(1+4+SE_MAX_PASSWORD_LENGTH), // SEconnect
                                      1+(1+1)+(1+2+7), //"SEclose",
                                      1+(1+1)+(1+2+19)+(1+4+28)\
                                      +(1+4+SE_MAX_DIR_LENGTH), // SEsetConnectionAttr - SEDNA_ATTR_SESSION_DIRECTORY
                                      1+(1+1)+(1+2+19)\
                                      +(1+4+ATTRNAME_MAXLENGTH), // SEgetConnectionAttr
                                      1+(1+1)+(1+2+24), // SEresetAllConnectionAttr
                                      1+(1+1)+(1+2+7), // SEbegin
                                      1+(1+1)+(1+2+8), // SEcommit
                                      1+(1+1)+(1+2+10), // SErollback
                                      1+(1+1)+(1+2+18), // SEconnectionStatus
                                      1+(1+1)+(1+2+19), // SEtransactionStatus
                                      1+(1+1)+(1+2+9)\
                                      +(1+4+SE_MAX_QUERY_SIZE), // SEexecute
                                      MAX_INPUT_PACKET_LENGTH, // SEexecuteLong
                                      1+(1+1)+(1+2+6), // SEnext
                                      1+(1+1)+(1+2+9), // SEgetData
                                      1+(1+1)+(1+2+17)+(1+4+4), // SEsetDebugHandler
                                      MAX_INPUT_PACKET_LENGTH, // SEloadData
                                      1+(1+1)+(1+2+13)}; // SEendLoadData

/*
   At present (commit 4b01892 @github) the last error code is 38, so, starting
   with 0 there should be 39 of them, but 22nd is missing.
*/
#define SE_ERRORS_NUM 38
const char* sedna_err_name[SE_ERRORS_NUM]=\
                     {"SEDNA_OPERATION_SUCCEEDED",
                      "SEDNA_SESSION_OPEN",
                      "SEDNA_SESSION_CLOSED",
                      "SEDNA_AUTHENTICATION_FAILED",
                      "SEDNA_OPEN_SESSION_FAILED",
                      "SEDNA_CLOSE_SESSION_FAILED",
                      "SEDNA_QUERY_SUCCEEDED",
                      "SEDNA_QUERY_FAILED",
                      "SEDNA_UPDATE_SUCCEEDED",
                      "SEDNA_UPDATE_FAILED",
                      "SEDNA_BULK_LOAD_SUCCEEDED",
                      "SEDNA_BULK_LOAD_FAILED",
                      "SEDNA_BEGIN_TRANSACTION_SUCCEEDED",
                      "SEDNA_BEGIN_TRANSACTION_FAILED",
                      "SEDNA_ROLLBACK_TRANSACTION_SUCCEEDED",
                      "SEDNA_ROLLBACK_TRANSACTION_FAILED",
                      "SEDNA_COMMIT_TRANSACTION_SUCCEEDED",
                      "SEDNA_COMMIT_TRANSACTION_FAILED",
                      "SEDNA_NEXT_ITEM_SUCCEEDED",
                      "SEDNA_NEXT_ITEM_FAILED",
                      "SEDNA_NO_ITEM",
                      "SEDNA_RESULT_END",
                      "SEDNA_DATA_CHUNK_LOADED",
                      "SEDNA_ERROR",
                      "SEDNA_TRANSACTION_ACTIVE",
                      "SEDNA_NO_TRANSACTION",
                      "SEDNA_CONNECTION_OK",
                      "SEDNA_CONNECTION_CLOSED",
                      "SEDNA_CONNECTION_FAILED",
                      "SEDNA_AUTOCOMMIT_OFF",
                      "SEDNA_AUTOCOMMIT_ON",
                      "SEDNA_SET_ATTRIBUTE_SUCCEEDED",
                      "SEDNA_GET_ATTRIBUTE_SUCCEEDED",
                      "SEDNA_RESET_ATTRIBUTES_SUCCEEDED",
                      "SEDNA_BOUNDARY_SPACE_PRESERVE_OFF",
                      "SEDNA_BOUNDARY_SPACE_PRESERVE_ON",
                      "SEDNA_CDATA_PRESERVE_OFF",
                      "SEDNA_CDATA_PRESERVE_ON"};
const int sedna_err_code[SE_ERRORS_NUM]={(-1),
                                           1,
                                           2,
                                         (-3),
                                         (-4),
                                         (-5),
                                           6,
                                         (-7),
                                           8,
                                         (-9),
                                          10,
                                        (-11),
                                          12,
                                        (-13),
                                          14,
                                        (-15),
                                          16,
                                        (-17),
                                          18,
                                        (-19),
                                        (-20),
                                        (-21),
                                          23,
                                        (-24),
                                          25,
                                          26,
                                          27,
                                          28,
                                        (-29),
                                          30,
                                          31,
                                          32,
                                          33,
                                          34,
                                          35,
                                          36,
                                          37,
                                          38};


#define DEBUG_HANDLER_NAME_MAXLENGTH 5 // With trailing NULL
#define NUM_DEBUG_HANDLERS 2
#define DEBUG_HANDLER_NONE 0
#define DEBUG_HANDLER_LOG  1
//const char* sedna_debug_handlers[NUM_DEBUG_HANDLERS]={"none","log"};
const char* sedna_debug_handlers[NUM_DEBUG_HANDLERS]={"NONE","LOG"};
// At present the only available debug trace handling is writing it to
// the log file. Other types of handlers may be added in future.


// declare some often-used variables with global scope so that we don't need
// to declare them on every function invocation.
//static unsigned char bitshift;
//static char bitshift;
static int bitshift;
// We don't use packetheader as a string, only use its bytes by counter.
// So we don't need extra byte to keep trailing NULL.
static unsigned char packetheader[PACKET_HEADER_LENGTH];
static char *tmp_ptr;

/*
  Variables that count bytes while receiving/sending data must be unsigned
  32 bit integer (PACKET_HEADER_LENGTH*8). However, read() and write() return
  signed ssize_t. So, when comparing them, cast both to signed type long long.
  Another issue is which format specifier to use in printf(3) (%d or %ld, %u
  or %lu) as we do not know what type is aliased as ssize_t (int or long) or
  uint32_t (uint or ulong) .
  To be simple, all signed values (namely those of type ssize_t returned by
  read(2) and write (2)) will be cast to long long and printed as "%lld".
  Unsigned values will be cast to unsigned long long and printed as "%llu".
  This may bring some computational overhead, but it will occur only when
  logging is on (i.e. when developing, deploying or testing, not under
  commercial load).
*/
static ssize_t bytecount0;
static uint32_t bytecount1, bytecount2, bytecount3, bytecount4;

// Function used as Sedna debug handler accepts only 2 variables - type of
// message (int) and message itself (char*). Making pointer to log file 
// global variable is the only way to make it accesible for this function.
// On Linux mkstemp requires exactly six positions for substitution.
#define LOGPATH_TEMPLATE "./erdnalog_%llu_XXXXXX"
#define LOGPATH_MAXLENGTH 37 // 9+20+1+6+1
// "erdnalog_" + "18446744073709551615" + "_" + "XXXXXX" + NULL
// No wish to find out what type pid_t actually is, so prepare to
// accomodate 2^64-1 as PID

FILE *logfile;

uint32_t read_bytes(char *buf, uint32_t numbytes)
{
 bytecount1=0;
#ifdef WITHDEBUG
 if (logfile != NULL ) {
  do {
   if ((bytecount0 = read(0, buf+bytecount1, numbytes-bytecount1)) <= 0) {
    fprintf(logfile,"read_bytes: failed, bytecount0=%lld, errno=%d\n", (long long)bytecount0, errno);
    fflush(logfile);
    return(0);
   }
   fprintf(logfile,"read_bytes: read, bytecount0=%lld, bytecount1=%llu\n", (long long)bytecount0, (unsigned long long)bytecount1);
   fflush(logfile);
   bytecount1 += bytecount0;
  } while (bytecount1<numbytes);
 } else {
#endif
  do {
   if ((bytecount0 = read(0, buf+bytecount1, numbytes-bytecount1)) <= 0) {
    return(0);
   }
   bytecount1 += bytecount0;
  } while (bytecount1<numbytes);
#ifdef WITHDEBUG
 }
#endif
 return(bytecount1);
}

uint32_t discard_bytes(uint32_t numbytes)
{
 uint32_t datasize=(uint32_t)getpagesize();
 // Or may be use sysconf(_SC_PAGESIZE)?
 bytecount0=0;
 bytecount1=0;
 tmp_ptr=(char*)malloc(datasize);
 if (tmp_ptr==NULL) {
  // Frankly, I am not sure if the program can report anything (and perform
  // as intended at all) after malloc has failed.
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"malloc failed");
   fflush(logfile);
  }
#endif
  return(0);
 }
 // Remember, that numbytes may be not multiple of page size
 if ( numbytes>datasize ) {
  // Note that numbytes and datasize are both unsigned, if numbytes<datasize
  // then numbytes-datasize will also be unsigned, wrapped around UINT32_MAX
  // (e,g, 1-2=(2^32)-1). We need this check if we want to work with small
  // input packet length (less that pagesize)
#ifdef WITHDEBUG
  if (logfile != NULL ) {
   while(bytecount1<numbytes-datasize) {
    if((bytecount0=read(0,tmp_ptr,datasize))<=0) {
     fprintf(logfile,"discard_bytes: failed, bytecount0=%lld, errno=%d\n", (long long)bytecount0, errno);
     fflush(logfile);
     return(0);
    }
    bytecount1 += bytecount0;
   }
  } else {
#endif
   while(bytecount1<numbytes-datasize) {
    if((bytecount0=read(0,tmp_ptr,datasize))<=0) {
     return(0);
    }
    bytecount1 += bytecount0;
   }
#ifdef WITHDEBUG
  }
#endif
 }
 datasize=0;
#ifdef WITHDEBUG
 if (logfile != NULL ) {
  do {
   if ((bytecount0=read(0,tmp_ptr,numbytes-bytecount1))<=0) {
    fprintf(logfile,"discard_bytes: failed, bytecount0=%lld, errno=%d\n", (long long)bytecount0, errno);
    fflush(logfile);
    return(0);
   }
   datasize += bytecount0;
  } while (datasize<numbytes-bytecount1);
 } else {
#endif
  do {
   if ((bytecount0=read(0,tmp_ptr,numbytes-bytecount1))<=0) {
    return(0);
   }
   datasize += bytecount0;
  } while (datasize<numbytes-bytecount1);
#ifdef WITHDEBUG
 }
#endif
 free(tmp_ptr);
 return(bytecount1+datasize);
}

uint32_t read_data(char **buf)
{
 bzero(packetheader,PACKET_HEADER_LENGTH);
 if (read_bytes(packetheader, PACKET_HEADER_LENGTH) != PACKET_HEADER_LENGTH) {
#ifdef WITHDEBUG
  if (logfile != NULL ) {
   fprintf(logfile,"read_data: failed to read packet header\n");
   fflush(logfile);
  }
#endif
  *buf=NULL;
  return(0);
 }
 bytecount2=0;
 for (bitshift=PACKET_HEADER_LENGTH-1;bitshift>=0;bitshift--) {
  bytecount2 |=(packetheader[PACKET_HEADER_LENGTH-1-bitshift]<<(bitshift*8));
#ifdef WITHDEBUG
  if (logfile != NULL ) {
   fprintf(logfile,"bitshift=%d, %d'th byte=%u, bytecount2=%llu \n",bitshift,PACKET_HEADER_LENGTH-1-bitshift,packetheader[PACKET_HEADER_LENGTH-1-bitshift],(unsigned long long)bytecount2);
   fflush(logfile);
  }
#endif
 }
 bzero(packetheader,PACKET_HEADER_LENGTH);

 if (bytecount2>MAX_INPUT_PACKET_LENGTH) {
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"Packet length exceeds MAX_INPUT_PACKET_LENGTH\n");
   fflush(logfile);
  }
#endif
  *buf=NULL;
  if (discard_bytes(bytecount2)!=bytecount2) {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"Failed to skip %llu bytes\n",(unsigned long long)bytecount2);
    fflush(logfile);
   }
#endif
   return(0);
  } else {
   return(UINT32_MAX);
  }
 }
 tmp_ptr=(char*)malloc(bytecount2);
 if (tmp_ptr==NULL) {
  // Frankly, I am not sure if the program can report anything (and perform
  // as intended at all) after malloc has failed.
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"malloc failed");
   fflush(logfile);
  }
#endif
  *buf=NULL;
  return(0);
 }
#ifdef WITHDEBUG
 if (logfile != NULL) {
  fprintf(logfile,"read_data: going to read %llu bytes\n", (unsigned long long)bytecount2);
  fflush(logfile);
 }
#endif
 bzero(tmp_ptr,bytecount2);
 // If everything goes right, all bytecount2 bytes at tmp_ptr will be filled
 // while reading. If something goes wrong, tmp_ptr will be just free'd.
 // But let's keep good tradition of bzero'ing data upon allocation :))
 bytecount3=read_bytes(tmp_ptr, bytecount2);
 if (bytecount3!=bytecount2) {
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"read_data failed, set in header=%llu, received=%llu\n", (unsigned long long)bytecount2, (unsigned long long)bytecount3);
   fflush(logfile);
  }
#endif
  free(tmp_ptr);
  *buf=NULL;
  return(0);
 }
#ifdef WITHDEBUG
 if (logfile != NULL) {
  for (bytecount3=0;bytecount3<bytecount2;bytecount3++)
   fprintf(logfile,"%llu - %d\n",(unsigned long long)bytecount3,tmp_ptr[bytecount3]);
 }
#endif
 *buf=tmp_ptr;
 return(bytecount2);
}

uint32_t write_bytes(char *buf, uint32_t numbytes)
{
 bytecount1=0;
#ifdef WITHDEBUG
 if (logfile!=NULL) {
  do {
   fprintf(logfile,"write_bytes: %llu - \"%s\" \n",(unsigned long long)numbytes,buf);
   fflush(logfile);
   if ((bytecount0 = write(1, buf+bytecount1, numbytes-bytecount1)) <= 0) {
    fprintf(logfile,"write_bytes: failed, bytecount0=%lld, errno=%d\n",(long long)bytecount0,errno);
    fflush(logfile);
    return (0);
   }
   bytecount1 += bytecount0;
  } while (bytecount1<numbytes);
 } else {
#endif
  do {
   if ((bytecount0 = write(1, buf+bytecount1, numbytes-bytecount1)) <= 0) {
    return (0);
   }
   bytecount1 += bytecount0;
  } while (bytecount1<numbytes);
#ifdef WITHDEBUG
 }
#endif
 return (bytecount1);
}

/*
  For the sake of code brevity and simplicity all results will be sent as
  strings (in C notion, "binaries" in Erlang notion). Numeric (integer) values
  should be expressed as strings (using sprintf()). This helps getting rid of
  necessity to use separate functions (write_tuple_string and
  write_tuple_integer) for string and integer values.
  Furthermore, some connection options are actually integers, but interchanging
  them with EVM port as integers will lay on EVM extra burden of decoding them
  into meaningful strings. Instead, strings (e.g. attr name 
  <<"SEDNA_ATTR_AUTOCOMMIT">> and value <<"ON">>) will be sent/received
  instead of numeric values (for this example - 0 and 31 respectively).
  Length of payload that will be encoded into packet may differ from length
  of data actually send. This is because data from Sedna are received as
  chunks and concatenating them into solid piece would require allocating
  memory enough to accomodate all received chunks. Another way is to set
  full length in the encoded tuple, but send it with first chunk only, and
  after that send remaining chunks directly using write_bytes. For all other
  cases number encoded into tuple as binary length and number of bytes
  actually passed to write_bytes are equal.
  payloadlength - length of piece of data actually sent in this invocation of
  write_tuple.
  binarylength - length to encode into packet, may be equal to payloadlength
  (if the whole amount of data is sent at once) or exceed it by total length
  of all subsequent portions of binary.
*/
uint32_t write_tuple(unsigned char result, char *buf, uint32_t binarylength,
                     uint32_t payloadlength)
{
 bzero(packetheader,PACKET_HEADER_LENGTH);
 if (result) {
#ifdef WITHDEBUG
  if (logfile != NULL){
   // When result is SE_OK, there may be chain of data chunks, so place 
   // different log records for complete and incomplete payload.
   if (binarylength==payloadlength) {
    fprintf(logfile, "Reply tuple: {'ok',<<\"%s\">>}\n",buf);
   } else  {
    fprintf(logfile, "Reply tuple: {'ok',<<\"%s...\">>}\n",buf);
   }
   fflush(logfile);
  }
#endif
  ok_tuple_part[OK_TUPLE_PART_LENGTH-4]=((binarylength) >>  24) & 0xff;
  ok_tuple_part[OK_TUPLE_PART_LENGTH-3]=((binarylength) >>  16) & 0xff;
  ok_tuple_part[OK_TUPLE_PART_LENGTH-2]=((binarylength) >>  8) & 0xff;
  ok_tuple_part[OK_TUPLE_PART_LENGTH-1]=(binarylength) & 0xff;
  bytecount2=OK_TUPLE_PART_LENGTH+binarylength;
  for (bitshift=0;bitshift<PACKET_HEADER_LENGTH;bitshift++) {
   packetheader[bitshift]=(bytecount2>>((PACKET_HEADER_LENGTH-1-bitshift)*8))&0xff;
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"encoded %d'th byte: %d\n",bitshift,packetheader[bitshift]);
    fflush(logfile);
   }
#endif
  }
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"Going to write header %4s\n",packetheader);
   fflush(logfile);
  }
#endif
  bytecount3=write_bytes(packetheader, (uint32_t)PACKET_HEADER_LENGTH);
  if (bytecount3!=PACKET_HEADER_LENGTH) {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"write_data: header fail, written=%llu\n",(unsigned long long)bytecount3);
    fflush(logfile);
   }
#endif
   return(0);
  }
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"Going to write %d bytes of tuple part.\n",OK_TUPLE_PART_LENGTH);
   fflush(logfile);
  }
#endif
  bytecount3=write_bytes(ok_tuple_part, (uint32_t)OK_TUPLE_PART_LENGTH);
  if (bytecount3!=OK_TUPLE_PART_LENGTH) {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"write_data: tuple part fail, written=%llu\n",(unsigned long long)bytecount3);
    fflush(logfile);
   }
#endif
   return(0);
  }
  if (payloadlength!=0) {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"Going to write %llu bytes of payload.\n",(unsigned long long)payloadlength);
    fflush(logfile);
   }
#endif
   bytecount3=write_bytes(buf, (uint32_t)payloadlength);
   if (bytecount3!=payloadlength) {
#ifdef WITHDEBUG
    if (logfile != NULL) {
     fprintf(logfile,"write_data: payload fail, written=%llu\n",(unsigned long long)bytecount3);
     fflush(logfile);
    }
#endif
    return(0);
   }
  } else {
   bytecount3=0;
  }
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"write_data: written=%d+%d+%llu\n",PACKET_HEADER_LENGTH,OK_TUPLE_PART_LENGTH,(unsigned long long)bytecount3);
   fflush(logfile);
  }
#endif
  ok_tuple_part[OK_TUPLE_PART_LENGTH-4]=0;
  ok_tuple_part[OK_TUPLE_PART_LENGTH-3]=0;
  ok_tuple_part[OK_TUPLE_PART_LENGTH-2]=0;
  ok_tuple_part[OK_TUPLE_PART_LENGTH-1]=0;
 } else {
#ifdef WITHDEBUG
  if (logfile != NULL){
   fprintf(logfile, "Reply tuple: {'error',<<\"%s\">>}\n",buf);
   fflush(logfile);
  }
#endif
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-4]=((binarylength) >>  24) & 0xff;
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-3]=((binarylength) >>  16) & 0xff;
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-2]=((binarylength) >>  8) & 0xff;
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-1]=(binarylength) & 0xff;
  bytecount2=ERROR_TUPLE_PART_LENGTH+binarylength;
  for (bitshift=0;bitshift<PACKET_HEADER_LENGTH;bitshift++) {
   packetheader[bitshift]=(bytecount2>>((PACKET_HEADER_LENGTH-1-bitshift)*8))&0xff;
   // When result is SE_ERROR, there may only be brief explanation of error,
   // no need for diferent log records for complete and incomplete payload.
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"encoded %d'th byte: %d\n",bitshift,packetheader[bitshift]);
    fflush(logfile);
   }
#endif
  }
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"Going to write header\n");
   fflush(logfile);
  }
#endif
  bytecount3=write_bytes(packetheader, (uint32_t)PACKET_HEADER_LENGTH);
  if (bytecount3!=PACKET_HEADER_LENGTH) {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"write_data: header fail, written=%llu\n",(unsigned long long)bytecount3);
    fflush(logfile);
   }
#endif
   return(0);
  }
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"Going to write %d bytes of tuple part.\n",ERROR_TUPLE_PART_LENGTH);
   fflush(logfile);
  }
#endif
  bytecount3=write_bytes(error_tuple_part, (uint32_t)ERROR_TUPLE_PART_LENGTH);
  if (bytecount3!=ERROR_TUPLE_PART_LENGTH) {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"write_data: tuple part fail, written=%llu\n",(unsigned long long)bytecount3);
    fflush(logfile);
   }
#endif
   return(0);
  }
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"Going to write %llu bytes of payload.\n",(unsigned long long)payloadlength);
   fflush(logfile);
  }
#endif
  bytecount3=write_bytes(buf, (uint32_t)payloadlength);
  if (bytecount3!=payloadlength) {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile,"write_data: payload fail, written=%llu\n",(unsigned long long)bytecount3);
    fflush(logfile);
   }
#endif
   return(0);
  }
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"write_data: written=%d+%d+%llu\n",PACKET_HEADER_LENGTH,ERROR_TUPLE_PART_LENGTH,(unsigned long long)bytecount3);
   fflush(logfile);
  }
#endif
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-4]=0;
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-3]=0;
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-2]=0;
  error_tuple_part[ERROR_TUPLE_PART_LENGTH-1]=0;
 }
 return(bytecount3);
}

void debug_logger(enum se_debug_info_type subtype, const char *msg)
{
// This function does not check validity of log pointer. Main function is to
// ensure it is valid before setting debug handler.
 switch (subtype) {
  case se_QueryTrace :
   fprintf(logfile,"Trace : ");
   break;
  case se_QueryDebug :
   fprintf(logfile,"Debug : ");
   break;
  default:
   fprintf(logfile,"Unknown : ");
 }
 fprintf(logfile,"%s\n", msg);
}

FILE *open_log()
{
 int log_fdescr=INT_MAX;
 char *logname=(char*)malloc(LOGPATH_MAXLENGTH);
 bzero(logname, LOGPATH_MAXLENGTH);
 sprintf(logname,LOGPATH_TEMPLATE,(unsigned long long)getpid());
 printf("%s -> %i\n",logname,log_fdescr);
 log_fdescr=mkstemp(logname);
 printf("%s -> %i\n",logname,log_fdescr);
 return(fdopen(log_fdescr,"w"));
}

int main(int argc, char *argv[])
{
 struct SednaConnection connection = SEDNA_CONNECTION_INITIALIZER;
#ifdef _WIN32
  SOCKET invalid_socket=INVALID_SOCKET;
#else
  int invalid_socket=-1;
#endif
 int int_value1, int_value2, int_value3, int_value4;
 long long_value1;
 unsigned long long ull_value1;
 int *buf_index;
 char *read_buf;
 // Allocate constant buffer for variables with size not exceeding certain
 // limit. Those are: command name, attribute name, text representation of
 // integer value (error code or some attribute values), some attribute
 // values like "ON" or "OFF". At present (Sedna commit 4b01892 @github)
 // SHORTBUFFER_LENGTH=46 (length of 
 // "SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD"+NULL) which is enogh to
 // accomodate even text representation of 128-bit integer (39 decimal digits 
 // 2^128-1=340282366920938463463374607431768211455), so no extra checks
 // needed to ensure that sprintf'ed integer will fit whthin the buffer.
 char short_buffer[SHORTBUFFER_LENGTH];
 char *tmp_ptr1, *tmp_ptr2, *tmp_ptr3, *tmp_ptr4;
 struct data_chunk *first_chunk, *current_chunk, *next_chunk;
 buf_index=(int*)malloc(sizeof(int));
 logfile=NULL;

 if (argc==2 && (strcmp(argv[1],"-h")==0 || strcmp(argv[1],"-help")==0 || strcmp(argv[1],"--help")==0)) {
  printf("This program relays data between Erlang VM and Sedna XML DB. It works\
 via Erlang \"port\" (stdin/stdout). It is not to be run from shell.\nUsage:\n\
erdna -help\n print this message\nerdna -log\n write log to ./erdnalog_<PID>_XXX.log\
 (if compiled with support for logging)\n");
  return 0;
 } else if (argc==2 && strncmp(argv[1],"-log",4)==0) {
  logfile=open_log();
  fprintf(logfile,"Ernda started.\n");
#ifndef WITHDEBUG
  fprintf(logfile,"This binary was compiled without support for logging.\n");
#endif
  fflush(logfile);
 } else if (argc>1) {
  printf("Usage:erdna -help\nor\nerdna -log");
  return(-1);
 }

 bytecount4=read_data(&read_buf);
 while (bytecount4>0) {
  if (bytecount4==UINT32_MAX) {
   write_tuple(SE_ERROR, "Packet length exceeds limit", 45, 45);
  } else {
#ifdef WITHDEBUG
   if (logfile != NULL) {
    fprintf(logfile, "Received %d bytes.\n", bytecount4);
//    for (bytecount1=0;bytecount1<bytecount4;bytecount1++)
//     fprintf(logfile, "%d - %d.\n", bytecount1,read_buf[bytecount1]);
    fflush(logfile);
   }
#endif
   *buf_index=0;
   if (ei_decode_version(read_buf, buf_index, &int_value1)!=0) {
    write_tuple(SE_ERROR, "Version decoding error", 22, 22);
   } else {
#ifdef WITHDEBUG
    if (logfile != NULL){
     fprintf(logfile,"Decoded version is: \"%d\"\n",int_value1);
     fflush(logfile);
    }
#endif
    if (ei_decode_tuple_header(read_buf, buf_index, &int_value1)!=0) {
     write_tuple(SE_ERROR, "Tuple header decoding error", 27, 27);
    } else {
#ifdef WITHDEBUG
     if (logfile != NULL){
      fprintf(logfile,"Decoded tuple arity is %d\n",int_value1);
      fflush(logfile);
     }
#endif
     // Now check that first element in the tuple is atom and it's length does not
     // exceed maximum command name length.
     if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
      if (logfile != NULL){
       fprintf(logfile,"Get encoded type&size for command name failed\n");
       fflush(logfile);
      }
#endif
      write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
     } else {
      // int_value2 is type, int_value3 is size
      if (int_value2!=ERL_ATOM_EXT || int_value3>COMMANDNAME_MAXLENGTH-1 ) {
       // COMMANDNAME_MAXLENGTH includes extra byte for trailing NULL:)
#ifdef WITHDEBUG
       if (logfile != NULL){
        fprintf(logfile,"Command name not atom or too long\n");
        fflush(logfile);
       }
#endif
       write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
      } else {
       // There may be two approaches to allocating memory:
       // - allocate it only once per cycle with size equal to packet
       //   length or even once at program start with size equal to maximum
       //   packet length (if we know our use case and we are confident
       //   in advance that maximum packet length will not exceed certain
       //   reasonable limit). This makes program to run faster but also
       //   limits it's ability to process arbitrary input - it can drop too
       //   long packets or even crash.
       // - check the size of every variable encoded in the packet and
       //   allocate every time just enough memory to accomodate it, free
       //   it when it is not needed. This takes some extra CPU cycles but
       //   eliminates chance to run out of allocated area (provided that
       //   data on input are a well-formed serialized term).
       // For command name we use pre-allocated string because it is small
       // enough to make extra code for allocating/zeroing/deallocating
       // worthless.
       if (ei_decode_atom(read_buf, buf_index, short_buffer)!=0) {
        write_tuple(SE_ERROR, "Sedna command decoding error", 28, 28);
       } else {
#ifdef WITHDEBUG
        if (logfile != NULL){
         fprintf(logfile,"Decoded sedna command is: \"%s\"\n",short_buffer);
         fflush(logfile);
        }
#endif
        int_value3=NUM_COMMANDS+1;
        for (int_value2=0;int_value2<NUM_COMMANDS;int_value2++) {
         if (strncmp(short_buffer,sedna_commands[int_value2],COMMANDNAME_MAXLENGTH)==0) {
          int_value3=int_value2;
          break;
         }
        }
        bzero(short_buffer,SHORTBUFFER_LENGTH);
        if (int_value3==NUM_COMMANDS+1) {
         write_tuple(SE_ERROR, "Invalid command", 15, 15);
        } else {

         switch (int_value3) {
          case Num_SEgetLastErrorMsg :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEgetLastErrorMsg", 31, 31);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "Not connected", 13, 13);
            } else {
             tmp_ptr1=(char*)SEgetLastErrorMsg(&connection);
             // If length of error message exceeds MAX_OUTPUT_PAYLOAD_LENGTH, then
             // only the first MAX_OUTPUT_PAYLOAD_LENGTH bytes will be returned, so
             // it still retains some usefulness for diagnostics.
             // I think it is better then complaining about too big error message
             // and losing its contents.
             int_value1=strnlen(tmp_ptr1,MAX_OUTPUT_PAYLOAD_LENGTH);
             write_tuple(SE_OK, tmp_ptr1, int_value1, int_value1);
             tmp_ptr1=NULL;
             // ToDo: find out if tmp_ptr1 needs to be free'd
            } // connected
           }; // right tuple arity
           break;

          case Num_SEgetLastErrorCode :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEgetLastErrorCode", 32, 32);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "No connection", 14, 14);
            } else {
             int_value1=SEgetLastErrorCode(&connection);
             // See explanation preceding write_tuple() definition about using
             // sprintf() here.
             // short_buffer is sufficiently big to accomodate decimal
             // representation of integer - no extra checks needed.
             sprintf(short_buffer,"%d",int_value1);
             int_value1=strlen(short_buffer);
             write_tuple(SE_OK, short_buffer, int_value1, int_value1);
            } // connected
           }; // right tuple arity
           break;

          case Num_SEconnect :
           if (int_value1 != 5){
            write_tuple(SE_ERROR, "Bad arity for SEconnect", 23, 23);
           } else {
            if (connection.socket!=invalid_socket){
             write_tuple(SE_ERROR, "SEconnect: already connected", 28, 28);
            } else {
             if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
              if (logfile != NULL){
               fprintf(logfile,"Get type&size for Server URL failed\n");
               fflush(logfile);
              }
#endif
              write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
             } else {
             // int_value2 is type, int_value3 is size
              if (int_value2!=ERL_BINARY_EXT || int_value3>SE_HOSTNAMELENGTH-1 ) {
#ifdef WITHDEBUG
               if (logfile != NULL){
                fprintf(logfile,"Server URL not binary or too long\n");
                fflush(logfile);
               }
#endif
               write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
              } else {
               tmp_ptr1=(char*)malloc(int_value3+1);
               bzero(tmp_ptr1,int_value3+1); // No need, but let's do it :)
               if (ei_decode_binary(read_buf, buf_index, tmp_ptr1, &long_value1)!=0){
                write_tuple(SE_ERROR, "Server URL decode failed", 24, 24);
               } else {
                if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
                 if (logfile != NULL){
                  fprintf(logfile,"Get type&size for DB name failed\n");
                  fflush(logfile);
                 }
#endif
                 write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
                } else {
                // int_value2 is type, int_value3 is size
                 if (int_value2!=ERL_BINARY_EXT || int_value3>SE_MAX_DB_NAME_LENGTH-1 ) {
#ifdef WITHDEBUG
                  if (logfile != NULL){
                   fprintf(logfile,"DB name not binary or too long\n");
                   fflush(logfile);
                  }
#endif
                  write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
                 } else {
                  tmp_ptr2=(char*)malloc(int_value3+1);
                  bzero(tmp_ptr2,int_value3+1);
                  if (ei_decode_binary(read_buf, buf_index, tmp_ptr2, &long_value1)!=0){
                   write_tuple(SE_ERROR, "DB name decode failed", 21, 21);
                  } else {
                   if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
                    if (logfile != NULL){
                     fprintf(logfile,"Get type&size for login failed\n");
                     fflush(logfile);
                    }
#endif
                    write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
                   } else {
                   // int_value2 is type, int_value3 is size
                    if (int_value2!=ERL_BINARY_EXT || int_value3>SE_MAX_LOGIN_LENGTH-1 ) {
#ifdef WITHDEBUG
                     if (logfile != NULL){
                      fprintf(logfile,"Login not binary or too long\n");
                      fflush(logfile);
                     }
#endif
                     write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
                    } else {
//                     tmp_ptr3=(char*)malloc(bytecount4);
//                     bzero(tmp_ptr3,bytecount4);
                     tmp_ptr3=(char*)malloc(int_value3+1);
                     bzero(tmp_ptr3,int_value3+1);
                     if (ei_decode_binary(read_buf, buf_index, tmp_ptr3, &long_value1)!=0){
                      write_tuple(SE_ERROR, "Login decode failed", 19, 19);
                     } else {
                      if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
                       if (logfile != NULL){
                        fprintf(logfile,"Get type&size for password failed\n");
                        fflush(logfile);
                       }
#endif
                       write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
                      } else {
                      // int_value2 is type, int_value3 is size
                       if (int_value2!=ERL_BINARY_EXT || int_value3>SE_MAX_PASSWORD_LENGTH-1 ) {
#ifdef WITHDEBUG
                        if (logfile != NULL){
                         fprintf(logfile,"Password not binary or too long\n");
                         fflush(logfile);
                        }
#endif
                        write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
                       } else {
//                        tmp_ptr4=(char*)malloc(bytecount4);
//                        bzero(tmp_ptr4,bytecount4);
                        tmp_ptr4=(char*)malloc(int_value3+1);
                        bzero(tmp_ptr4,int_value3+1);
                        if (ei_decode_binary(read_buf, buf_index, tmp_ptr4, &long_value1)!=0){
                         write_tuple(SE_ERROR, "Password decode failed", 22, 22);
                        } else {
                         int_value2=SEconnect(&connection, tmp_ptr1, tmp_ptr2, tmp_ptr3, tmp_ptr4);
                         switch (int_value2) {
                          case SEDNA_SESSION_OPEN :
                           write_tuple(SE_OK, "Connected", 9, 9);
                           break;
                          case SEDNA_AUTHENTICATION_FAILED :
                           write_tuple(SE_ERROR, "Auth failed", 11, 11);
                           break;
                          case SEDNA_OPEN_SESSION_FAILED :
                           write_tuple(SE_ERROR, "Open session failed", 19, 19);
                           break;
                          // May be, leave "SEDNA_ERROR" (some other unspecified error) to be
                          // default?
                          case SEDNA_ERROR :
                           write_tuple(SE_ERROR, "Unspecified error", 17, 17);
                           break;
                          default:
                           write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                         }; //switch
                        } // password decoded ok
                        free(tmp_ptr4);
                       } // password type&size are correct
                      } // password type&size obtained
                     } // login decoded ok
                    free(tmp_ptr3);
                    } // login type&size are correct
                   } // login type&size obtained
                  } // DB name decoded ok
                  free(tmp_ptr2);
                 } // DB name type&size are correct
                } // DB name type&size obtained
               } // tmp_ptr1 decoded ok
               free(tmp_ptr1);
              } // server URL type&size are correct
             } // server URL type&size obtained
            } // not connected yet
           }; // right tuple arity
           break;

          case Num_SEclose :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEclose", 21, 21);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEclose: not connected", 22, 22);
             // Though this is the desired state, return error if connection
             // is already closed.
            } else {
             int_value2=SEclose(&connection);
             switch (int_value2) {
              case SEDNA_SESSION_CLOSED:
               write_tuple(SE_OK, "Closed", 6, 6);
               break;
              case SEDNA_CLOSE_SESSION_FAILED:
               write_tuple(SE_ERROR, "Close session failed", 20, 20);
               break;
              case SEDNA_ERROR:
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch return value
            } // connected
           } // right tuple arity
           break;

          case Num_SEsetConnectionAttr :
           if (int_value1 != 3){
            write_tuple(SE_ERROR, "bad arity for SEsetConnectionAttr", 33, 33);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEsetConnectionAttr: not connected", 34, 34);
            } else {
             if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
              if (logfile != NULL){
               fprintf(logfile,"Get type&size for attr name failed\n");
               fflush(logfile);
              }
#endif
              write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
             } else {
             // int_value2 is type, int_value3 is size
              if (int_value2!=ERL_BINARY_EXT || int_value3>ATTRNAME_MAXLENGTH-1 ) {
#ifdef WITHDEBUG
               if (logfile != NULL){
                fprintf(logfile,"Attr name not binary or too long\n");
                fflush(logfile);
               }
#endif
               write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
              } else {
               if (ei_decode_binary(read_buf, buf_index, short_buffer, &long_value1)!=0){
#ifdef WITHDEBUG
                if (logfile != NULL) {
                 fprintf(logfile,"ATTR name decode failed\n");
                 fflush(logfile);
                }
#endif
                write_tuple(SE_ERROR, "Attr name decode failed", 23, 23);
                //free(short_buffer);
               } else {
#ifdef WITHDEBUG
                if (logfile != NULL) {
                 fprintf(logfile,"ATTR name decoded: %s\n",short_buffer);
                 fflush(logfile);
                }
#endif
                // Hope, reassigning int_value1 here is safe - "break" at the end
                // of this "case" will prevent from matchig it against remaining
                // command name numbers.
                int_value2=NUM_ATTRS+1;
                for (int_value1=0;int_value1<NUM_ATTRS;int_value1++) {
#ifdef WITHDEBUG
                 if (logfile!=NULL) {
                  fprintf(logfile,"Comparing: \"%s\" vs \"%s\"\n", tmp_ptr1,sedna_attrs[int_value1]);
                  fflush(logfile);
                 }
#endif
//                 if (strncmp(tmp_ptr1,sedna_attrs[int_value1],ATTRNAME_MAXLENGTH)==0) {
                 if (strncmp(short_buffer,sedna_attrs[int_value1],ATTRNAME_MAXLENGTH)==0) {
                  int_value2=int_value1;
                  break;
                 }
                }
                if (int_value2==NUM_ATTRS+1) {
#ifdef WITHDEBUG
                 if (logfile!=NULL) {
                  fprintf(logfile,"Invalid attr name: %s\n", short_buffer);
                  fflush(logfile);
                 }
#endif
                 write_tuple(SE_ERROR, "Bad attr name", 13, 13);
                } else {
#ifdef WITHDEBUG
                 if (logfile != NULL) {
                  fprintf(logfile,"Valid attr name \"%s\"\n",sedna_attrs[int_value2]);
                  fflush(logfile);
                 }
#endif
                 int_value1=0; // We'll assign return value of SEsetConnectionAttr
                               // to int_value1. If attr value will be decoded
                               // successfully and SEsetConnectionAttr will be
                               // invoked, int_value1 will be assigned value either
                               // SEDNA_SET_ATTRIBUTE_SUCCEEDED (32) or
                               // SEDNA_ERROR (24)
                               // so we can move switch(int_value1) outside
                               // the switch on attr types

                 if(ei_get_type(read_buf, buf_index, &int_value3, &int_value4)!=0) {
#ifdef WITHDEBUG
                  if (logfile != NULL){
                   fprintf(logfile,"Get type&size for attr value failed\n");
                   fflush(logfile);
                  }
#endif
                  write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
                 } else {
                 // int_value3 is type, int_value4 is size
                  if (int_value3!=ERL_BINARY_EXT) {
#ifdef WITHDEBUG
                   if (logfile != NULL){
                    fprintf(logfile,"Attr value not binary\n");
                    fflush(logfile);
                   }
#endif
                   write_tuple(SE_ERROR, "Bad encoded type", 16, 16);
                  } else {
                   bzero(short_buffer,SHORTBUFFER_LENGTH);
                   // All attr values are sent as binary strings (even
                   // numeric ones). We will decode them separately and
                   // specifically for each attribyte.
                   // Note that attribute value is already checked to be
                   // a binary
                   switch (int_value2) {
                    case SEDNA_ATTR_AUTOCOMMIT:
                     // values are: OFF (30), ON (31)
                    case SEDNA_ATTR_DEBUG:
                     // values are: OFF (0), ON (1)
                    case SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD:
                     // values are: OFF (35), ON (36)
                    case SEDNA_ATTR_CDATA_PRESERVE_WHILE_LOAD:
                     // values are: OFF (37), ON (38)
                     if (int_value4!=2 && int_value4!=3) {
#ifdef WITHDEBUG
                      if (logfile != NULL){
                       fprintf(logfile,"Bad attr value size, must be 2 (ON) or 3 (OFF)\n");
                       fflush(logfile);
                      }
#endif
                      write_tuple(SE_ERROR, "Bad encoded size", 16, 16);
                     } else {
                      if (ei_decode_binary(read_buf, buf_index, short_buffer, &long_value1)!=0) {
                       write_tuple(SE_ERROR, "Attr value decode failed", 24, 24);
                      } else {
#ifdef WITHDEBUG
                       if (logfile != NULL) {
                        fprintf(logfile,"Attr value decoded\"%s\"\n",short_buffer);
                        fflush(logfile);
                       }
#endif
                       if (strncmp(short_buffer,"OFF",4)==0) {
                        int_value3=attr_off[int_value2];
                        int_value1=SEsetConnectionAttr(&connection, int_value2, &int_value3, sizeof(int));
                       } else if (strncmp(short_buffer,"ON",3)==0) {
                        int_value3=attr_on[int_value2];
                        int_value1=SEsetConnectionAttr(&connection, int_value2, &int_value3, sizeof(int));
                       } else {
                        write_tuple(SE_ERROR, "Bad value for attr", 18, 18);
                       } // correct attr value
                      } // attr value decoded
                     } // size of attr value correct
                     break;
                    case SEDNA_ATTR_SESSION_DIRECTORY:
                     // value is path (arbitrary string)
                     tmp_ptr1=(char*)malloc(int_value4);
                     if (ei_decode_binary(read_buf, buf_index, tmp_ptr1, &long_value1)!=0) {
                      write_tuple(SE_ERROR, "Attr value decode failed", 24, 24);
                     } else {
#ifdef WITHDEBUG
                      if (logfile != NULL) {
                       fprintf(logfile,"Attr value decoded\"%s\"\n",tmp_ptr1);
                       fflush(logfile);
                      }
#endif
                     } // attr value decoded
                     int_value1=SEsetConnectionAttr(&connection, int_value2, tmp_ptr1, long_value1);
                     break;
                    case SEDNA_ATTR_CONCURRENCY_TYPE:
                     // values are: READONLY (2), UPDATE (3)
                     // Length of tmp_ptr1 is 1+2+5+27+5+n=40+n
                     // (header+tuple+binary+"SEDNA_ATTR_CONCURRENCY_TYPE"+binary+value)
                     // which is >9, and tmp_ptr1 was bzero'ed, so we are not at risk
                     // to run out of boundaries when strncmp'ing it against
                     // "READONLY" or "UPDATE" (even if attr value was empty string).
                     if (int_value4!=6 && int_value4!=8) {
#ifdef WITHDEBUG
                      if (logfile != NULL){
                       fprintf(logfile,"Bad attr value size, must be 8 (READONLY) or 6 (UPDATE)\n");
                       fflush(logfile);
                      }
#endif
                      write_tuple(SE_ERROR, "Bad encoded size", 16, 16);
                     } else {
                      if (ei_decode_binary(read_buf, buf_index, short_buffer, &long_value1)!=0) {
                       write_tuple(SE_ERROR, "Attr value decode failed", 24, 24);
                      } else {
#ifdef WITHDEBUG
                       if (logfile != NULL) {
                        fprintf(logfile,"Attr value decoded\"%s\"\n",short_buffer);
                        fflush(logfile);
                       }
#endif
                       if (strncmp(short_buffer,"READONLY",9)==0) {
                        int_value3=SEDNA_READONLY_TRANSACTION;
                        int_value1=SEsetConnectionAttr(&connection, int_value2, &int_value3, sizeof(int));
                       } else if (strncmp(short_buffer,"UPDATE",7)==0) {
                        int_value3=SEDNA_UPDATE_TRANSACTION;
                        int_value1=SEsetConnectionAttr(&connection, int_value2, &int_value3, sizeof(int));
                       } else {
                        write_tuple(SE_ERROR, "Bad value for attr", 18, 18);
                       }
                      } // attr value decoded
                      bzero(short_buffer,SHORTBUFFER_LENGTH);
                     } // attr value size correct
                     break;
                    case SEDNA_ATTR_QUERY_EXEC_TIMEOUT:
                     // value is N seconds (int)
                    case SEDNA_ATTR_MAX_RESULT_SIZE:
                     // value is size (int)
                     // Erlang's ei C library can only decode char, long and long long
                     // (and bignum, having GMP installed).
                     // Simple experiment reveals that term_to_binary may convert
                     // integer to char, long, long long, "small bignum" or "large
                     // bignum", based on it's value. So, assuming that we can not
                     // know in advance what type of variable will be found in the
                     // tuple, we either need to move through all options, or
                     // manually check integer at buf_index position (97 for char,
                     // 98 for long, 110 for long long, etc.), or receive the number
                     // as a string and then sscanf() it.
                     // This is just a preliminary check, it's purpose is to
                     // avoid buffer overflow when decoding value.
                     if (int_value4>SHORTBUFFER_LENGTH) {
#ifdef WITHDEBUG
                      if (logfile != NULL){
                       fprintf(logfile,"Bad attr value size, must be shorter than %d\n",INT_MAX);
                       fflush(logfile);
                      }
#endif
                      write_tuple(SE_ERROR, "Bad encoded size", 16, 16);
                     } else {
                      if (ei_decode_binary(read_buf, buf_index, short_buffer, &long_value1)!=0) {
                       write_tuple(SE_ERROR, "Attr value decode failed", 24, 24);
                      } else {
#ifdef WITHDEBUG
                       if (logfile != NULL) {
                        fprintf(logfile,"Attr value decoded\"%s\"\n",short_buffer);
                        fflush(logfile);
                       }
#endif
                       // scan value into unsigned long long. If provided number
                       // is too big, this will help detect it.
                       sscanf(short_buffer,"%llu", &ull_value1);
                       if (ull_value1>INT_MAX) {
#ifdef WITHDEBUG
                        if (logfile != NULL){
                         fprintf(logfile,"Bad attr value %llu, must not exceed %d\n", ull_value1, INT_MAX);
                         fflush(logfile);
                        }
#endif
                        write_tuple(SE_ERROR, "Number too big", 19, 19);
                       } else {
                        int_value3=(int)ull_value1;
                        // Now check if integer decoded correctly
                        tmp_ptr1=(char*)malloc(SHORTBUFFER_LENGTH);
                        // We might use some formula to approximate maximum length
                        // of decimal representation of integer.
                        // Example formula is ((sizeof(int) / 2) * 3 + sizeof(int)) + 2
                        // (src: https://stackoverflow.com/questions/43787672/),
                        // or less accurate sizeof(int)*CHAR_BIT (this exceeds
                        // maximum length of integer by approximately factor of 3).
                        // However, I am not sure if decreasing number of bytes
                        // to be bzero'ed is worth using more complex formula.
                        // Another reason for using fixed value here is that if
                        // SHORTBUFFER_LENGTH is not enough (if this program
                        // will be run on host with 256-bit integer :)), then
                        // definition of SHORTBUFFER_LENGTH needs to be changed
                        // accordingly.
                        bzero(tmp_ptr1,SHORTBUFFER_LENGTH);
                        sprintf(tmp_ptr1,"%d",int_value3);
                        if(strncmp(short_buffer,tmp_ptr1,SHORTBUFFER_LENGTH) !=0 ) {
                         write_tuple(SE_ERROR, "Integer decode failed", 21, 21);
                        } else {
                         int_value1=SEsetConnectionAttr(&connection, int_value2, &int_value3, sizeof(int));
                        } // value is valid integer
                        free(tmp_ptr1);
                        bzero(short_buffer,SHORTBUFFER_LENGTH);
                       } // number not too big
                      } // value decoded
                     } // value length not too big
                     break;
                    case SEDNA_ATTR_LOG_AMOUNT:
                     // values are LESS (7), FULL (8)
                     if (int_value4!=4) {
#ifdef WITHDEBUG
                      if (logfile != NULL){
                       fprintf(logfile,"Bad attr value size, must be 4 (LESS or FULL)\n");
                       fflush(logfile);
                      }
#endif
                      write_tuple(SE_ERROR, "Bad encoded size", 21, 21);
                     } else {
                      if (ei_decode_binary(read_buf, buf_index, short_buffer, &long_value1)!=0) {
                       write_tuple(SE_ERROR, "Attr value decode failed", 24, 24);
                      } else {
#ifdef WITHDEBUG
                       if (logfile != NULL) {
                        fprintf(logfile,"Attr value decoded\"%s\"\n",short_buffer);
                        fflush(logfile);
                       }
#endif
                       if (strncmp(short_buffer,"LESS",5)==0) {
                        int_value3=SEDNA_LOG_LESS;
                        int_value1=SEsetConnectionAttr(&connection, int_value2, &int_value3, sizeof(int));
                       } else if (strncmp(short_buffer,"FULL",5)==0) {
                        int_value3=SEDNA_LOG_FULL;
                        int_value1=SEsetConnectionAttr(&connection, int_value2, &int_value3, sizeof(int));
                       } else {
                        write_tuple(SE_ERROR, "Bad value for attr", 18, 18);
                       } // values of attr value
                      } // attr value decoded
                     } // attr value length is 4
                     break;
                    default:
#ifdef WITHDEBUG
                     if (logfile != NULL) {
                      fprintf(logfile,"Invalid attr name\n");
                      fflush(logfile);
                     }
#endif
                     write_tuple(SE_ERROR, "Invalid attr name", 17, 17);
                   } // switch on attribute name index
                  } // attr value is a binary
                 } // attribute value type&size obtained
                 if (int_value1==SEDNA_SET_ATTRIBUTE_SUCCEEDED) {
                  write_tuple(SE_OK, "Attr set succeeded", 18, 18);
                 } else if (int_value1==SEDNA_ERROR) {
                  write_tuple(SE_ERROR, "Attr set failed", 15, 15);
                 } // if (int_value == ??? )
                } // if attr name recognized
                bzero(short_buffer,SHORTBUFFER_LENGTH);
               } // attr name decoded ok
              } // attribute name type&size correct
             } // attribute name type&size obtained
            } // connected
           }; // right tuple arity
           break;

          case Num_SEgetConnectionAttr :
           if (int_value1 != 2){
            write_tuple(SE_ERROR, "Bad arity for SEgetConnectionAttr", 33, 33);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEgetConnectionAttr: not connected", 34, 34);
            } else {
             if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
              if (logfile != NULL){
               fprintf(logfile,"Get type&size for attr name failed\n");
               fflush(logfile);
              }
#endif
              write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
             } else {
             // int_value2 is type, int_value3 is size
              if (int_value2!=ERL_BINARY_EXT || int_value3>ATTRNAME_MAXLENGTH-1 ) {
#ifdef WITHDEBUG
               if (logfile != NULL){
                fprintf(logfile,"Attr name not binary or too long\n");
                fflush(logfile);
               }
#endif
               write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
              } else {
               if (ei_decode_binary(read_buf, buf_index, short_buffer, &long_value1)!=0){
                write_tuple(SE_ERROR, "Attr name decode failed", 23, 23);
                //free(tmp_ptr1);
               } else {
#ifdef WITHDEBUG
                if (logfile != NULL) {
                 fprintf(logfile,"Attr name decoded: %s\n",short_buffer);
                 fflush(logfile);
                }
#endif
                // Hope, reassigning int_value1 here is safe - "break" at the end
                // of this "case" will prevent from matchig it against remaining
                // command name numbers.
                int_value2=NUM_ATTRS+1;
                for (int_value1=0;int_value1<NUM_ATTRS;int_value1++) {
#ifdef WITHDEBUG
                 if (logfile != NULL) {
                  fprintf(logfile,"Comparing \"%s\" vs \"%s\"\n",short_buffer,sedna_attrs[int_value1]);
                  fflush(logfile);
                 }
#endif
                 if (strncmp(short_buffer,sedna_attrs[int_value1],ATTRNAME_MAXLENGTH)==0) {
                  int_value2=int_value1;
                  break;
                 }
                }
                bzero(short_buffer,SHORTBUFFER_LENGTH);
                if (int_value2==NUM_ATTRS+1) {
#ifdef WITHDEBUG
                 if (logfile!=NULL) {
                  fprintf(logfile,"Invalid attr name: %s\n", short_buffer);
                  fflush(logfile);
                 }
#endif
                 write_tuple(SE_ERROR, "Bad attr name", 13, 13);
                } else {
                 int_value1=0;
                 switch (int_value2) {
                  // Attributes whose values are integer codes
                  case SEDNA_ATTR_AUTOCOMMIT:
                   // values are: OFF (30), ON (31)
                  case SEDNA_ATTR_DEBUG:
                   // values are: OFF (0), ON (1)
                  case SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD:
                   // values are: OFF (35), ON (36)
                  case SEDNA_ATTR_CDATA_PRESERVE_WHILE_LOAD:
                   // values are: OFF (37), ON (38)
                  case SEDNA_ATTR_CONCURRENCY_TYPE:
                   // values are: READONLY (2), UPDATE (3)
                  case SEDNA_ATTR_LOG_AMOUNT:
                   // values are LESS (7), FULL (8)
#ifdef WITHDEBUG
                   if (logfile != NULL) {
                    fprintf(logfile,"Valid attr name (#%i)\n",int_value2);
                    fflush(logfile);
                   }
#endif
                   int_value1=SEgetConnectionAttr(&connection, int_value2, &int_value3, &int_value4);
                   switch (int_value1) {
                    case SEDNA_GET_ATTRIBUTE_SUCCEEDED:
                     switch (int_value3) {
                      case SEDNA_AUTOCOMMIT_OFF:
                      case SEDNA_DEBUG_OFF:
                      case SEDNA_BOUNDARY_SPACE_PRESERVE_OFF:
                      case SEDNA_CDATA_PRESERVE_OFF:
                       write_tuple(SE_OK, "OFF", 3, 3);
                       break;
                      case SEDNA_AUTOCOMMIT_ON:
                      case SEDNA_DEBUG_ON:
                      case SEDNA_BOUNDARY_SPACE_PRESERVE_ON:
                      case SEDNA_CDATA_PRESERVE_ON:
                       write_tuple(SE_OK, "ON", 2, 2);
                       break;
                      case SEDNA_READONLY_TRANSACTION:
                       write_tuple(SE_OK, "READONLY", 8, 8);
                       break;
                      case SEDNA_UPDATE_TRANSACTION:
                       write_tuple(SE_OK, "UPDATE", 6, 6);
                       break;
                      case SEDNA_LOG_LESS:
                       write_tuple(SE_OK, "LESS", 4, 4);
                       break;
                      case SEDNA_LOG_FULL:
                       write_tuple(SE_OK, "FULL", 4, 4);
                       break;
                      default:
                       write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                     }
                     break;
                    case SEDNA_ERROR:
                     write_tuple(SE_ERROR, "Unspecified error", 17, 17);
                     break;
                    default:
                     write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                   }
                   break;
                  // Attributes whose values are numbers
                  case SEDNA_ATTR_QUERY_EXEC_TIMEOUT:
                  // number of seconds
                  case SEDNA_ATTR_MAX_RESULT_SIZE:
                  // number of bytes
#ifdef WITHDEBUG
                   if (logfile != NULL) {
                    fprintf(logfile,"Valid attr name (#%i)\n",int_value2);
                    fflush(logfile);
                   }
#endif
                   int_value1=SEgetConnectionAttr(&connection, int_value2, &int_value3, &int_value4);
                   switch (int_value1) {
                    case SEDNA_GET_ATTRIBUTE_SUCCEEDED:
                     sprintf(short_buffer,"%d",int_value3);
                     int_value3=strlen(short_buffer);
                     write_tuple(SE_OK, short_buffer,  int_value3, int_value3);
                     bzero(short_buffer,int_value3);
                     break;
                    case SEDNA_ERROR:
                     write_tuple(SE_ERROR, "Unspecified error", 17, 17);
                     break;
                    default:
                     write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                   }
                   break;
                  case SEDNA_ATTR_SESSION_DIRECTORY:
                  // path (string)
                  // afaik, there's no limit for maximum path length in ext2/3/4
                  // and UFS2, but in Sedna max workdir path length is limited
                  // by SE_MAX_DIR_LENGTH. At present (commit 4b01892 of 2017.03.26)
                  // it is set to 255, but we should be ready to correctly process
                  // it if this value will be increased.
                   tmp_ptr1=(char*)malloc(SE_MAX_DIR_LENGTH+1);
                   bzero(tmp_ptr1,SE_MAX_DIR_LENGTH+1);
                   int_value1=SEgetConnectionAttr(&connection,int_value2,tmp_ptr1,&int_value4);
                   switch (int_value1) {
                    case SEDNA_GET_ATTRIBUTE_SUCCEEDED:
                     // strnlen(tmp_ptr1,...) should be equal to int_value4
                     if (strnlen(tmp_ptr1,MAX_OUTPUT_PAYLOAD_LENGTH+1)>MAX_OUTPUT_PAYLOAD_LENGTH) {
                     // Unlike SEgetLastErrorMsg we can not return part of return value here.
                     // Either return full path, or report error.
                      write_tuple(SE_ERROR, "Value too long", 14,14);
                     } else {
                      int_value3=strlen(tmp_ptr1);
                      write_tuple(SE_OK, tmp_ptr1, int_value3, int_value3);
                     }
                     break;
                    case SEDNA_ERROR:
                     write_tuple(SE_ERROR, "Unspecified error", 17, 17);
                     break;
                    default:
                     write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                   }
                   free(tmp_ptr1);
                   break;
                  default:
                   // This should not happen
                   write_tuple(SE_ERROR, "Unknown attr", 12, 12);
                 } // switch attr name index
                } // valid attr name
               } // attr name decoded
              } // attr name type&length ok
             } // attr name type&length obtained
            } //connected
           } //right arity
           break;

          case Num_SEresetAllConnectionAttr :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEresetAllConnectionAttr", 38, 38);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEresetAllConnectionAttr: not connected", 39, 39);
            } else {
             int_value1=SEresetAllConnectionAttr(&connection);
             switch (int_value1) {
              case SEDNA_RESET_ATTRIBUTES_SUCCEEDED:
               write_tuple(SE_OK, "Attrs reset", 11, 11);
               break;
              case SEDNA_ERROR:
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch on return value
            } // connected
           } // right tuple arity
           break;

          case Num_SEbegin:
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "bad arity for SEbegin", 21, 21);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEbegin: not connected", 22, 22);
            } else {
             int_value1=SEbegin(&connection);
             switch (int_value1) {
              case SEDNA_BEGIN_TRANSACTION_SUCCEEDED:
               write_tuple(SE_OK, "Begin transaction succeeded", 27, 27);
               break;
              case SEDNA_BEGIN_TRANSACTION_FAILED:
               write_tuple(SE_ERROR, "Begin transaction failed", 24, 24);
               break;
              case SEDNA_ERROR:
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch on return value
            } // connected
           } // right tuple arity
           break;

          case Num_SEcommit:
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEcommit", 22, 22);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEcommit: not connected", 23, 23);
            } else {
             int_value1=SEcommit(&connection);
             switch (int_value1) {
              case SEDNA_COMMIT_TRANSACTION_SUCCEEDED:
               write_tuple(SE_OK, "Commit transaction succeeded", 28, 28);
               break;
              case SEDNA_COMMIT_TRANSACTION_FAILED:
               write_tuple(SE_ERROR, "Commit transaction failed", 25, 25);
               break;
              case SEDNA_ERROR:
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch on return value
            } // connected
           } // right tuple arity
           break;

          case Num_SErollback:
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SErollback", 24, 24);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SErollback: not connected", 25, 25);
            } else {
             int_value1=SErollback(&connection);
             switch (int_value1) {
              case SEDNA_ROLLBACK_TRANSACTION_SUCCEEDED:
               write_tuple(SE_OK, "Rollback transaction succeeded", 30, 30);
               break;
              case SEDNA_ROLLBACK_TRANSACTION_FAILED:
               write_tuple(SE_ERROR, "Rollback transaction failed", 27, 27);
               break;
              case SEDNA_ERROR:
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch on return value
            } // connected
           } // right tuple arity
           break;

          case Num_SEconnectionStatus :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEconnectionStatus", 32, 32);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEconnectionStatus: not connected", 33, 33);
            } else {
             int_value1=SEconnectionStatus(&connection);
             switch (int_value1) {
              case SEDNA_CONNECTION_OK:
               write_tuple(SE_OK, "Connection open", 15, 15);
               break;
              case SEDNA_CONNECTION_CLOSED:
               write_tuple(SE_OK, "Connection closed", 17, 17);
               break;
              case SEDNA_CONNECTION_FAILED:
               write_tuple(SE_ERROR, "unspecified error", 17, 17);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch on return value
            } // connected
           } // right tuple arity
           break;

          case Num_SEtransactionStatus :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEtransactionStatus", 33, 33);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEtransactionStatus: not connected", 34, 34);
            } else {
             int_value1=SEtransactionStatus(&connection);
             switch (int_value1) {
              case SEDNA_TRANSACTION_ACTIVE :
               write_tuple(SE_OK, "Transaction is running", 22, 22);
               break;
              case SEDNA_NO_TRANSACTION:
               write_tuple(SE_OK, "No transaction running", 22, 22);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch on return value
            } // connected
           } // right tuple arity
           break;

          case Num_SEexecute :
           if (int_value1 != 2){
            write_tuple(SE_ERROR, "Bad arity for SEexecute", 23, 23);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEexecute: not connected", 24, 24);
            } else {
             if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
              if (logfile != NULL){
               fprintf(logfile,"Get type&size for query body failed\n");
               fflush(logfile);
              }
#endif
              write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
             } else {
             // int_value2 is type, int_value3 is size
              if (int_value2!=ERL_BINARY_EXT || int_value3>SE_MAX_QUERY_SIZE-1 ) {
#ifdef WITHDEBUG
               if (logfile != NULL){
                fprintf(logfile,"Query not binary or too long\n");
                fflush(logfile);
               }
#endif
               write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
              } else {
               tmp_ptr1=(char*)malloc(int_value3+1);
               bzero(tmp_ptr1,int_value3+1);
               if (ei_decode_binary(read_buf, buf_index, tmp_ptr1, &long_value1)!=0){
                write_tuple(SE_ERROR, "Query decode failed", 19, 19);
               } else {
                // Do we need this check? I am not sure.
                if (long_value1>SE_MAX_QUERY_SIZE || strlen(tmp_ptr1)>SE_MAX_QUERY_SIZE) {
                 // long_value1 and strlen(tmp_ptr1) should be equal.
                 // ToDo: when the program will prove to work as intended, add
                 // printing them to log to verify they are always the same.
                 // Once it is proved - remove one of these checks (and walk
                 // through thecode to replace strlen() with long_value1
#ifdef WITHDEBUG
                 if (logfile != NULL) {
                  fprintf(logfile,"Query exceeds SE_MAX_QUERY_SIZE\n");
                  fflush(logfile);
                 }
#endif
                 write_tuple(SE_ERROR,"Query too long", 14, 14);
                } else {
                 int_value1=SEexecute(&connection,tmp_ptr1);
                 switch (int_value1) {
                  case SEDNA_QUERY_SUCCEEDED:
                   write_tuple(SE_OK, "Query succeeded", 15, 15);
                   break;
                  case SEDNA_UPDATE_SUCCEEDED:
                   write_tuple(SE_OK, "Update succeeded", 16, 16);
                   break;
                  case SEDNA_BULK_LOAD_SUCCEEDED:
                   write_tuple(SE_OK, "Bulk load succeeded", 19, 19);
                   break;
                  case SEDNA_QUERY_FAILED:
                   write_tuple(SE_ERROR, "Query failed", 12, 12);
                   break;
                  case SEDNA_UPDATE_FAILED:
                   write_tuple(SE_ERROR, "Update failed", 13, 13);
                   break;
                  case SEDNA_BULK_LOAD_FAILED:
                   write_tuple(SE_ERROR, "Bulk load failed", 16, 16);
                   break;
                  case SEDNA_ERROR:
                   write_tuple(SE_ERROR, "Unspecified error", 17, 17);
                   break;
                  default:
                   write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                 } // switch on return value
                } // query size ok
               } // query decoded
               free(tmp_ptr1);
              } // query type&size correct
             } // query type&size obtained
            } // connected
           } // right tuple arity
           break;

          case Num_SEexecuteLong :
           if (int_value1 != 2){
            write_tuple(SE_ERROR, "Bad arity for SEexecuteLong", 27, 27);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEexecuteLong: not connected", 24, 24);
            } else {
             if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
              if (logfile != NULL){
               fprintf(logfile,"Get type&size for path failed\n");
               fflush(logfile);
              }
#endif
              write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
             } else {
             // int_value2 is type, int_value3 is size
              if (int_value2!=ERL_BINARY_EXT) {
#ifdef WITHDEBUG
               if (logfile != NULL){
                fprintf(logfile,"Path not binary\n");
                fflush(logfile);
               }
#endif
               write_tuple(SE_ERROR, "Bad encoded type", 16, 16);
              } else {
               tmp_ptr1=(char*)malloc(int_value3+1);
               bzero(tmp_ptr1,int_value3+1);
               if (ei_decode_binary(read_buf, buf_index, tmp_ptr1, &long_value1)!=0){
                write_tuple(SE_ERROR, "Path decode failed", 18, 18);
               } else {
                int_value1=SEexecuteLong(&connection,tmp_ptr1);
                switch (int_value1) {
                 case SEDNA_QUERY_SUCCEEDED:
                  write_tuple(SE_OK, "Query succeeded", 15, 15);
                  break;
                 case SEDNA_UPDATE_SUCCEEDED:
                  write_tuple(SE_OK, "Update succeeded", 16, 16);
                  break;
                 case SEDNA_BULK_LOAD_SUCCEEDED:
                  write_tuple(SE_OK, "Bulk load succeeded", 19, 19);
                  break;
                 case SEDNA_QUERY_FAILED:
                  write_tuple(SE_ERROR, "Query failed", 12, 12);
                  break;
                 case SEDNA_UPDATE_FAILED:
                  write_tuple(SE_ERROR, "Update failed", 13, 13);
                  break;
                 case SEDNA_BULK_LOAD_FAILED:
                  write_tuple(SE_ERROR, "Bulk load failed", 16, 16);
                  break;
                 case SEDNA_ERROR:
                  write_tuple(SE_ERROR, "Unspecified error", 17, 17);
                  break;
                 default:
                  write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                } // switch on return value
               } // path decoded
               free(tmp_ptr1);
              } // path is binary
             } // path type&size obtained
            } // connected
           } // right tuple arity
           break;

          case Num_SEnext :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEnext", 20, 20);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEnext: not connected", 21, 21);
            } else {
             int_value1=SEnext(&connection);
             switch (int_value1) {
              case SEDNA_NEXT_ITEM_SUCCEEDED:
               write_tuple(SE_OK, "Next succeeded", 14, 14);
               break;
              case SEDNA_NEXT_ITEM_FAILED:
               write_tuple(SE_ERROR, "Next failed", 11, 11);
               break;
              case SEDNA_RESULT_END:
               write_tuple(SE_ERROR, "End reached", 11, 11);
               break;
              case SEDNA_NO_ITEM:
               write_tuple(SE_ERROR, "No item", 7, 7);
               break;
              case SEDNA_ERROR:
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
               break;
//              case :
//               write_tuple(SE_ERROR, "", );
//               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } // switch on return value
            } // connected
           } // right tuple arity
           break;

          case Num_SEgetData :
           if (int_value1 != 1){
            write_tuple(SE_ERROR, "Bad arity for SEgetData", 23, 23);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEgetData: not connected", 24, 24);
            } else {
             first_chunk=(struct data_chunk*)malloc(sizeof(struct data_chunk));
             current_chunk=first_chunk;
             int_value2=0; // Number of chunks successfully read;
             do {
              bzero((*current_chunk).data,DATA_CHUNK_SIZE);
              int_value1=SEgetData(&connection,(*current_chunk).data, (DATA_CHUNK_SIZE-1));
              if (int_value1==(DATA_CHUNK_SIZE-1)) {
               // Full chunk is read - possibly some more data awaiting to be read.
#ifdef WITHDEBUG
               if (logfile!=NULL) {
                fprintf(logfile,"Read full chunk %d bytes:\n%s\nGoing on.\n",int_value1,(*current_chunk).data);
                fflush(logfile);
               }
#endif
               ++int_value2;
               (*current_chunk).next=(struct data_chunk*)malloc(sizeof(struct data_chunk));
               current_chunk=(*current_chunk).next;
              } else if((int_value1>=0) && (int_value1<DATA_CHUNK_SIZE-1)) {
               // Last incomplete chunk was read or no data available
#ifdef WITHDEBUG
               if (logfile!=NULL) {
                fprintf(logfile,"Read less than full chunk %d bytes:\n%s\nStop.",int_value1,(*current_chunk).data);
                fflush(logfile);
               }
#endif
               ++int_value2;
               (*current_chunk).next=NULL;
               current_chunk=NULL;
              } else if (int_value1==SEDNA_ERROR) {
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
              } else {
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
              }
             } while (int_value1==(DATA_CHUNK_SIZE-1));
             if (int_value2>0) {
              // Here is the only case when binarylength != payloadlength in
              // write_tuple arguments.
              int_value3=(DATA_CHUNK_SIZE-1)*(int_value2-1)+int_value1;
              // Send typle with atom and first fragment of the binary.
              // Check if it is a single chunk.
              if ((*first_chunk).next==NULL) {
               // No chunk besides the first one - send the whole data as a solid
               // packet in a single invocation of write_tuple
#ifdef WITHDEBUG
               if (logfile!=NULL) {
                fprintf(logfile, "First chunk: %d, total: %d\n",int_value1,int_value3);
                // These should be equal
                fflush(logfile);
               }
#endif
               // When sending data to the client in reply to SEgetData,
               // Sedna prepends a newline (e.g. \x0A for UNIX-like OSes )
               // in front of all items but the first one. This make every
               // next item to be printed on the new line when they are
               // printed "as is". We do not need this and we will strip that
               // leading newline.
#ifdef WITHDEBUG
                 if (logfile!=NULL) {
                  fprintf(logfile, "conn.first_next=%d; chunk[0]=%d, chunk[1]=%d\n",(int)(connection.first_next),(int)((*first_chunk).data[0]),(int)((*first_chunk).data[1]));
                  fflush(logfile);
                 }
#endif
               if (connection.first_next==0 && (*first_chunk).data[0]=='\n')
                {
#ifdef WITHDEBUG
                 if (logfile!=NULL) {
                  fprintf(logfile, "First chunk of non-first item - skip leading newline\n");
                  fflush(logfile);
                 }
#endif
                 int_value4=sizeof("\n")-1;
                 write_tuple(SE_OK, &((*first_chunk).data[int_value4]), int_value1-int_value4, int_value1-int_value4);
                } else {
                 write_tuple(SE_OK, (*first_chunk).data, int_value1, int_value1);
                }
              } else {
               // 3'rd argument is binarylength - total length of data to be
               // encoded into the packet as binary part of the tuple, 4'th is
               // payloadlength - actual length of the chunk to be sent with
               // tuple header and result atom.
#ifdef WITHDEBUG
               if (logfile!=NULL) {
                fprintf(logfile, "First chunk in sequence: %d, total: %d\n",DATA_CHUNK_SIZE-1,int_value3);
                fflush(logfile);
               }
#endif
               // Strip leading newline, if any - see explanation above
               if (connection.first_next==0 && (*first_chunk).data[0]=='\n')
                {
#ifdef WITHDEBUG
                 if (logfile!=NULL) {
                  fprintf(logfile, "First chunk of non-first item - skip leading newline\n");
                  fflush(logfile);
                 }
#endif
                 int_value4=sizeof("\n")-1;
                 write_tuple(SE_OK, &((*first_chunk).data[int_value4]), int_value3-int_value4, (DATA_CHUNK_SIZE-1)-int_value4);
                } else {
                 write_tuple(SE_OK, (*first_chunk).data, int_value3, DATA_CHUNK_SIZE-1);
                }
               current_chunk=(*first_chunk).next;
//               while (current_chunk!=NULL && (*current_chunk).next) {
               while ( ((*current_chunk).next)!=NULL) {
#ifdef WITHDEBUG
                if (logfile!=NULL) {
                 fprintf(logfile, "Current chunk in sequence: %d, total: %d\n",DATA_CHUNK_SIZE-1,int_value3);
                 fflush(logfile);
                }
#endif
                write_bytes((*current_chunk).data,DATA_CHUNK_SIZE-1);
                current_chunk=(*current_chunk).next;
               }
#ifdef WITHDEBUG
               if (logfile!=NULL) {
                fprintf(logfile, "Last chunk: %d, total: %d\n",int_value1,int_value3);
                // These should be equal
                fflush(logfile);
               }
#endif
               if(int_value1!=0) {
                write_bytes((*current_chunk).data,int_value1);
               }
              }
              // tmp_ptr1 is allocated with length bytecount4.
              // Result migh not fit in tmp_ptr1, so allocating new one.
              // tmp_ptr2=(char*)malloc(int_value2*DATA_CHUNK_SIZE)
             } // if data were read

            } // connected
           } // right tuple arity
           break;

          case Num_SEsetDebugHandler :
           if (int_value1 != 2){
           // SEsetDebugHandler accepts pointer to function as an argument.
           // At present we have only one kind of debug handler - a function
           // that prints debug data to log file.
            write_tuple(SE_ERROR, "Bad arity for SEsetDebugHandler", 31, 31);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEsetDebugHandler: not connected", 32, 32);
            } else {
             if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
              if (logfile != NULL){
               fprintf(logfile,"Get type&size for debug handler failed\n");
               fflush(logfile);
              }
#endif
              write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
             } else {
             // int_value2 is type, int_value3 is size
              if (int_value2!=ERL_BINARY_EXT || !(int_value3==3 || int_value3==4)) {
#ifdef WITHDEBUG
               if (logfile != NULL){
                fprintf(logfile,"Option not binary (%d) or has wrong size (%d)\n",int_value2,int_value3);
                fflush(logfile);
               }
#endif
               write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
              } else {
               tmp_ptr1=(char*)malloc(int_value3+1);
               bzero(tmp_ptr1,int_value3+1);
               if (ei_decode_binary(read_buf, buf_index, tmp_ptr1, &long_value1)!=0){
                write_tuple(SE_ERROR, "Debug handler name decode failed", 32, 32);
               } else {
#ifdef WITHDEBUG
                if (logfile != NULL) {
                 fprintf(logfile,"Debug handler name decoded: %s\n",tmp_ptr1);
                 fflush(logfile);
                }
#endif
                int_value2=NUM_DEBUG_HANDLERS+1;
                for (int_value1=0;int_value1<NUM_DEBUG_HANDLERS;int_value1++) {
#ifdef WITHDEBUG
                if (logfile != NULL) {
                 fprintf(logfile,"Comparing \"%s\" vs \"%s\" %d %d \n",tmp_ptr1,sedna_debug_handlers[int_value1],int_value1,int_value2);
                 fflush(logfile);
                }
#endif
                 if (strncmp(tmp_ptr1,sedna_debug_handlers[int_value1],DEBUG_HANDLER_NAME_MAXLENGTH)==0) {
                  int_value2=int_value1;
                  break;
                 }
                }
//????                bzero(tmp_ptr1,bytecount4);
                if (int_value2==NUM_DEBUG_HANDLERS+1) {
#ifdef WITHDEBUG
                 if (logfile!=NULL) {
                  fprintf(logfile,"Unsupported debug handler requested: %s\n", tmp_ptr1);
                  fflush(logfile);
                 }
#endif
                 write_tuple(SE_ERROR, "Not implemented", 15, 15);
                } else {
                 int_value1=0;
                 switch (int_value2) {
                  case DEBUG_HANDLER_NONE:
                   // Setting debug handler to "none" is intended to turn logging
                   // debug messages off when it is on (and log file pointer is not
                   // NULL). But one may issue command to stop logging when it is
                   // already off, so this, at first sight senseless check should
                   // lead to no-op behaviour.
                   if (logfile!=NULL) {
                    fprintf(logfile,"Stop debug trace logging\n");
                    fflush(logfile);
                    if (argc==1) { // log file was open on demand for debugging
                     fclose(logfile);
                     logfile=NULL;
                    }
                   }
                   SEsetDebugHandler(&connection, NULL);
                   write_tuple(SE_OK, "Debug logging stopped", 21, 21);
                   break;
                  case DEBUG_HANDLER_LOG:
                   if (logfile==NULL) {
                    logfile=open_log();
                   }
                   if (logfile==NULL) {
                    write_tuple(SE_ERROR,"Unable to log", 13,13);
                   } else {
                    fprintf(logfile,"Start debug trace\n");
                    fflush(logfile);
                    SEsetDebugHandler(&connection,debug_logger);
                    write_tuple(SE_OK, "Debug logging started", 21, 21);
                   }
                   break;
                  default:
#ifdef WITHDEBUG
                   if (logfile!=NULL) {
                    fprintf(logfile,"Unknown debug handler\n");
                    fflush(logfile);
                   }
#endif
                   write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                 } // switch on handler types
                } // valid handler type
               } // handler type decoded
              } // handler type&size correct
              free(tmp_ptr1);
             } // handler type&size obtained

            } // connected
           } // right tuple arity
           break;

          case Num_SEloadData :
           if ((int_value1!=3) && (int_value1 != 4)){
            write_tuple(SE_ERROR, "Bad arity for SEloadData", 23, 23);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEloadData: not connected", 25, 25);
            } else {
             if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
              if (logfile != NULL){
               fprintf(logfile,"Get type&size for data chunk failed\n");
               fflush(logfile);
              }
#endif
              write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
             } else {
               // int_value2 is type, int_value3 is size
               // No limit for data chunk size, check type only
              if (int_value2!=ERL_BINARY_EXT) {
#ifdef WITHDEBUG
               if (logfile != NULL){
                fprintf(logfile,"Data chunk not binary\n");
                fflush(logfile);
               }
#endif
               write_tuple(SE_ERROR, "Bad encoded type", 16, 16);
              } else {
               tmp_ptr1=(char*)malloc(int_value3+1);
               bzero(tmp_ptr1,int_value3+1);
               if (ei_decode_binary(read_buf, buf_index, tmp_ptr1, &long_value1)!=0){
                write_tuple(SE_ERROR, "Data chunk decode failed", 24, 24);
               } else {
                // Here value of long_value1 must be somehow reserved
                int_value4=(int)long_value1; // value of long_value1 is expected
                                             // to be less than INT_MAX due to
                                             // packet length check when reading
                if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
                 if (logfile != NULL){
                  fprintf(logfile,"Get type&size for document name failed\n");
                  fflush(logfile);
                 }
#endif
                 write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
                 int_value4=0;
                } else {
                  // int_value2 is type, int_value3 is size
                 if (int_value2!=ERL_BINARY_EXT || int_value3>SE_MAX_DOCUMENT_NAME_LENGTH-1) {
#ifdef WITHDEBUG
                  if (logfile != NULL){
                   fprintf(logfile,"Document name not binary or too long\n");
                   fflush(logfile);
                  }
#endif
                  write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
                  int_value4=0;
                 } else {
                  tmp_ptr2=(char*)malloc(int_value3+1);
                  bzero(tmp_ptr2,int_value3+1);
                  if (ei_decode_binary(read_buf, buf_index, tmp_ptr2, &long_value1)!=0){
                   write_tuple(SE_ERROR, "Document name decode failed", 27, 27);
                   int_value4=0;
                  } else {
                   if (int_value1==3) {
                    tmp_ptr3=NULL;
                   } else {
                    if(ei_get_type(read_buf, buf_index, &int_value2, &int_value3)!=0) {
#ifdef WITHDEBUG
                     if (logfile != NULL){
                      fprintf(logfile,"Get type&size for collection name failed\n");
                      fflush(logfile);
                     }
#endif
                     write_tuple(SE_ERROR, "Get encoded type&size failed", 28, 28);
                     int_value4=0;
                    } else {
                      // int_value2 is type, int_value3 is size
                     if (int_value2!=ERL_BINARY_EXT || int_value3>SE_MAX_COLLECTION_NAME_LENGTH-1) {
#ifdef WITHDEBUG
                      if (logfile != NULL){
                       fprintf(logfile,"Collection name not binary or too long\n");
                       fflush(logfile);
                      }
#endif
                      write_tuple(SE_ERROR, "Bad encoded type/size", 21, 21);
                      int_value4=0;
                     } else {
                      tmp_ptr3=(char*)malloc(int_value3+1);
                      bzero(tmp_ptr3,int_value3+1);
                      if (ei_decode_binary(read_buf, buf_index, tmp_ptr3, &long_value1)!=0){
                       write_tuple(SE_ERROR, "Collection name decode failed", 29, 29);
                       int_value4=0;
                      } else {
                       if(long_value1==0) {
                        free(tmp_ptr3);
                        tmp_ptr3=NULL;
                       }
                      }
                     } // type&size for collection name are correct
                    } // type&size for collection name obtained
                   } // collection name is not NULL
                   if(int_value4!=0) {
#ifdef WITHDEBUG
                    if (logfile != NULL){
                     fprintf(logfile,"Going to load %d bytes of data <<\"%s\">>\n",int_value4,tmp_ptr1);
                     fflush(logfile);
                    }
#endif
                    int_value3=SEloadData(&connection, tmp_ptr1, int_value4, tmp_ptr2, tmp_ptr3);
                    switch (int_value3) {
                     case SEDNA_DATA_CHUNK_LOADED :
                      write_tuple(SE_OK, "Data loaded", 11, 11);
                      break;
                     case SEDNA_ERROR :
                      write_tuple(SE_ERROR, "Unspecified error", 17, 17);
                      break;
                     default:
                      write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
                    }; //switch
                   } // tmp_ptr3 decoded ok
                   free(tmp_ptr3);
                  } // tmp_ptr2 decoded ok
                  free(tmp_ptr2);
                 } // docname type&size correct
                } // docname type&size obtained
               } // tmp_ptr1 decoded ok
               free(tmp_ptr1);
              } // data chunk type correct
             } // data chunk type&size obtained
            } // connected
           }; // right tuple arity
          break;

          case Num_SEendLoadData :
           if (int_value1!=1){
            write_tuple(SE_ERROR, "Bad arity for SEloadData", 23, 23);
           } else {
            if (connection.socket==invalid_socket){
             write_tuple(SE_ERROR, "SEloadData: not connected", 25, 25);
            } else {
             int_value1=SEendLoadData(&connection);
             switch (int_value1) {
              case SEDNA_BULK_LOAD_SUCCEEDED:
               write_tuple(SE_OK, "Load finished", 13, 13);
               break;
              case SEDNA_BULK_LOAD_FAILED:
               write_tuple(SE_ERROR, "Load failed", 11, 11);
               break;
              case SEDNA_ERROR:
               write_tuple(SE_ERROR, "Unspecified error", 17, 17);
               break;
              default:
               write_tuple(SE_ERROR, "Unexpected condition", 20, 20);
             } //switch
            } // connected
           }; // right tuple arity
           break;
          defaut :
#ifdef WITHDEBUG
           if (logfile!=NULL) {
            fprintf(logfile, "Bad command name\n");
            fflush(logfile);
           }
#endif
           write_tuple(SE_ERROR, "Bad command name", 16, 16);
         } // switch (int_value3)

        } // Valid command
       } // Sedna command decoded ok
//       free(tmp_ptr1); // tmp_ptr was allocated after successful decoding tuple
                       // header and was in use throughout all processing the tuple
      } // command is of type atom ang length not too long
     } // get type&size for command succeeded
    } // tuple arity decoded ok
   } // version decoded ok
   bytecount1=0;
   bytecount2=0;
  } // input packet not too long
#ifdef WITHDEBUG
  if (logfile != NULL) {
   fprintf(logfile,"Cycle pass finished\n");
   fflush(logfile);
  }
#endif
  bzero(read_buf,bytecount4);
  free(read_buf);
  bytecount4=read_data(&read_buf);
 } // while (bytecount4>0)
//#ifdef WITHDEBUG
 if (logfile != NULL) {
  fclose(logfile);
//#endif
 } // if argc and argv are ok
 return(0);
}