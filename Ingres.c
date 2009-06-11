/*
**      Copyright (c) 2007 Ingres Corporation
**
**      This program is free software; you can redistribute it and/or modify
**      it under the terms of the GNU General Public License version 2 as 
**      published by the Free Software Foundation.
**
**      This program is distributed in the hope that it will be useful,
**      but WITHOUT ANY WARRANTY; without even the implied warranty of
**      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
**      GNU General Public License for more details.
**
**      You should have received a copy of the GNU General Public License along
**      with this program; if not, write to the Free Software Foundation, Inc.,
**      51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

/*      static char     Sccsid[] = "@(#)Ingres.c        1.2.2    17-jan-2008";        */

/*
**      INGRES.C
**
**      History
**              12/18/06 (Jared Richardson)
**                      Community developer provided original source 
**                      for Ingres Corp to use as basis for Ruby driver
**              02/21/07 (robert.kent@ingres.com)
**                      General cleanup, removal of unused variables, etc,
**                      replacement of trim function with Ingres version,
**                      creation of debug flag setting function
**              11/30/07 (bruce.lunsford@ingres.com)
**                      Fix Segvio when selecting char or varchar with data
**                      longer than 4074 bytes (co_sizeAdvise + 2).  Also
**                      improve performance on non-lob colunn fetches by
**                      eliminating alloc/free on every column.
**              06/17/08 (bruce.lunsford@ingres.com)
**                      Add support for datatypes float4, float8, money,
**                      decimal, bigint.  Removed unref'd processShortField().
**                      Change INT2NUM->INT2FIX where feasible for performance.
**                      Add support for Ingres ANSI date/time datatypes.
 */

#include "ruby.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <iiapi.h>

static VALUE cIngres;

typedef unsigned short ucs2;
typedef unsigned long ucs4;
typedef unsigned char utf8;
typedef short i2;
typedef long i4;
typedef unsigned short u_i2;
#ifndef _WIN32
typedef long long __int64;
#endif

#define UTF8				utf8
#define UCS2				ucs2
#define UCS4				ucs4

#define RUBY_STRING    			"STRING"
#define RUBY_INTEGER   			"INTEGER"
#define RUBY_BYTE      			"BYTE"
#define RUBY_TEXT      			"TEXT"
#define RUBY_VARCHAR   			"VARCHAR"
#define RUBY_DATE      			"DATE"
#define RUBY_DOUBLE    			"DOUBLE"
#define RUBY_TINYINT   			"TINYINT"
#define RUBY_LOB       			"LOB"
#define RUBY_UNMAPPED  			"UNMAPPED_DATATYPE"
#define ERROR_MESSAGE_HEADER 		"Ingres Diagnostic Messages:\n"
#define RubyParamOffset(ip, ipc) 	((ip - ipc) * (2 + ipc))

#define RUBY_NVARCHAR_PARAMETER		'N'
#define RUBY_NCHAR_PARAMETER		'n'
#define RUBY_VARCHAR_PARAMETER		'v'
#define RUBY_DECIMAL_PARAMETER		'D'
#define RUBY_LONG_BYTE_PARAMETER	'B'
#define RUBY_LONG_TEXT_PARAMETER	'T'
#define RUBY_LONG_VARCHAR_PARAMETER	'V'

#define RUBY_INTEGER_PARAMETER		'i'
#define RUBY_BYTE_PARAMETER		'b'
#define RUBY_CHAR_PARAMETER		'c'
#define RUBY_DATE_PARAMETER		'd'
#define RUBY_TEXT_PARAMETER		't'
#define RUBY_FLOAT_PARAMETER		'f'

#define DECIMAL_BUFFER_LEN		16
#define DECIMAL_PRECISION		31
#define DECIMAL_SCALE			15
#define DOUBLE_PRECISION		31
#define DOUBLE_SCALE			15
#define MAX_CHAR_SIZE		        32000  /* max #bytes Ingres (var)char */

typedef struct _II_GLOBALS
{
  int autocommit;
  II_PTR connHandle;
  II_PTR tranHandle;
  II_PTR stmtHandle;
  II_PTR envHandle;
  II_LONG fieldCount;
  II_LONG co_sizeAdvise;
  IIAPI_DESCRIPTOR *descriptor;
  II_CHAR *errorText;
  II_CHAR sqlstate[6];
  II_LONG errorCode;
  int apiLevel;
  int paramCount;
  int allow_persistent;
  int num_persistent;
  char *cursor_id;
  long cursor_mode;
  char *currentDatabase;
  int debug;
  int debug_connection;
  int debug_sql;
  int debug_termination;
  int debug_transactions;
} II_GLOBALS;

typedef struct _RUBY_IIAPI_DATAVALUE
{
  IIAPI_DATAVALUE dataValue[1];
  long dv_length;
} RUBY_IIAPI_DATAVALUE;

typedef struct _RUBY_PARAMETER
{
  VALUE vkey;
  VALUE vtype;
  VALUE vvalue;
} RUBY_PARAMETER;


II_GLOBALS ii_globals;
int GLOBAL_TABLE_LIST_QUERY_FLAG = FALSE;
II_LONG global_rows_affected = 0;
VALUE global_keep_me = (VALUE) FALSE;
VALUE global_resultset = (VALUE) FALSE;
VALUE global_table_list = (VALUE) FALSE;
VALUE global_r_column_names = (VALUE) FALSE;
VALUE global_r_data_sizes = (VALUE) FALSE;
VALUE global_r_data_types = (VALUE) FALSE;


/*
 * Function declarations
 */
VALUE ii_execute (VALUE param_self, VALUE param_queryText);
void closeQuery ();
size_t STtrmwhite (register char *string);	// available via Ingres Compatibility Layer
static int utf8_to_utf16 (UTF8 * sourceStart, const UTF8 * sourceEnd, UCS2 * targetStart, const UCS2 * targetEnd, long *reslen);	// available via Ingres Corporation ADU layer
static int utf16_to_utf8 (UCS2 * sourceStart, const UCS2 * sourceEnd, UTF8 * targetStart, const UTF8 * targetEnd, long *reslen);	// available via Ingres Corporation ADU layer


/* static int ii_sync(IIAPI_GENPARM *genParm)
 * Waits for completion of the last Ingres api call used because of the asynchronous design of this api
*/
static int
ii_sync (IIAPI_GENPARM * genParm)
{
  static IIAPI_WAITPARM waitParm = {
    -1,				/* no timeout, we don't want asynchronous queries */
    0				/* wt_status (output) */
  };
  static char function_name[] = "ii_sync";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  while (genParm->gp_completed == FALSE)
    {
      IIapi_wait (&waitParm);
    }

  if (waitParm.wt_status != IIAPI_ST_SUCCESS)
    rb_raise (rb_eRuntimeError, "IIapi_wait() failed.");

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return 0;
}


void *
ii_allocate (size_t nitems, size_t size)
{
  void *ptr = NULL;
  char function_name[] = "ii_allocate";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  ptr = calloc (nitems, size);

  if (ptr == NULL)
    rb_raise (rb_eRuntimeError, "Error! Failed to allocate memory. ");

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return ptr;
}


void *
ii_reallocate (void *oldPtr, size_t nitems, size_t size)
{
  void *ptr = NULL;
  char function_name[] = "ii_reallocate";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (oldPtr == NULL)
    return ii_allocate (nitems, size);

  ptr = realloc (oldPtr, nitems * size);

  if (ptr == NULL)
    rb_raise (rb_eRuntimeError, "Error! Failed to reallocate memory. ");

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return ptr;
}


void
ii_free (void **ptr)
{
  char function_name[] = "ii_free";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (*ptr != NULL)
    {
      free (*ptr);
      *ptr = NULL;
    }
  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return;
}


void
ii_api_init ()
{
  IIAPI_INITPARM initParm;
  char function_name[] = "ii_api_init";
  if (ii_globals.debug)
    printf ("Entering %s, API Version is %d.\n", function_name, IIAPI_VERSION);

  initParm.in_version = IIAPI_VERSION;
  initParm.in_timeout = -1;
  IIapi_initialize (&initParm);
  ii_globals.envHandle = initParm.in_envHandle;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


VALUE
ii_init (VALUE param_self)
{
  char function_name[] = "ii_init";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  ii_globals.debug = FALSE;
  ii_globals.debug_connection = FALSE;
  ii_globals.debug_sql = FALSE;
  ii_globals.debug_termination = FALSE;
  ii_globals.debug_transactions = FALSE;
  ii_globals.allow_persistent = FALSE;
  ii_globals.autocommit = TRUE;
  ii_globals.currentDatabase = NULL;

  if (ii_globals.debug)
    printf ("%s: About to execute ii_api_init ()\n", function_name);
  ii_api_init ();

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return param_self;
}


void
init_rb_public_global (VALUE * param_rb_global)
{
  char function_name[] = "init_rb_public_global";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (!(*param_rb_global))
    {
      *param_rb_global = rb_ary_new ();
      rb_global_variable (param_rb_global);
    }
  rb_ary_clear (*param_rb_global);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


void
init_rb_private_global (VALUE * param_rb_global)
{
  char function_name[] = "init_rb_private_global";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (!(*param_rb_global))
    *param_rb_global = rb_ary_new ();

  rb_ary_clear (*param_rb_global);
  rb_ary_push (global_keep_me, *param_rb_global);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


void
init_globals ()
{
  char function_name[] = "init_globals";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  init_rb_public_global (&global_keep_me);
  init_rb_public_global (&global_resultset);
  rb_ary_push (global_keep_me, global_resultset);

  init_rb_private_global (&global_table_list);
  init_rb_private_global (&global_r_column_names);
  init_rb_private_global (&global_r_data_sizes);
  init_rb_private_global (&global_r_data_types);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


/* Just convert the Ingres datatypes to a string value.
 * I've included all the Ingres datatypes here for reference.
 * Taken from from iiapi.h
 *
# define IIAPI_HNDL_TYPE    ((IIAPI_DT_ID) -1)
# define IIAPI_CHR_TYPE     ((IIAPI_DT_ID) 32)
# define IIAPI_CHA_TYPE     ((IIAPI_DT_ID) 20)
# define IIAPI_VCH_TYPE     ((IIAPI_DT_ID) 21)
# define IIAPI_LVCH_TYPE    ((IIAPI_DT_ID) 22)
# define IIAPI_BYTE_TYPE    ((IIAPI_DT_ID) 23)
# define IIAPI_VBYTE_TYPE   ((IIAPI_DT_ID) 24)
# define IIAPI_LBYTE_TYPE   ((IIAPI_DT_ID) 25)
# define IIAPI_NCHA_TYPE    ((IIAPI_DT_ID) 26)
# define IIAPI_NVCH_TYPE    ((IIAPI_DT_ID) 27)
# define IIAPI_LNVCH_TYPE   ((IIAPI_DT_ID) 28)
# define IIAPI_TXT_TYPE     ((IIAPI_DT_ID) 37)
# define IIAPI_LTXT_TYPE    ((IIAPI_DT_ID) 41)
# define IIAPI_INT_TYPE     ((IIAPI_DT_ID) 30)
# define IIAPI_FLT_TYPE     ((IIAPI_DT_ID) 31)
# define IIAPI_MNY_TYPE     ((IIAPI_DT_ID)  5)
# define IIAPI_DEC_TYPE     ((IIAPI_DT_ID) 10)
# define IIAPI_DTE_TYPE     ((IIAPI_DT_ID)  3)  ** Ingres Date **
# define IIAPI_DATE_TYPE    ((IIAPI_DT_ID)  4)  ** ANSI Date **
# define IIAPI_TIME_TYPE    ((IIAPI_DT_ID)  8)
# define IIAPI_TMWO_TYPE    ((IIAPI_DT_ID)  6)
# define IIAPI_TMTZ_TYPE    ((IIAPI_DT_ID)  7)
# define IIAPI_TS_TYPE      ((IIAPI_DT_ID) 19)
# define IIAPI_TSWO_TYPE    ((IIAPI_DT_ID)  9)
# define IIAPI_TSTZ_TYPE    ((IIAPI_DT_ID) 18)
# define IIAPI_INTYM_TYPE   ((IIAPI_DT_ID) 33)
# define IIAPI_INTDS_TYPE   ((IIAPI_DT_ID) 34)
# define IIAPI_LOGKEY_TYPE  ((IIAPI_DT_ID) 11)
# define IIAPI_TABKEY_TYPE  ((IIAPI_DT_ID) 12)
*/

char *
getIngresDataTypeAsString (IIAPI_DT_ID param_dt_id)
{
  char *dataType = NULL;
  char function_name[] = "getIngresDataTypeAsString";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  switch (param_dt_id)
    {
    case IIAPI_LNVCH_TYPE:
    case IIAPI_LBYTE_TYPE:
    case IIAPI_LVCH_TYPE:
      dataType = RUBY_LOB;
      break;

    case IIAPI_CHA_TYPE:
    case IIAPI_NCHA_TYPE:
    case IIAPI_CHR_TYPE:
    case IIAPI_DEC_TYPE:
      dataType = RUBY_STRING;
      break;

    case IIAPI_INT_TYPE:
      dataType = RUBY_INTEGER;
      break;

    case IIAPI_FLT_TYPE:
    case IIAPI_MNY_TYPE:
      dataType = RUBY_DOUBLE;
      break;

    case IIAPI_BYTE_TYPE:
      dataType = RUBY_BYTE;
      break;

    case IIAPI_TXT_TYPE:
      dataType = RUBY_TEXT;
      break;

    case IIAPI_VCH_TYPE:
    case IIAPI_NVCH_TYPE:
      dataType = RUBY_VARCHAR;
      break;

    case IIAPI_DTE_TYPE:
#ifdef IIAPI_DATE_TYPE
    case IIAPI_DATE_TYPE:
    case IIAPI_TIME_TYPE:
    case IIAPI_TMWO_TYPE:
    case IIAPI_TMTZ_TYPE:
    case IIAPI_TS_TYPE:
    case IIAPI_TSWO_TYPE:
    case IIAPI_TSTZ_TYPE:
    case IIAPI_INTYM_TYPE:
    case IIAPI_INTDS_TYPE:
#endif
      dataType = RUBY_DATE;
      break;

    default:
      dataType = RUBY_UNMAPPED;
      printf ("\nUnmapped data type is %d\n", param_dt_id);
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return dataType;
}


/* from iiapi.h
# define IIAPI_ST_SUCCESS    		0
# define IIAPI_ST_MESSAGE    		1
# define IIAPI_ST_WARNING    		2
# define IIAPI_ST_NO_DATA    		3
# define IIAPI_ST_ERROR      		4
# define IIAPI_ST_FAILURE    		5
# define IIAPI_ST_NOT_INITIALIZED  	6
# define IIAPI_ST_INVALID_HANDLE  	7
# define IIAPI_ST_OUT_OF_MEMORY    	8
*/

/** check for and print out any errors found */
void
printGPStatus (int param_gp_status)
{
  char *ptr = NULL;
  char val[33] = "";
  char function_name[] = "printGPStatus";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  switch (param_gp_status)
    {
    case IIAPI_ST_ERROR:
      ptr = "IIAPI_ST_ERROR";
      break;

    case IIAPI_ST_FAILURE:
      ptr = "IIAPI_ST_FAILURE";
      break;

    case IIAPI_ST_NOT_INITIALIZED:
      ptr = "IIAPI_ST_NOT_INITIALIZED";
      break;

    case IIAPI_ST_INVALID_HANDLE:
      ptr = "IIAPI_ST_INVALID_HANDLE";
      break;

    case IIAPI_ST_OUT_OF_MEMORY:
      ptr = "IIAPI_ST_OUT_OF_MEMORY";
      break;

    default:
      ptr = val;
      sprintf (val, "%d", param_gp_status);
      break;
    }

  printf ("\n%s: status = %s\n", function_name, ptr);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


char *
getErrorTypeStr (int param_ge_type)
{
  char *ptr = NULL;
  char function_name[] = "getErrorTypeStr";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  switch (param_ge_type)
    {
    case IIAPI_GE_ERROR:
      ptr = "ERROR";
      break;
    case IIAPI_GE_WARNING:
      ptr = "WARNING";
      break;
    case IIAPI_GE_MESSAGE:
      ptr = "USER MESSAGE";
      break;
    default:
      ptr = "?";
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ptr;
}


int
ii_checkError (IIAPI_GENPARM * param_genParm)
{
  int ret_val = FALSE;
  char function_name[] = "ii_checkError";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (param_genParm != NULL && param_genParm->gp_status >= IIAPI_ST_ERROR)
    {
      ret_val = TRUE;
      printGPStatus (param_genParm->gp_status);
    }

  if (param_genParm != NULL && param_genParm->gp_errorHandle)
    {
      IIAPI_GETEINFOPARM getErrParm;
      char *errorTypeStr = NULL;
      char *message = NULL;

      ret_val = TRUE;
      printf ("\n%s: %s\n", function_name, ERROR_MESSAGE_HEADER);

      // Provide initial error handle.
      getErrParm.ge_errorHandle = param_genParm->gp_errorHandle;

      // Call IIapi_getErrorInfo() in loop until no data.
      while (IIapi_getErrorInfo (&getErrParm),
	     getErrParm.ge_status == IIAPI_ST_SUCCESS)
	{
	  errorTypeStr = getErrorTypeStr (getErrParm.ge_type);
	  message = (getErrParm.ge_message ? getErrParm.ge_message : "NULL");

	  // Print error message info
	  printf ("%s: SQLSTATE: %s, CODE: 0x%x: %s\n",
		  errorTypeStr, getErrParm.ge_SQLSTATE,
		  getErrParm.ge_errorCode, message);
	}
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


void
connect2ingres (char *param_targetDB, char *param_username,
		char *param_password)
{
  IIAPI_CONNPARM connParm;
  char function_name[] = "connect2ingres";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (ii_globals.debug || ii_globals.debug_connection)
    printf
      ("%s: Attempting to connect to database: %s, as username: %s, password: %s.\n",
       function_name, param_targetDB, param_username, param_password);

  connParm.co_genParm.gp_callback = NULL;
  connParm.co_genParm.gp_closure = NULL;
  connParm.co_target = param_targetDB;
  connParm.co_connHandle =
    (ii_globals.connHandle !=
     NULL) ? ii_globals.connHandle : ii_globals.envHandle;
  connParm.co_tranHandle = NULL;
  connParm.co_type = IIAPI_CT_SQL;
  connParm.co_username = param_username;
  connParm.co_password = param_password;
  connParm.co_timeout = -1;

  if (ii_globals.debug || ii_globals.debug_connection)
    printf ("%s: About to execute IIapi_connect (&connParm)\n",
	    function_name);

  IIapi_connect (&connParm);
  ii_sync (&(connParm.co_genParm));

  if (ii_globals.debug || ii_globals.debug_connection)
    printf ("%s: Executed IIapi_connect, status is %i\n", function_name,
	    connParm.co_genParm.gp_status);
  if (connParm.co_genParm.gp_status == IIAPI_ST_SUCCESS)
    {
      ii_globals.connHandle = connParm.co_connHandle;
      ii_globals.co_sizeAdvise = connParm.co_sizeAdvise;
      ii_globals.apiLevel = connParm.co_apiLevel;
    }
  else
    {
      ii_checkError (&connParm.co_genParm);
      rb_raise (rb_eRuntimeError,
		"Error! Failed to connect to database: %s, as username: %s, password: %s. ",
		param_targetDB, param_username, param_password);
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}

/* connects to the database */
VALUE
ii_connect_with_credentials (VALUE param_self, VALUE param_targetDB,
			     VALUE param_username, VALUE param_password)
{
  IIAPI_CONNPARM connParm;
  IIAPI_WAITPARM waitParm = { -1 };
  char function_name[] = "ii_connect_with_credentials";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (ii_globals.debug || ii_globals.debug_connection)
    printf ("%s: About to verify username is a string\n", function_name);
  Check_Type (param_username, T_STRING);
  if (ii_globals.debug || ii_globals.debug_connection)
    printf ("%s: About to verify password is a string\n", function_name);
  Check_Type (param_password, T_STRING);
  if (ii_globals.debug || ii_globals.debug_connection)
    printf ("%s: About to verify target database is a string\n",
	    function_name);
  Check_Type (param_targetDB, T_STRING);

  connect2ingres (RSTRING (param_targetDB)->ptr,
		  RSTRING (param_username)->ptr,
		  RSTRING (param_password)->ptr);

  ii_free ((void **) &(ii_globals.currentDatabase));
  ii_globals.currentDatabase =
    ii_allocate (RSTRING (param_targetDB)->len + 1, sizeof (char));
  memcpy (ii_globals.currentDatabase, RSTRING (param_targetDB)->ptr,
	  RSTRING (param_targetDB)->len);

  if (ii_globals.debug || ii_globals.debug_connection)
    printf ("%s: Set ii_globals.currentDatabase to %s\n", function_name,
	    ii_globals.currentDatabase);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return param_self;
}


/** for those who want to use a default connection **/
VALUE
ii_connect (VALUE param_self, VALUE param_targetDB)
{
  char function_name[] = "ii_connect";
  VALUE user = rb_str_new2 ("");
  VALUE pswd = rb_str_new2 ("");

  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (ii_globals.debug || ii_globals.debug_connection)
    printf ("%s: Set username and password to empty strings\n", function_name,
	    ii_globals.currentDatabase);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ii_connect_with_credentials (param_self, param_targetDB, user, pswd);
}


static VALUE
ii_disconnect (VALUE param_self)
{
  char function_name[] = "ii_disconnect";
  IIAPI_DISCONNPARM disconnParm;
  IIAPI_TERMPARM termParm;

  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);
  if (ii_globals.debug || ii_globals.debug_termination)
    printf ("%s: Preparing to disconnect\n", function_name);

  if (global_keep_me)
    rb_ary_clear (global_keep_me);

  if (ii_globals.connHandle == NULL)
    {
      if (ii_globals.debug || ii_globals.debug_termination)
	printf ("%s: Not connected\n", function_name);
      return Qnil;
    }

  disconnParm.dc_genParm.gp_callback = NULL;
  disconnParm.dc_genParm.gp_closure = NULL;
  disconnParm.dc_connHandle = ii_globals.connHandle;

  if (ii_globals.debug || ii_globals.debug_termination)
    printf ("%s: Next, IIapi_disconnect\n", function_name);
  IIapi_disconnect (&disconnParm);
  ii_sync (&(disconnParm.dc_genParm));

  if (ii_globals.debug || ii_globals.debug_termination)
    printf ("%s: Disconnect status is >>%d<<\n", function_name,
	    disconnParm.dc_genParm.gp_status);
  ii_checkError (&disconnParm.dc_genParm);
  if (ii_globals.debug || ii_globals.debug_termination)
    printf
      ("%s: Completed IIapi_disconnect and related error checks\n",
       function_name);

  if (ii_globals.debug || ii_globals.debug_termination)
    printf ("ii_disconnect: Next, IIapi_terminate( &termParm )\n");
  IIapi_terminate (&termParm);

  ii_globals.connHandle = NULL;

  if (ii_globals.debug || ii_globals.debug_termination)
    printf ("%s: Completed IIapi_terminate( &termParm )\n", function_name);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return Qnil;
}


void
startTransaction (VALUE param_self)
{
  VALUE sql_string;
  IIAPI_WAITPARM waitParm = { -1 };
  char function_name[] = "startTransaction";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  // turn off autocommit. The calling program is now
  // responsible for transactions
  ii_globals.autocommit = FALSE;
  ii_globals.tranHandle = NULL;

  // now issue a simple query to get a transaction handle
  // FIXEME: consider adding a small, one entry table to make
  // this a really short, small query
  sql_string = rb_str_new2 ("SELECT relid FROM iirelation WHERE relid='iirelation'");
  ii_execute (param_self, sql_string);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


void
commit ()
{
  IIAPI_COMMITPARM commitParm;
  char function_name[] = "commit";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  /* commit the transaction */
  commitParm.cm_genParm.gp_callback = NULL;
  commitParm.cm_genParm.gp_closure = NULL;
  commitParm.cm_tranHandle = ii_globals.tranHandle;

  IIapi_commit (&commitParm);

  ii_sync (&(commitParm.cm_genParm));

  if (ii_globals.debug)
    printf ("\nTransaction commit status is ++%d++\n",
	    commitParm.cm_genParm.gp_status);
  ii_checkError (&commitParm.cm_genParm);

  // now turn automatic transaction handling back on
  ii_globals.tranHandle = NULL;
  ii_globals.autocommit = TRUE;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


void
rollback ()
{
  IIAPI_ROLLBACKPARM rollbackParm;
  char function_name[] = "rollback";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  rollbackParm.rb_tranHandle = ii_globals.tranHandle;
  rollbackParm.rb_genParm.gp_callback = NULL;
  rollbackParm.rb_genParm.gp_closure = NULL;
  rollbackParm.rb_savePointHandle = NULL;

  IIapi_rollback (&rollbackParm);

  ii_sync (&(rollbackParm.rb_genParm));
  ii_checkError (&rollbackParm.rb_genParm);

  // now turn automatic transaction handling back on
  ii_globals.tranHandle = NULL;
  ii_globals.autocommit = TRUE;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


II_PTR
executeQuery (char *param_sqlText)
{
  IIAPI_QUERYPARM queryParm;
  char function_name[] = "executeQuery";


  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  /*
   ** Call IIapi_query to execute statement.
   */
  queryParm.qy_connHandle = ii_globals.connHandle;
  queryParm.qy_genParm.gp_callback = NULL;
  queryParm.qy_genParm.gp_closure = NULL;
  queryParm.qy_queryType = IIAPI_QT_QUERY;
  queryParm.qy_queryText = param_sqlText;
  queryParm.qy_parameters = FALSE;
  queryParm.qy_tranHandle = ii_globals.tranHandle;
  queryParm.qy_stmtHandle = NULL;
#if defined(IIAPI_VERSION_6)
  queryParm.qy_flags  = 0;
#endif


  IIapi_query (&queryParm);

  ii_sync (&(queryParm.qy_genParm));

  ii_globals.stmtHandle = queryParm.qy_stmtHandle;

  if (ii_globals.debug)
    printf ("%s: Query status is >>%d<<\n", function_name,
	    queryParm.qy_genParm.gp_status);

  if (ii_globals.tranHandle == NULL)
    ii_globals.tranHandle = queryParm.qy_tranHandle;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return queryParm.qy_stmtHandle;
}


/* char * getProcedureName(char *statement) */
/* check to see if the query is for a procedure or not, if it is return the procedure name 
 * 
 * Procedure calls come in two forms either:
 * execute procedure procname
 * call procedure 
 *
 */
static char *
getProcedureName (char *param_sqlText)
{
  int proc_len = 0;
  char *start = NULL;
  char *end_space = NULL;
  char *end_term = NULL;
  char *end_bracket = NULL;
  char *procedureName = NULL;
  char exec_proc[] = "{execute procedure";
  char call_proc[] = "{call";
  char function_name[] = "getProcedureName";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (strncmp (param_sqlText, exec_proc, strlen (exec_proc)) == 0)
    start = param_sqlText + strlen (exec_proc);
  else if (strncmp (param_sqlText, call_proc, strlen (call_proc)) == 0)
    start = param_sqlText + strlen (call_proc);

  if (start != NULL)
    {
      while (*start == ' ')	// skip over spaces after 'call' or 'procedure'
	start++;

      /* look for a space, bracket or null terminator to determine end of */
      /* the procedure name, end_term should never be NULL */
      end_term = strchr (start, '}');
      end_space = strchr (start, ' ');
      end_bracket = strchr (start, '(');

      if (end_term == NULL)
	rb_raise (rb_eRuntimeError,
		  "%s: Error! Call to procedure not terminated with a '}'. ",
		  function_name);

      if (end_space == NULL && end_bracket == NULL)
	proc_len = end_term - start;
      else if (end_space != NULL && end_bracket == NULL)
	proc_len = end_space - start;
      else if (end_space == NULL && end_bracket != NULL)
	proc_len = end_bracket - start;
      else if (end_space != NULL && end_bracket != NULL)
	{
	  if (end_space > end_bracket)
	    proc_len = end_bracket - start;
	  else
	    proc_len = end_space - start;
	}

      procedureName = ii_allocate (proc_len + 1, sizeof (char));
      memcpy (procedureName, start, proc_len);
    }

  if (ii_globals.debug)
    printf ("Exiting %s, returning %s.\n", function_name, procedureName);
  return procedureName;
}


/* static long ii_paramcount(char *statement) */
/* ----------------------------------------------------------------------
 * int php_ii_paramcount(char *statement TSRMLS_DC)
 *
 * Count the placeholders (?) parameters in the statement
 * return -1 for error. 0 or number of question marks
 *
 * Thanks to ext/informix (based on php_intifx_preparse).
 *
 * ----------------------------------------------------------------------
*/
static long
countParameters (char *statement)
{
  char *src = statement;
  char *dst = statement;
  int parameterCount = 0;
  char ch;
  char end_quote = '\0';
  char function_name[] = "ii_paramcount";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  while ((ch = *src++) != '\0')
    {
      if (ch == end_quote)
	{
	  end_quote = '\0';
	}
      else if (end_quote != '\0')
	{
	  *dst++ = ch;
	  continue;
	}
      else if (ch == '\'' || ch == '\"')
	{
	  end_quote = ch;
	}
//      else if (ch == '{')
//      {
//        end_quote = '}';
//      }
      if (ch == '?')
	{
	  /* X/Open standard       */
	  *dst++ = '?';
	  parameterCount++;
	}
      else
	{
	  *dst++ = ch;
	  continue;
	}
    }

  if (ii_globals.debug)
    printf ("Exiting %s, returning %li.\n", function_name, parameterCount);
  return (parameterCount);
}


int
getIIParameter (RUBY_PARAMETER * parameter, VALUE rubyParams, int iiParam,
		int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "getIIParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  parameter->vkey =
    rb_ary_entry (rubyParams, RubyParamOffset (iiParam, isProcedureCall) + 0);
  parameter->vtype =
    rb_ary_entry (rubyParams, RubyParamOffset (iiParam, isProcedureCall) +
		  isProcedureCall);
  parameter->vvalue =
    rb_ary_entry (rubyParams, RubyParamOffset (iiParam, isProcedureCall) +
		  isProcedureCall + 1);
  if (ii_globals.debug)
    printf
      ("%s: Completed calls to rb_ary_entry, param = %i, vkey = %s, vtype = %s, vvalue = %s.\n",
       function_name, iiParam, RSTRING (parameter->vkey)->ptr,
       RSTRING (parameter->vtype)->ptr, RSTRING (parameter->vvalue)->ptr);

  if (!isProcedureCall)
    parameter->vkey = Qnil;

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setDescriptor (IIAPI_DESCRIPTOR * sd_descriptor, RUBY_PARAMETER * parameter,
	       int isProcedureCall, IIAPI_DT_ID ds_dataType,
	       II_UINT2 ds_length, II_INT2 ds_precision, II_INT2 ds_scale)
{
  int returnValue = 0;
  char function_name[] = "setDescriptorCommon";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (!isProcedureCall)
    {
      sd_descriptor->ds_columnName = NULL;
      sd_descriptor->ds_columnType = IIAPI_COL_QPARM;
    }
  else
    {
      sd_descriptor->ds_columnType = IIAPI_COL_PROCPARM;
      Check_Type (parameter->vkey, T_STRING);
      sd_descriptor->ds_columnName = RSTRING (parameter->vkey)->ptr;
    }
  sd_descriptor->ds_nullable = FALSE;
  sd_descriptor->ds_dataType = ds_dataType;
  sd_descriptor->ds_length = ds_length;
  sd_descriptor->ds_precision = ds_precision;
  sd_descriptor->ds_scale = ds_scale;

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setLongByteDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		       RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setLongByteDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_STRING);
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_LBYTE_TYPE,
		 (II_UINT2) ii_globals.co_sizeAdvise, 0, 0);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setCharDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		   RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setCharDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_STRING);
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_CHA_TYPE,
		 (II_UINT2) RSTRING (parameter->vvalue)->len, 0, 0);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setVarcharDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		      RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setVarcharDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_STRING);
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_VCH_TYPE,
		 (II_UINT2) (RSTRING (parameter->vvalue)->len + 2), 0, 0);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setDecimalDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		      RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setDecimalDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_STRING);
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_DEC_TYPE,
		 (II_UINT2) DECIMAL_BUFFER_LEN, DECIMAL_PRECISION, DECIMAL_SCALE);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setFloatDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		    RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setFloatDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_FLOAT);
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_FLT_TYPE,
		 (II_UINT2) sizeof (double), DOUBLE_PRECISION, DOUBLE_SCALE);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setLongVarcharDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
			  RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setLongVarcharDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_STRING);
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_VCH_TYPE,
		 (II_UINT2) ii_globals.co_sizeAdvise, 0, 0);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setIntegerDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		      RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setIntegerDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_FIXNUM);
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_INT_TYPE,
		 (II_UINT2) sizeof (long), 0, 0);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setNCharDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		    RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  long valueLen = RSTRING (parameter->vvalue)->len;
  char *valuePtr = RSTRING (parameter->vvalue)->ptr;
  long ncharLen = valueLen * sizeof (UCS2);
  char *nchar = ii_allocate (ncharLen + sizeof (UCS2), sizeof (char));
  char function_name[] = "setNCharDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_STRING);

  if (utf8_to_utf16
      (valuePtr, valuePtr + valueLen, (UCS2 *) nchar,
       (UCS2 *) (nchar + ncharLen), &ncharLen))
    rb_raise (rb_eRuntimeError,
	      "Error! Failed to transcode %s (n) to utf16.\n", valuePtr);
  ii_free ((void **) &nchar);

  /* Allow a null at the end */
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_NCHA_TYPE,
		 (II_UINT2) (ncharLen * sizeof (UCS2)), 0, 0);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setNVarcharDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
		       RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  long valueLen = RSTRING (parameter->vvalue)->len;
  char *valuePtr = RSTRING (parameter->vvalue)->ptr;
  long nvarcharlen = valueLen * sizeof (UCS2);
  char *nvarchar =
    ii_allocate (2 + nvarcharlen + sizeof (UCS2), sizeof (char));
  char function_name[] = "setNVarcharDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vvalue, T_STRING);
  nvarcharlen = valueLen * sizeof (UCS2);
  nvarchar = ii_allocate (2 + nvarcharlen + sizeof (UCS2), sizeof (char));
  if (utf8_to_utf16 (valuePtr, valuePtr + valueLen, (UCS2 *) nvarchar,
		     (UCS2 *) (nvarchar + nvarcharlen), &nvarcharlen))
    rb_raise (rb_eRuntimeError,
	      "Error! Failed to transcode %s (N) to utf16.\n", valuePtr);

  /* The first two bytes will contain the length */
  setDescriptor (sd_descriptor, parameter, isProcedureCall, IIAPI_NVCH_TYPE,
		 (II_UINT2) (2 + (nvarcharlen * sizeof (UCS2))), 0, 0);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setParameterDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
			RUBY_PARAMETER * parameter, int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setParameterDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (parameter->vtype, T_STRING);
  if (RSTRING (parameter->vtype)->len != 1)
    {
      rb_raise (rb_eRuntimeError, "Paramter type (%s) length (%i) != 1.",
		RSTRING (parameter->vtype)->ptr,
		RSTRING (parameter->vtype)->len);
    }
  if (ii_globals.debug)
    printf ("%s: Type is %c.\n", function_name,
	    (char) *(RSTRING (parameter->vtype)->ptr));

  switch ((char) *(RSTRING (parameter->vtype)->ptr))
    {
    case RUBY_LONG_BYTE_PARAMETER:
      setLongByteDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_BYTE_PARAMETER:
    case RUBY_CHAR_PARAMETER:
    case RUBY_DATE_PARAMETER:
    case RUBY_TEXT_PARAMETER:
      setCharDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_DECIMAL_PARAMETER:
      setDecimalDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_FLOAT_PARAMETER:
      setFloatDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_LONG_TEXT_PARAMETER:
    case RUBY_LONG_VARCHAR_PARAMETER:
      setLongVarcharDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_INTEGER_PARAMETER:
      setIntegerDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_NCHAR_PARAMETER:
      setNCharDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_NVARCHAR_PARAMETER:
      setNVarcharDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    case RUBY_VARCHAR_PARAMETER:
      setVarcharDescriptor (sd_descriptor, parameter, isProcedureCall);
      break;

    default:
      if (ii_globals.debug)
	printf ("%s: Not set ds_length for type = %c.\n",
		function_name, (char) *(RSTRING (parameter->vtype)->ptr));
      break;
    }

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setProcedureNameDescriptor (IIAPI_DESCRIPTOR * sd_descriptor,
			    char *procedureName)
{
  int returnValue = 0;
  char function_name[] = "setProcedureNameDescriptor";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  /* setup the first parameter as the procedure name */
  sd_descriptor->ds_dataType = IIAPI_CHA_TYPE;
  sd_descriptor->ds_length = strlen (procedureName);
  sd_descriptor->ds_nullable = FALSE;
  sd_descriptor->ds_precision = 0;
  sd_descriptor->ds_scale = 0;
  sd_descriptor->ds_columnType = IIAPI_COL_SVCPARM;
  sd_descriptor->ds_columnName = NULL;

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
setDescriptorParms (IIAPI_SETDESCRPARM * setDescrParm, int paramCount,
		    int isProcedureCall)
{
  int returnValue = 0;
  char function_name[] = "setDescriptorParms";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  /* if we are sending params then we need to describe them into to Ingres */
  /* if no parameters have been provided to a procedure call there is always 1 */
  /* parameter, the procedure name */
  setDescrParm->sd_genParm.gp_callback = NULL;
  setDescrParm->sd_genParm.gp_closure = NULL;
  setDescrParm->sd_stmtHandle = ii_globals.stmtHandle;
  setDescrParm->sd_descriptorCount = paramCount + isProcedureCall;
  setDescrParm->sd_descriptor =
    ii_allocate (setDescrParm->sd_descriptorCount, sizeof (IIAPI_DESCRIPTOR));

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
ii_putParamter (II_BOOL moreSegments, II_BOOL dv_null, II_UINT2 dv_length,
		II_PTR dv_value)
{
  IIAPI_PUTPARMPARM putParmParm;
  IIAPI_DATAVALUE dataValue[1];
  int returnValue = 0;
  char function_name[] = "ii_putParamter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  dataValue[0].dv_null = dv_null;
  dataValue[0].dv_length = dv_length;
  dataValue[0].dv_value = dv_value;

  putParmParm.pp_moreSegments = moreSegments;
  putParmParm.pp_parmData = dataValue;
  putParmParm.pp_genParm.gp_callback = NULL;
  putParmParm.pp_genParm.gp_closure = NULL;
  putParmParm.pp_stmtHandle = ii_globals.stmtHandle;
  putParmParm.pp_parmCount = 1;
  IIapi_putParms (&putParmParm);
  ii_sync (&(putParmParm.pp_genParm));

  if (ii_checkError (&(putParmParm.pp_genParm)))
    rb_raise (rb_eRuntimeError, "Error putting a parameter.");

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putProcedureNameParameter (char *procedureName)
{
  int returnValue = 0;
  char function_name[] = "putProcedureNameParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  returnValue =
    ii_putParamter (0, FALSE, (II_UINT2) strlen (procedureName), procedureName);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putRubyFixNumParameter (RUBY_PARAMETER * parameter)
{
  long number = NUM2INT (parameter->vvalue);
  int returnValue = 0;
  char function_name[] = "putRubyFixNumParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  returnValue = ii_putParamter (0, FALSE, sizeof (number), &number);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putRubyFloatParameter (RUBY_PARAMETER * parameter)
{
  double number = NUM2DBL (parameter->vvalue);
  int returnValue = 0;
  char function_name[] = "putRubyFloatParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  returnValue = ii_putParamter (0, FALSE, sizeof (number), &number);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putNVarcharParameter (RUBY_PARAMETER * parameter)
{
  char *nvarchar = NULL;
  char *value_ptr = RSTRING (parameter->vvalue)->ptr;
  int value_len = RSTRING (parameter->vvalue)->len;
  long ucs2strLen = value_len * sizeof (UCS2);
  char *ucs2str = ii_allocate (ucs2strLen + 2 + sizeof (UCS2), sizeof (char));
  int returnValue = 0;
  char function_name[] = "putNVarcharParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (utf8_to_utf16 (value_ptr, value_ptr + value_len, (UCS2 *) ucs2str,
		     (UCS2 *) (ucs2str + ucs2strLen), &ucs2strLen))
    rb_raise (rb_eRuntimeError,
	      "Error! Failed to transcode %s (N) to utf16.\n", value_ptr);
  ucs2strLen *= sizeof (UCS2);

  /* copy the data to a new buffer then set the size of the string in 
   * chars at the begining of the buffer
   * Note: allocate 2 bytes at the start and a null char at the end
   * of the new buffer
   */
  nvarchar = ii_allocate (2 + ucs2strLen + sizeof (UCS2), sizeof (char));
  memcpy (nvarchar + 2, ucs2str, ucs2strLen);
  *((II_INT2 *) (nvarchar)) = (II_INT2) ucs2strLen / sizeof (UCS2);

  returnValue = ii_putParamter (0, FALSE, (II_UINT2) (ucs2strLen + 2), nvarchar);

  ii_free ((void **) &nvarchar);
  ii_free ((void **) &ucs2str);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putNCharParameter (RUBY_PARAMETER * parameter)
{
  char *value_ptr = RSTRING (parameter->vvalue)->ptr;
  int value_len = RSTRING (parameter->vvalue)->len;
  long ucs2strlen = value_len * sizeof (UCS2);
  char *nchar = ii_allocate (ucs2strlen + sizeof (UCS2), sizeof (char));
  int returnValue = 0;
  char function_name[] = "putNCharParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (utf8_to_utf16 (value_ptr, value_ptr + value_len, (UCS2 *) nchar,
		     (UCS2 *) (nchar + ucs2strlen), &ucs2strlen))
    rb_raise (rb_eRuntimeError,
	      "Error! Failed to transcode %s (n) to utf16.\n", value_ptr);
  ucs2strlen *= sizeof (UCS2);

  returnValue = ii_putParamter (0, FALSE, (II_UINT2) ucs2strlen, nchar);

  ii_free ((void **) &nchar);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putVarcharParameter (RUBY_PARAMETER * parameter)
{
  char *value_ptr = RSTRING (parameter->vvalue)->ptr;
  int value_len = RSTRING (parameter->vvalue)->len;
  char *varchar = ii_allocate (value_len + 2 + sizeof (char), sizeof (char));
  int returnValue = 0;
  char function_name[] = "putVarcharParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  /* copy the data to a new buffer then set the size
   * of the string at the begining of the buffer 
   */
  memcpy (varchar + 2, value_ptr, value_len);
  /* set the 1st 2 bytes as the length of the string */
  *((II_INT2 *) (varchar)) = (II_INT2) value_len;

  returnValue = ii_putParamter (0, FALSE, (II_UINT2) (value_len + 2), varchar);

  ii_free ((void **) &varchar);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}

int
putDecimalParameter (RUBY_PARAMETER * parameter)
{
  IIAPI_FORMATPARM formatParm;
  char *value_ptr = RSTRING (parameter->vvalue)->ptr;
  int value_len = RSTRING (parameter->vvalue)->len;
  char decimal[DECIMAL_BUFFER_LEN + 1] = "";
  int returnValue = 0;
  char function_name[] = "putDecimalParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  formatParm.fd_envHandle = ii_globals.envHandle;
  formatParm.fd_srcDesc.ds_dataType = IIAPI_CHA_TYPE;
  formatParm.fd_srcDesc.ds_nullable = FALSE;
  formatParm.fd_srcDesc.ds_length = value_len;
  formatParm.fd_srcDesc.ds_precision = 0;
  formatParm.fd_srcDesc.ds_scale = 0;
  formatParm.fd_srcDesc.ds_columnType = IIAPI_COL_QPARM;
  formatParm.fd_srcDesc.ds_columnName = NULL;
  formatParm.fd_srcValue.dv_null = FALSE;
  formatParm.fd_srcValue.dv_length = value_len;
  formatParm.fd_srcValue.dv_value = value_ptr;
  formatParm.fd_dstDesc.ds_dataType = IIAPI_DEC_TYPE;
  formatParm.fd_dstDesc.ds_nullable = FALSE;
  formatParm.fd_dstDesc.ds_length = DECIMAL_BUFFER_LEN;
  formatParm.fd_dstDesc.ds_precision = DECIMAL_PRECISION;
  formatParm.fd_dstDesc.ds_scale = DECIMAL_SCALE;
  formatParm.fd_dstDesc.ds_columnType = IIAPI_COL_QPARM;
  formatParm.fd_dstDesc.ds_columnName = NULL;
  formatParm.fd_dstValue.dv_null = FALSE;
  formatParm.fd_dstValue.dv_length = DECIMAL_BUFFER_LEN;
  formatParm.fd_dstValue.dv_value = decimal;
  IIapi_formatData (&formatParm);
  if (formatParm.fd_status != IIAPI_ST_SUCCESS)
    {
      rb_raise (rb_eRuntimeError,
		"Error occured converting to DECIMAL. Value supplied was %s",
		value_ptr);
    }

  returnValue = ii_putParamter (0, FALSE, formatParm.fd_dstValue.dv_length, decimal);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putLOBParameter (RUBY_PARAMETER * parameter)
{
  IIAPI_FORMATPARM formatParm;
  II_BOOL moreSegments = 0;
  char *segment = ii_allocate (ii_globals.co_sizeAdvise + 2, sizeof (char));
  char *value_ptr = RSTRING (parameter->vvalue)->ptr;
  long value_len = RSTRING (parameter->vvalue)->len;
  long segment_length = 0;
  int returnValue = 0;
  char function_name[] = "putLOBParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  do
    {
      memset (segment, 0, ii_globals.co_sizeAdvise + 2);

      if (value_len <= ii_globals.co_sizeAdvise)
	{
	  moreSegments = 0;
	  segment_length = value_len;
	}
      else
	{
	  moreSegments = 1;
	  segment_length = ii_globals.co_sizeAdvise;
	}
      /* copy the segment data to a buffer then set the size
       * of the segment at the begining of the buffer 
       */
      memcpy (segment + 2, value_ptr, segment_length);
      /* set the 1st 2 bytes as the length of the segment */
      *((II_UINT2 *) segment) = (II_UINT2) segment_length;

      returnValue =
	ii_putParamter (moreSegments, FALSE, (II_UINT2) (segment_length + 2), segment);

      /* bump pointer for data by segment_length */
      value_ptr += segment_length;
      value_len -= segment_length;
    }
  while (value_len);

  ii_free ((void **) &segment);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putCharParameter (RUBY_PARAMETER * parameter)
{
  int returnValue = 0;
  char function_name[] = "putCharParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  returnValue =
    ii_putParamter (0, FALSE, (II_UINT2) RSTRING (parameter->vvalue)->len,
		    RSTRING (parameter->vvalue)->ptr);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putNullParameter (RUBY_PARAMETER * parameter)
{
  int returnValue = 0;
  char function_name[] = "putNullParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  returnValue =
    ii_putParamter (0, TRUE, (II_UINT2) RSTRING (parameter->vvalue)->len,
		    RSTRING (parameter->vvalue)->ptr);

  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}


int
putParameter (RUBY_PARAMETER * parameter)
{
  int returnValue = 0;
  char function_name[] = "putParameter";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  switch (TYPE (parameter->vvalue))
    {
    case T_FIXNUM:
      returnValue = putRubyFixNumParameter (parameter);
      break;

    case T_FLOAT:
      returnValue = putRubyFloatParameter (parameter);
      break;

    case T_STRING:
      switch ((char) *(RSTRING (parameter->vtype)->ptr))
	{
	case RUBY_NVARCHAR_PARAMETER:
	  returnValue = putNVarcharParameter (parameter);
	  break;

	case RUBY_NCHAR_PARAMETER:
	  returnValue = putNCharParameter (parameter);
	  break;

	case RUBY_VARCHAR_PARAMETER:
	  returnValue = putVarcharParameter (parameter);
	  break;

	case RUBY_DECIMAL_PARAMETER:
	  returnValue = putDecimalParameter (parameter);
	  break;

	case RUBY_LONG_BYTE_PARAMETER:
	case RUBY_LONG_TEXT_PARAMETER:
	case RUBY_LONG_VARCHAR_PARAMETER:
	  returnValue = putLOBParameter (parameter);
	  break;

	case RUBY_INTEGER_PARAMETER:
	case RUBY_BYTE_PARAMETER:
	case RUBY_CHAR_PARAMETER:
	case RUBY_DATE_PARAMETER:
	case RUBY_TEXT_PARAMETER:
	case RUBY_FLOAT_PARAMETER:
	  returnValue = putCharParameter (parameter);
	  break;

	default:		/* everything else */
	  rb_raise (rb_eRuntimeError,
		    "Error putting a parameter of unknown type");
	  break;
	}
      break;

    case T_NIL:		/* TODO need to check this */
      returnValue = putNullParameter (parameter);
      break;

    default:
      rb_raise (rb_eRuntimeError,
		"Error putting a parameter of unknown type");
      break;
    }
  if (ii_globals.debug)
    printf ("Exiting %s, returning %i.\n", function_name, returnValue);
  return (returnValue);
}



/* static short ii_bind_params (VALUE param_params, char *procname, long paramCount) */
/* Binds and sends data for parameters passed via param_params */
/* param_params is expected to be a repeating list of n * [key, type, value] */
static short
ii_bind_params (int param_argc, VALUE param_params, char *procname,
		long paramCount)
{
  IIAPI_SETDESCRPARM setDescrParm;
  IIAPI_PUTPARMPARM putParmParm;
  int param = 0;
  short isProcedureCall = 0;
  char function_name[] = "ii_bind_params";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (ii_globals.debug)
    printf ("%s: argc = %i, paramCount = %li, procedure name = %s.\n",
	    function_name, param_argc, paramCount, procname);

  isProcedureCall = (procname != NULL) ? 1 : 0;
  setDescriptorParms (&setDescrParm, paramCount, isProcedureCall);

  if (isProcedureCall)
    setProcedureNameDescriptor (&(setDescrParm.sd_descriptor[0]), procname);

  /* extract the paramtypes */
  for (param = isProcedureCall; param < setDescrParm.sd_descriptorCount;
       param++)
    {
      RUBY_PARAMETER parameter;

      if (ii_globals.debug)
	printf ("%s: At start of loop for param = %i.\n", function_name,
		param);

      getIIParameter (&parameter, param_params, param, isProcedureCall);
      setParameterDescriptor (&(setDescrParm.sd_descriptor[param]),
			      &parameter, isProcedureCall);
    }

  if (ii_globals.debug)
    printf ("%s: About to set parameter descriptors.\n", function_name);
  IIapi_setDescriptor (&setDescrParm);

  if (ii_checkError (&setDescrParm.sd_genParm))
    rb_raise (rb_eRuntimeError, "Failed to set parameter descriptors.");

  ii_free ((void **) &setDescrParm.sd_descriptor);

  if (ii_globals.debug)
    printf ("%s: About to put parameters.\n", function_name);

  if (isProcedureCall)
    putProcedureNameParameter (procname);

  for (param = isProcedureCall; param < setDescrParm.sd_descriptorCount;
       param++)
    {
      RUBY_PARAMETER parameter;
      getIIParameter (&parameter, param_params, param, isProcedureCall);
      putParameter (&parameter);
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return 0;
}


char *
convertParamMarkers (char *param_sqlText, long param_count)
{
  /* allow for space either side and a null */
  char *new_statement =
    ii_allocate (strlen (param_sqlText) + (param_count * 3) + 1,
		 sizeof (char));
  char function_name[] = "convertParamMarkers";
  int i = 0, j = 0;

  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  for (i = 0; i < strlen (param_sqlText); i++)
    {
      if (param_sqlText[i] == '?')
	{
	  /* check for space before '?' */
	  /* if there is no space before the '~V' */
	  /* ingres will error with "Invalid operator '~V'" */
	  if (param_sqlText[i - 1] != ' ')
	    new_statement[j++] = ' ';
	  new_statement[j++] = '~';
	  new_statement[j++] = 'V';
	  /* check for space before '?' */
	  /* if there is no space after the '~V' */
	  /* ingres will error with "Invalid operator '~V'" */
	  if (param_sqlText[i + 1] != ' ')
	    new_statement[j++] = ' ';
	}
      else
	new_statement[j++] = param_sqlText[i];
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return new_statement;
}



II_PTR
execute (char *param_sqlText, int param_argc, VALUE param_params)
{
  IIAPI_QUERYPARM queryParm;
  char function_name[] = "execute";
  char *procedureName = getProcedureName (param_sqlText);
  char *statement = NULL;
  long paramCount = countParameters (param_sqlText);
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  /*
   ** Call IIapi_query to execute statement.
   */

  if (procedureName == NULL)
    statement = ((paramCount == 0) ?
		 (param_sqlText) :
		 (convertParamMarkers (param_sqlText, paramCount)));
  queryParm.qy_connHandle = ii_globals.connHandle;
  queryParm.qy_genParm.gp_callback = NULL;
  queryParm.qy_genParm.gp_closure = NULL;
  queryParm.qy_queryType =
    ((procedureName != NULL) ? IIAPI_QT_EXEC_PROCEDURE : IIAPI_QT_QUERY);
  queryParm.qy_queryText = ((procedureName == NULL) ? statement : NULL);
  queryParm.qy_parameters = ((param_argc > 0) ? TRUE : FALSE);
  queryParm.qy_tranHandle = ii_globals.tranHandle;
  queryParm.qy_stmtHandle = NULL;

  IIapi_query (&queryParm);
  ii_sync (&(queryParm.qy_genParm));
  ii_globals.stmtHandle = queryParm.qy_stmtHandle;

  if (param_argc > 0
      && ii_bind_params (param_argc, param_params, procedureName, paramCount))
    {
      rb_raise (rb_eRuntimeError, "Error binding parameters.");
    }

  if (ii_globals.debug)
    printf ("%s: Query status is >>%d<<\n", function_name,
	    queryParm.qy_genParm.gp_status);

  if (ii_globals.tranHandle == NULL)
    ii_globals.tranHandle = queryParm.qy_tranHandle;

  if (statement != param_sqlText)
    ii_free ((void **) &statement);
  ii_free ((void **) &procedureName);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return queryParm.qy_stmtHandle;
}


void
getColumnNamesAndTypes (IIAPI_GETDESCRPARM * param_descrParm)
{
  int i;
  char function_name[] = "getColumnNamesAndTypes";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  for (i = 0; i < param_descrParm->gd_descriptorCount; i++)
    {
      rb_ary_push (global_r_data_types,
		   rb_str_new2 (getIngresDataTypeAsString
				(param_descrParm->gd_descriptor[i].
				 ds_dataType)));

      rb_ary_push (global_r_column_names,
		   rb_str_new2 (param_descrParm->gd_descriptor[i].
				ds_columnName));

      if (param_descrParm->gd_descriptor[i].ds_dataType ==
	  (IIAPI_DT_ID) RUBY_UNMAPPED)
	{
	  // we've gotten something not yet handled.
	  // let's print out some information to help with the debugging process
	  printf
	    ("%s: An unmapped data type was encountered.\nThe column name is %s\n",
	     function_name, param_descrParm->gd_descriptor[i].ds_columnName);
	}
    }
  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


void
getDescriptors (IIAPI_GETDESCRPARM * param_descrParm)
{
  char function_name[] = "getDescriptors";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  param_descrParm->gd_genParm.gp_callback = NULL;
  param_descrParm->gd_genParm.gp_closure = NULL;
  param_descrParm->gd_stmtHandle = ii_globals.stmtHandle;
  param_descrParm->gd_descriptorCount = 0;
  param_descrParm->gd_descriptor = NULL;

  IIapi_getDescriptor (param_descrParm);

  ii_sync (&(param_descrParm->gd_genParm));

  if (ii_globals.debug)
    printf ("%s: GetDescriptor status is **%d**", function_name,
	    param_descrParm->gd_genParm.gp_status);

  if (ii_checkError (&(param_descrParm->gd_genParm)))
    {
      closeQuery (ii_globals.stmtHandle);
      rollback ();
      rb_raise (rb_eRuntimeError,
		"Error! Failed while getting Descriptors. ");
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


void
closeQuery ()
{
  IIAPI_CLOSEPARM closeParm;
  char function_name[] = "closeQuery";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  closeParm.cl_genParm.gp_callback = NULL;
  closeParm.cl_genParm.gp_closure = NULL;
  closeParm.cl_stmtHandle = ii_globals.stmtHandle;

  IIapi_close (&closeParm);

  ii_sync (&(closeParm.cl_genParm));

  if (ii_globals.debug)
    printf ("%s: closeParm status is >>%d<<", function_name,
	    closeParm.cl_genParm.gp_status);
  ii_checkError (&closeParm.cl_genParm);

  ii_globals.stmtHandle = NULL;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


II_LONG
getRowsAffected ()
{
  IIAPI_GETQINFOPARM getQInfoParm;
  char function_name[] = "getRowsAffected";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  getQInfoParm.gq_genParm.gp_callback = NULL;
  getQInfoParm.gq_genParm.gp_closure = NULL;
  getQInfoParm.gq_stmtHandle = ii_globals.stmtHandle;

  IIapi_getQueryInfo (&getQInfoParm);

  ii_sync (&(getQInfoParm.gq_genParm));

  if (ii_globals.debug)
    printf ("%s: GetQueryInfo status is >>%d<<", function_name,
	    getQInfoParm.gq_genParm.gp_status);
  ii_checkError (&getQInfoParm.gq_genParm);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return (II_LONG) getQInfoParm.gq_rowCount;
}


VALUE
processDateField (IIAPI_DATAVALUE * param_columnData, int param_dataType)
{
  VALUE returnValue;
  II_LONG paramvalue = IIAPI_EPV_DFRMT_ISO;
  IIAPI_SETENVPRMPARM setEnvPrmParm;
  IIAPI_CONVERTPARM convertParm;
  int dateStrLen = 260;
  char *dateStr = NULL;
  char function_name[] = "processDateField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  dateStr = ii_allocate (dateStrLen + 1, sizeof (char));

  if (ii_globals.debug)
    printf ("%s: Found a DATE or TIME field of type %d >>%s<<\n", function_name,
	    param_dataType, param_columnData->dv_value);

  setEnvPrmParm.se_envHandle = ii_globals.envHandle;
  setEnvPrmParm.se_paramID = IIAPI_EP_DATE_FORMAT;
  setEnvPrmParm.se_paramValue = (II_PTR) (&paramvalue);
  IIapi_setEnvParam (&setEnvPrmParm);

  convertParm.cv_srcDesc.ds_dataType = param_dataType;
  convertParm.cv_srcDesc.ds_nullable = FALSE;
  convertParm.cv_srcDesc.ds_length = param_columnData->dv_length;
  convertParm.cv_srcDesc.ds_precision = 0;
  convertParm.cv_srcDesc.ds_scale = 0;
  convertParm.cv_srcDesc.ds_columnType = IIAPI_COL_QPARM;
  convertParm.cv_srcDesc.ds_columnName = NULL;

  convertParm.cv_srcValue.dv_null = FALSE;
  convertParm.cv_srcValue.dv_length = param_columnData->dv_length;
  convertParm.cv_srcValue.dv_value = param_columnData->dv_value;

  convertParm.cv_dstDesc.ds_dataType = IIAPI_CHA_TYPE;
  convertParm.cv_dstDesc.ds_nullable = FALSE;
  convertParm.cv_dstDesc.ds_length = dateStrLen;
  convertParm.cv_dstDesc.ds_precision = 0;
  convertParm.cv_dstDesc.ds_scale = 0;
  convertParm.cv_dstDesc.ds_columnType = IIAPI_COL_QPARM;
  convertParm.cv_dstDesc.ds_columnName = NULL;

  convertParm.cv_dstValue.dv_null = FALSE;
  convertParm.cv_dstValue.dv_length = dateStrLen;
  convertParm.cv_dstValue.dv_value = dateStr;

  IIapi_convertData (&convertParm);

  dateStr[convertParm.cv_dstValue.dv_length] = '\0';
  if (ii_globals.debug)
    printf ("%s: Converted the DATE/TIME field >>%s<< to the string >>%s<<\n",
	    function_name, param_columnData->dv_value, dateStr);

  returnValue = rb_str_new (dateStr, convertParm.cv_dstValue.dv_length);
  ii_free ((void **) &dateStr);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return returnValue;
}


VALUE
processDecimalField (IIAPI_DATAVALUE * param_columnData, IIAPI_DESCRIPTOR * param_descrParm)
{
  VALUE returnValue;
  IIAPI_CONVERTPARM convertParm;
  int decimalStrLen = 42;
  /* FIXME: use Ingres MAX_DECIMAL_LEN or something like that */
  char decimalStr[42] = {0};
  char function_name[] = "processDecimalField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  convertParm.cv_srcDesc.ds_dataType = IIAPI_DEC_TYPE;
  convertParm.cv_srcDesc.ds_nullable = FALSE;
  convertParm.cv_srcDesc.ds_length = param_descrParm->ds_length;
  convertParm.cv_srcDesc.ds_precision = param_descrParm->ds_precision;
  convertParm.cv_srcDesc.ds_scale = param_descrParm->ds_scale;
  convertParm.cv_srcDesc.ds_columnType = IIAPI_COL_QPARM;
  convertParm.cv_srcDesc.ds_columnName = NULL;

  convertParm.cv_srcValue.dv_null = FALSE;
  convertParm.cv_srcValue.dv_length = param_columnData->dv_length;
  convertParm.cv_srcValue.dv_value = param_columnData->dv_value;

  convertParm.cv_dstDesc.ds_dataType = IIAPI_VCH_TYPE;
  convertParm.cv_dstDesc.ds_nullable = FALSE;
  convertParm.cv_dstDesc.ds_length = decimalStrLen;
  convertParm.cv_dstDesc.ds_precision = 0;
  convertParm.cv_dstDesc.ds_scale = 0;
  convertParm.cv_dstDesc.ds_columnType = IIAPI_COL_QPARM;
  convertParm.cv_dstDesc.ds_columnName = NULL;

  convertParm.cv_dstValue.dv_null = FALSE;
  convertParm.cv_dstValue.dv_length = decimalStrLen;
  convertParm.cv_dstValue.dv_value = decimalStr;

  IIapi_convertData (&convertParm);

  returnValue = rb_str_new2 (decimalStr+2);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return returnValue;
}


/*
**      processIntField() - Convert Ingres integer to Ruby numeric
**
**      Description - 
**              Convert Ingres integer (lengths 1, 2, 4 or 8) to Ruby
**              numeric (Fixnum or Bignum).
**
**              Ruby numeric types w/ ranges:
**                 Fixnum  -2**30      ->    2**30-1    #most platforms
**                    (-1,073,741,824)   (1,073,741,823)
**                         -2**62      ->    2**62-1    #some 64-bit? platforms
**                 Bignum: unlimited...anything outside range of Fixnum
**
**              Ingres integer types w/ ranges and mapping to Ruby:
**                 tinyint(integer1)  -128 -> 127                  Fixnum
**                 smallint(integer2) -32768 -> 32767              Fixnum
**                 integer(integer4)  -2**31     ->    2**31-1     Fixnum/Bignum
**                               (-2,147,483,648)  (2,147,483,647)
**                 bigint(integer8)   -2**63     ->    2**63-1     Fixnum/Bignum
**
**              The Ruby macros xxx2NUM/FIX are used to do the conversion.
**              If the Ingres type will always fit within a Ruby Fixnum,
**              then use INT2FIX because it is faster.  Otherwise, use the
**              appropriate xxx2NUM macro which will convert it to either
**              Fixnum or Bignum depending on the value.
**
**      History
**              06/18/08 (lunbr01)
**                      Added Ingres bigint support and used INT2FIX 
**                      instead of INT2NUM where possible for performance.
**                      Add function documentation.
*/
VALUE
processIntField (IIAPI_DATAVALUE * param_columnData)
{
  VALUE ret_val;
  char function_name[] = "processIntField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  switch (param_columnData->dv_length)
    {
    case 1:
      ret_val = INT2FIX ((char) *((II_INT1 *) param_columnData->dv_value));
      //printf("case 1 1 m_current_database is %s \n",m_current_database);
      break;
    case 2:
      ret_val = INT2FIX ((short) *((II_INT2 *) param_columnData->dv_value));
      //printf("case 2 1 m_current_database is %s \n",m_current_database);
      break;
    case 4:
      ret_val = LONG2NUM ((long) *((II_INT4 *) param_columnData->dv_value));
      //printf("case 4 1 m_current_database is %s \n",m_current_database);
      break;
    case 8:
      ret_val = LL2NUM ((__int64) *((__int64 *) param_columnData->dv_value));
      //printf("case 8 1 m_current_database is %s \n",m_current_database);
      break;
    default:
      if (ii_globals.debug)
	printf
	  ("%s: Bad size for IIAPI_INT_TYPE. The size %d is invalid. Returning NULL.\n",
	   function_name, param_columnData->dv_length);
      // if the data size is zero, this is a NULL value.
      ret_val = rb_str_new2 ("NULL");
      break;
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


VALUE
processFloatField (IIAPI_DATAVALUE * param_columnData)
{
  VALUE ret_val;
  char function_name[] = "processFloatField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  switch (param_columnData->dv_length)
    {
    case 4:
      ret_val =
	rb_float_new ((double) *((II_FLOAT4 *) param_columnData->dv_value));
      break;

    case 8:
      ret_val =
	rb_float_new ((double) *((II_FLOAT8 *) param_columnData->dv_value));
      break;

    default:
      if (ii_globals.debug)
	printf
	  ("%s: Bad size for IIAPI_FLT_TYPE. The size %d is invalid. Valid sizes are 4 and 8. Returning NULL.",
	   function_name, param_columnData->dv_length);
      ret_val = rb_str_new2 ("NULL");;
      break;
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


VALUE
processMoneyField (IIAPI_DATAVALUE * param_columnData)
{
  VALUE ret_val;
  char function_name[] = "processMoneyField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  ret_val = rb_float_new ((double) *((II_FLOAT8 *) param_columnData->dv_value) / 100.00);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}



VALUE
processCharField (char *param_char_field, int param_char_length)
{
  VALUE ret_val;
  int trimmed_len = 0;
  char *newstring = ii_allocate ((param_char_length + 1), sizeof (char));
  char function_name[] = "processCharField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  memcpy (newstring, param_char_field, param_char_length);

  if (ii_globals.debug)
    printf ("oldchar is >>%s<<, newchar is >>%s<<\n", param_char_field,
	    newstring);

  // remove any trailing blanks
  trimmed_len = STtrmwhite (newstring);
  ret_val = rb_str_new (newstring, trimmed_len);
  ii_free ((void **) &newstring);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


VALUE
processStringField (char *param_varchar_field, int param_varchar_length)
{
  VALUE result_value;
  int i;
  char *newstring = ii_allocate (param_varchar_length - 1, sizeof (char));
  char function_name[] = "processStringField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  // the first two bytes holds the length.
  // since we already have that, we don't need it,
  // so we just skip over that information.
  memcpy (newstring, param_varchar_field + 2, param_varchar_length - 2);

  if (ii_globals.debug)
    printf ("oldstring is >>%s<<, newstring is >>%s<<\n", param_varchar_field,
	    newstring);

  // make a Ruby value out of the result
  result_value = rb_str_new (newstring, param_varchar_length - 2);
  ii_free ((void **) &newstring);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return result_value;
}

VALUE
processUTF16StringField (char *param_nvarchar_field,
			 int param_nvarchar_length)
{
  VALUE result_value;
  int i, ret_val;
  long utf8strlen;
  unsigned char *utf8str = NULL;
  char function_name[] = "processUTF16StringField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  utf8strlen = param_nvarchar_length * 4;
  utf8str = ii_allocate (utf8strlen + 3, sizeof (char));

  if (utf16_to_utf8
      ((UCS2 *) (param_nvarchar_field + 2),
       (UCS2 *) (param_nvarchar_field + param_nvarchar_length),
       (char *) (utf8str + 2), (char *) (utf8str + utf8strlen + 2),
       &utf8strlen))
    rb_raise (rb_eRuntimeError,
	      "Transcode of UTF16 %s, string to UTF failed.",
	      param_nvarchar_field);

  result_value = processStringField (utf8str, utf8strlen + 2);

  ii_free ((void **) &utf8str);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return result_value;
}


VALUE
processUTF16CharField (char *param_nchar_field, int param_nchar_length)
{
  VALUE result_value;
  long utf8strlen;
  unsigned char *utf8str = NULL;
  char function_name[] = "processUTF16CharField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  utf8strlen = param_nchar_length * 4;
  utf8str = ii_allocate (utf8strlen + 1, sizeof (char));

  if (utf16_to_utf8
      ((UCS2 *) param_nchar_field,
       (UCS2 *) (param_nchar_field + param_nchar_length), (char *) utf8str,
       (char *) (utf8str + utf8strlen), &utf8strlen))
    rb_raise (rb_eRuntimeError, "Transcode of UTF16 %s, char to UTF failed.",
	      param_nchar_field);

  result_value = processCharField (utf8str, utf8strlen);

  ii_free ((void **) &utf8str);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return result_value;
}


VALUE
processUTF16LOBField (char *param_nlob_field, int param_nlob_length)
{
  VALUE result_value;
  long utf8strlen;
  unsigned char *utf8str = NULL;
  char function_name[] = "processUTF16LOBField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  utf8strlen = param_nlob_length * 4;
  utf8str = ii_allocate (utf8strlen + 1, sizeof (char));

  if (utf16_to_utf8
      ((UCS2 *) param_nlob_field,
       (UCS2 *) (param_nlob_field + param_nlob_length), (char *) utf8str,
       (char *) (utf8str + utf8strlen), &utf8strlen))
    rb_raise (rb_eRuntimeError, "Transcode of UTF16 %s, char to UTF failed.",
	      param_nlob_field);

  result_value = rb_str_new (utf8str, utf8strlen);

  ii_free ((void **) &utf8str);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return result_value;
}


VALUE
processLOBField (char *param_nlob_field, int param_nlob_length)
{
  VALUE result_value;
  char function_name[] = "processLOBField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  result_value = rb_str_new (param_nlob_field, param_nlob_length);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return result_value;
}


VALUE
processUnImplementedField ()
{
  char function_name[] = "processUnImplementedField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  rb_notimplement ();

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return rb_str_new2 ("NULL");
}


VALUE
processField (RUBY_IIAPI_DATAVALUE * param_columnData, int param_columnNumber,
	      IIAPI_DESCRIPTOR * param_descrParm)
{
  VALUE ret_val;
  IIAPI_DATAVALUE *dataValue = param_columnData->dataValue;
  int param_dataType = param_descrParm->ds_dataType;
  char function_name[] = "processField";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  rb_ary_store (global_r_data_sizes, param_columnNumber,
		INT2NUM (param_columnData->dv_length));

  // fetch the data differently depending on the type of data it is
  switch (param_dataType)
    {
      // I followed the Ingres doc for these types
      // anything listed as a char * will be treated as varchar or text
    case IIAPI_NVCH_TYPE:
      ret_val = processUTF16StringField (dataValue->dv_value,
					 param_columnData->dv_length);
      break;

    case IIAPI_LNVCH_TYPE:
      ret_val = processUTF16LOBField (dataValue->dv_value,
				      param_columnData->dv_length);
      break;

    case IIAPI_LBYTE_TYPE:
    case IIAPI_LVCH_TYPE:
      ret_val = processLOBField (dataValue->dv_value,
				 param_columnData->dv_length);
      break;

    case IIAPI_BYTE_TYPE:
    case IIAPI_LOGKEY_TYPE:
    case IIAPI_TABKEY_TYPE:
    case IIAPI_VBYTE_TYPE:
      // tested
    case IIAPI_TXT_TYPE:
    case IIAPI_VCH_TYPE:
      ret_val = processStringField (dataValue->dv_value,
				    param_columnData->dv_length);
      break;

    case IIAPI_INT_TYPE:
      ret_val = processIntField (dataValue);
      break;

    case IIAPI_DEC_TYPE:
      ret_val = processDecimalField (dataValue, param_descrParm);
      break;

    case IIAPI_FLT_TYPE:
      ret_val = processFloatField (dataValue);
      break;

    case IIAPI_DTE_TYPE:
#ifdef IIAPI_DATE_TYPE
    case IIAPI_DATE_TYPE:
    case IIAPI_TIME_TYPE:
    case IIAPI_TMWO_TYPE:
    case IIAPI_TMTZ_TYPE:
    case IIAPI_TS_TYPE:
    case IIAPI_TSWO_TYPE:
    case IIAPI_TSTZ_TYPE:
    case IIAPI_INTYM_TYPE:
    case IIAPI_INTDS_TYPE:
#endif
      ret_val = processDateField (dataValue, param_dataType);
      break;

    case IIAPI_MNY_TYPE:
      ret_val = processMoneyField (dataValue);
      break;

    case IIAPI_NCHA_TYPE:
      ret_val = processUTF16CharField (dataValue->dv_value,
				       param_columnData->dv_length);
      break;

    case IIAPI_CHR_TYPE:
    case IIAPI_CHA_TYPE:
    default:
      ret_val = processCharField (dataValue->dv_value,
				  param_columnData->dv_length);
    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


int
getColumn (RUBY_IIAPI_DATAVALUE * param_columnData)
{
  IIAPI_GETCOLPARM getColParm;
  IIAPI_DATAVALUE *dataValue = param_columnData->dataValue;
  int status = 0;
  long bufferLen = 0;
  char *buffer = NULL;
  char function_name[] = "getColumn";

  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  getColParm.gc_columnData = dataValue;

  do
    {
      getColParm.gc_genParm.gp_callback = NULL;
      getColParm.gc_genParm.gp_closure = NULL;
      getColParm.gc_rowCount = 1;
      getColParm.gc_columnCount = 1;
      getColParm.gc_rowsReturned = 0;
      getColParm.gc_stmtHandle = ii_globals.stmtHandle;
      getColParm.gc_moreSegments = 0;
      dataValue->dv_length = 0;

      IIapi_getColumns (&getColParm);
      ii_sync (&(getColParm.gc_genParm));
      if (ii_checkError (&(getColParm.gc_genParm)))
	{
	  memset (dataValue, 0, sizeof (IIAPI_DATAVALUE));
	  rb_raise (rb_eRuntimeError, "IIapi_getColumns() failed.");
	  break;
	}

      param_columnData->dv_length = dataValue->dv_length;
      if (getColParm.gc_moreSegments || buffer != NULL)
	{
	  short int segmentLen;
	  long newBufferLen;

	  memcpy ((char *) &segmentLen, dataValue->dv_value, 2);
	  newBufferLen = bufferLen + segmentLen;
	  /*
	  ** TODO Improve performance by not reallocating new larger
	  ** buffer for every lob segment (approx 4K bytes each).  See 
	  ** Ingres ODBC driver for more efficient algorithm.
	  */
	  buffer = ii_reallocate (buffer, newBufferLen + 1, sizeof (char));
	  memcpy (buffer + bufferLen, (char *)dataValue->dv_value + 2, segmentLen);
	  bufferLen = newBufferLen;
	  param_columnData->dv_length = bufferLen;
	}
    }
  while (getColParm.gc_moreSegments);

  if (buffer != NULL)   /* If blob col, return alloc'd buffer */
    dataValue->dv_value = buffer;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return getColParm.gc_genParm.gp_status;
}


int
processColumn (VALUE * param_values,
	       int param_columnNumber, IIAPI_DESCRIPTOR * param_descrParm)
{
  RUBY_IIAPI_DATAVALUE columnData;
  int done = FALSE;
  char tmp[MAX_CHAR_SIZE + 2];  /* More than enuf for blob segments as well */
  char function_name[] = "processColumn";

  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  columnData.dataValue[0].dv_value = tmp;

  if (getColumn (&columnData) >= IIAPI_ST_NO_DATA)
    {
      // we've reached the end of the data
      done = TRUE;
    }
  else
    {
      // let's copy out and convert the data
      VALUE nextEntry;

      if (columnData.dataValue[0].dv_null == TRUE)
	{
	  // this is a null value. Don't try to convert it.
	  if (ii_globals.debug)
	    printf ("\nFound a NULL value\n");
	  nextEntry = rb_str_new2 ("NULL");
	  rb_ary_store (global_r_data_sizes, param_columnNumber, INT2FIX (0));
	}
      else
	{
	  nextEntry =
	    processField (&columnData, param_columnNumber,
			  param_descrParm);
	}

      rb_ary_push ((*param_values), nextEntry);
    }

  if (columnData.dataValue[0].dv_value != tmp)  /* if lob... */
    ii_free ((void **) &(columnData.dataValue[0].dv_value));

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return done;
}


void
processResultSet (IIAPI_GETDESCRPARM * param_descrParm)
{
  int done = FALSE;
  char function_name[] = "processResultSet";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  // loop until all rows are fetched
  while (!done)
    {
      VALUE values;
      int columnCount = 0;

      // set up the Ruby variable to hold this row of data
      if (GLOBAL_TABLE_LIST_QUERY_FLAG)
	values = global_table_list;
      else
	{
	  values = rb_ary_new ();
	  rb_ary_push (global_keep_me, values);
	}

      while (columnCount < param_descrParm->gd_descriptorCount && !done)
	{
	  done =
	    processColumn (&values, columnCount,
			   &(param_descrParm->gd_descriptor[columnCount]));
	  columnCount++;
	}

      if (!done && !GLOBAL_TABLE_LIST_QUERY_FLAG)
	rb_ary_push (global_resultset, values);

    }

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


VALUE
query (char *param_sqlText)
{
  IIAPI_GETDESCRPARM getDescrParm;
  IIAPI_WAITPARM waitParm = { -1 };
  VALUE ret_val;
  char function_name[] = "query";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (ii_globals.debug)
    printf ("\n AUTOCOMMIT_ON = %d\n", ii_globals.autocommit);

  init_globals ();

  executeQuery (param_sqlText);
  getDescriptors (&getDescrParm);

  if (ii_globals.debug)
    printf ("\nFound %d column(s)\n", getDescrParm.gd_descriptorCount);

  // fetch the query results
  if (getDescrParm.gd_descriptorCount > 0)
    {
      getColumnNamesAndTypes (&getDescrParm);
      processResultSet (&getDescrParm);
    }

  if (GLOBAL_TABLE_LIST_QUERY_FLAG)
    ret_val = global_table_list;
  else
    ret_val = global_resultset;

  global_rows_affected = getRowsAffected ();

  closeQuery ();

  if (ii_globals.autocommit)
    commit ();

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


VALUE
procedureOrDML (char *param_sqlText, int param_argc, VALUE param_params)
{
  IIAPI_GETDESCRPARM getDescrParm;
  IIAPI_WAITPARM waitParm = { -1 };
  VALUE ret_val;
  char function_name[] = "procedureOrDML";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (ii_globals.debug)
    printf ("\n AUTOCOMMIT_ON = %d\n", ii_globals.autocommit);

  init_globals ();

  execute (param_sqlText, param_argc, param_params);
  getDescriptors (&getDescrParm);

  if (ii_globals.debug)
    printf ("\nFound %d column(s)\n", getDescrParm.gd_descriptorCount);

  // fetch the query results
  if (getDescrParm.gd_descriptorCount > 0)
    {
      getColumnNamesAndTypes (&getDescrParm);
      processResultSet (&getDescrParm);
    }

  if (GLOBAL_TABLE_LIST_QUERY_FLAG)
    ret_val = global_table_list;
  else
    ret_val = global_resultset;

  global_rows_affected = getRowsAffected ();

  closeQuery ();

  if (ii_globals.autocommit)
    commit ();

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


/*
** Name: ii_execute
**
** Description:
**  Execute an SQL statement and the result set avaiable
**
** Input:
**  queryText  SQL query text
**
** Output:
**  Shared variables with Ruby are:
**    data_types - the text name of each column's data type (as translated by this code)
**    resultset - this is both the return value and shared with Ruby
**
** Return value:
**  resultset, a Ruby datastructure.
**   it contains an Array(a1) of Arrays(a2).
**  a1 has one entry per row in the result set
**  a2 has one entry per column in the result set
** TODO : remove the need to uppercase the query text
*/
VALUE
ii_execute (VALUE param_self, VALUE param_queryText)
{
  VALUE ret_val;
  int i;
  char *sqlText =
    ii_allocate (RSTRING (param_queryText)->len + 1, sizeof (char));
  char *sqlTextUpper =
    ii_allocate (RSTRING (param_queryText)->len + 1, sizeof (char));
  char function_name[] = "ii_execute";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  if (ii_globals.debug)
    printf ("\n in ii_execute \n");

  memcpy (sqlText, StringValuePtr (param_queryText),
	  RSTRING (param_queryText)->len);
  memcpy (sqlTextUpper, StringValuePtr (param_queryText),
	  RSTRING (param_queryText)->len);

  for (i = 0; i < RSTRING (param_queryText)->len; i++)
    {
      if (isalpha (sqlTextUpper[i]))
	sqlTextUpper[i] = toupper (sqlTextUpper[i]);
    }

  if (ii_globals.debug || ii_globals.debug_transactions
      || ii_globals.debug_sql)
    printf ("\nexecuting >%s<.\n", sqlText);

  // check to see if it's a request to start a transaction
  if (strcmp (sqlTextUpper, "START TRANSACTION") == 0)
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\n\n++++++++++++ Found START TRANSACTION ++++++++++++++++\n\n");
      startTransaction (param_self);
      ret_val = Qnil;
    }
  // if the SQL is "COMMIT", commit the outstanding transactions
  else if (strcmp (sqlTextUpper, "COMMIT") == 0)
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf ("\n\n-------------- Found COMMIT ---------------\n\n");
      if (ii_globals.tranHandle != NULL)
	commit ();
      else if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\nWarning! Attempting to commit a non-existent transaction.\n\n");
      ret_val = Qnil;
    }
  // if the SQL is "ROLLBACK", commit the outstanding transactions
  else if (strcmp (sqlTextUpper, "ROLLBACK") == 0)
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\n\n################### Found ROLLBACK ###################\n\n");
      if (ii_globals.tranHandle != NULL)
	rollback ();
      else if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\nError! Attempting to rollback a non-existent transaction.\n\n");
      ret_val = Qnil;
    }
  else if (strstr (sqlTextUpper, "{CALL ") != NULL
	   || strstr (sqlTextUpper, "{EXECUTE PROCEDURE ") != NULL)
    {
      rb_raise (rb_eRuntimeError,
		"Error! Use pexecute for stored procedures. ");
    }
  // finally, process dml (Inserts, Updates, Deletes, etc)
  else
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf ("\n\n******************* Found DML *******************\n\n");
      ret_val = query (sqlText);
    }
  ii_free ((void **) &sqlTextUpper);
  ii_free ((void **) &sqlText);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


/*
** Name: ii_pexecute
**
** Description:
**  Execute an SQL statement and the result set avaiable
**
** Input:
**  queryText  SQL query text
**
** Output:
**  Shared variables with Ruby are:
**    data_types - the text name of each column's data type (as translated by this code)
**    resultset - this is both the return value and shared with Ruby
**
** Return value:
**  resultset, a Ruby datastructure.
**   it contains an Array(a1) of Arrays(a2).
**  a1 has one entry per row in the result set
**  a2 has one entry per column in the result set
*/
VALUE
ii_pexecute (int param_argc, VALUE * param_argv, VALUE param_self)
{
  VALUE ret_val, queryText, params;
  int i;
  char *sqlText = NULL;
  char *sqlTextUpper = NULL;
  char function_name[] = "ii_pexecute";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  rb_scan_args (param_argc, param_argv, "1*", &queryText, &params);
  
  sqlText = ii_allocate (RSTRING (queryText)->len + 1, sizeof (char));
  sqlTextUpper = ii_allocate (RSTRING (queryText)->len + 1, sizeof (char));

  memcpy (sqlText, StringValuePtr (queryText), RSTRING (queryText)->len);
  memcpy (sqlTextUpper, StringValuePtr (queryText), RSTRING (queryText)->len);

  for (i = 0; i < RSTRING (queryText)->len; i++)
    {
      if (isalpha (sqlTextUpper[i]))
	sqlTextUpper[i] = toupper (sqlTextUpper[i]);
    }

  if (ii_globals.debug || ii_globals.debug_transactions
      || ii_globals.debug_sql)
    printf ("\nexecuting >%s<.\n", sqlText);

  // check to see if it's a request to start a transaction
  if (strcmp (sqlTextUpper, "START TRANSACTION") == 0)
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\n\n++++++++++++ Found START TRANSACTION ++++++++++++++++\n\n");
      startTransaction (param_self);
      ret_val = Qnil;
    }
  // if the SQL is "COMMIT", commit the outstanding transactions
  else if (strcmp (sqlTextUpper, "COMMIT") == 0)
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf ("\n\n-------------- Found COMMIT ---------------\n\n");
      if (ii_globals.tranHandle != NULL)
	commit ();
      else if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\nWarning! Attempting to commit a non-existent transaction.\n\n");
      ret_val = Qnil;
    }
  // if the SQL is "ROLLBACK", commit the outstanding transactions
  else if (strcmp (sqlTextUpper, "ROLLBACK") == 0)
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\n\n################### Found ROLLBACK ###################\n\n");
      if (ii_globals.tranHandle != NULL)
	rollback ();
      else if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\nError! Attempting to rollback a non-existent transaction.\n\n");
      ret_val = Qnil;
    }
  else
    {
      if (ii_globals.debug || ii_globals.debug_transactions)
	printf
	  ("\n\n=================== Found Other ===================\n\n");
      ret_val = procedureOrDML (sqlText, param_argc - 1, params);
    }
  ii_free ((void **) &sqlTextUpper);
  ii_free ((void **) &sqlText);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return ret_val;
}


//return a list of all the tables in this database
static VALUE
ii_tables (VALUE param_self)
{
  VALUE table_list;
  VALUE sql_string;
  char function_name[] = "ii_tables";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  sql_string = rb_str_new2 ("SELECT table_name FROM iitables WHERE table_type = 'T' AND table_name not like 'ii%'");
  GLOBAL_TABLE_LIST_QUERY_FLAG = TRUE;
  table_list = ii_execute (param_self, sql_string);
  GLOBAL_TABLE_LIST_QUERY_FLAG = FALSE;
  rb_ary_push (global_keep_me, table_list);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return table_list;
}

/* return the current database (if one is open) */

VALUE
ii_current_database (VALUE param_self)
{
  VALUE curr_db = rb_str_new2 (ii_globals.currentDatabase);
  char function_name[] = "ii_current_database";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  rb_ary_push (global_keep_me, curr_db);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return curr_db;
}


VALUE
ii_crash_it (VALUE param_self)
{
  VALUE a_string;
  VALUE yet_another_string;
  char *my_string = NULL;
  char *another_string = NULL;
  char function_name[] = "ii_crash_it";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  printf ("\n\nMalloc some memory\n");
  my_string = malloc (strlen ("activerecord_unittest") + 1);
  my_string = "activerecord_unittest";

  printf ("\n Create the Ruby string \n");
  a_string = rb_str_new2 (my_string);

  printf ("Now convert it to a C string\n");
  another_string = STR2CSTR (a_string);

  printf ("Now convert it back to a Ruby string\n");
  yet_another_string = rb_str_new2 (another_string);

  ii_free ((void **) &my_string);
  ii_free ((void **) &another_string);
  printf ("Done!");

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return yet_another_string;
}


/* how many rows were affected by the last sql statement? */
VALUE
ii_rows_affected (VALUE param_self)
{
  VALUE return_value;
  char function_name[] = "ii_rows_affected";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  return_value = INT2NUM (global_rows_affected);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return return_value;
}


VALUE
ii_return_data_types ()
{
  char function_name[] = "ii_return_data_types";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  return global_r_data_types;
}


VALUE
ii_column_names (VALUE param_self)
{
  char function_name[] = "ii_column_names";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  return global_r_column_names;
}


VALUE
ii_data_sizes (VALUE param_self)
{
  char function_name[] = "ii_data_sizes";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);


  return global_r_data_sizes;
}


VALUE
ii_test_default (VALUE param_self)
{
  // test the local database
  int loop = 0;
  int max_count = 2;
  char function_name[] = "ii_test_default";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  ii_connect (param_self, rb_str_new2 ("activerecord_unittest"));
  while (loop < max_count)
    {
      ii_execute (param_self, rb_str_new2 ("START TRANSACTION"));

      // a random query
      ii_execute (param_self, rb_str_new2 ("select * from iicolumns"));

      // drop the table or two

      // ii_execute(self, rb_str_new2("drop table cusomters"));
      // ii_execute(self, rb_str_new2("DROP TABLE orders"));

      // create the customer tables again
      //ii_execute(self, rb_str_new2("CREATE TABLE customers ( id integer NOT NULL, name char(50),balance integer default 0,address_street char(50),address_city char(50), address_country char(50), gps_location char(50),constraint pk_customers primary key (id)) with page_size=4096"));

      //ii_execute(self, rb_str_new2("CREATE TABLE orders (id integer NOT NULL, name char(50), billing_customer_id integer, shipping_customer_id integer, constraint pk_orders primary key (id)) with page_size=4096"));

      ii_execute (param_self, rb_str_new2 ("delete from customers"));
      ii_execute (param_self, rb_str_new2 ("delete from orders"));
      // now use it
      ii_execute (param_self,
		  rb_str_new2
		  ("INSERT INTO customers (address_country, address_city, name, address_street, id, gps_location, balance) VALUES ('Loony Land', 'Scary Town', 'David', 'Funny Street', 1, '35.544623640962634x-105.9309951055148', 50)"));
      ii_execute (param_self,
		  rb_str_new2
		  ("INSERT INTO customers (address_country, address_city, name, address_street, id, gps_location, balance) VALUES ('Loony Land2', 'Scary Town3', 'David2', 'Funny Streets', 2, '32.544623640962634x-105.9309951055148', 51)"));

      ii_execute (param_self,
		  rb_str_new2
		  ("UPDATE customers SET name = 'David', address_country = 'Loony Land', address_city = 'Scary Town', address_street = 'Funny Street', gps_location = '35.544623640962634x-105.9309951055148', balance = 100 WHERE id = 1"));

      ii_execute (param_self,
		  rb_str_new2
		  ("INSERT INTO orders (name, shipping_customer_id, id, billing_customer_id) VALUES(NULL, NULL, 1, NULL)"));

      ii_execute (param_self,
		  rb_str_new2
		  ("INSERT INTO orders (name, shipping_customer_id, id, billing_customer_id) VALUES(NULL, NULL, 2, NULL)"));
      ii_execute (param_self,
		  rb_str_new2
		  ("INSERT INTO orders (name, shipping_customer_id, id, billing_customer_id) VALUES(NULL, NULL, 3, NULL)"));

      ii_execute (param_self,
		  rb_str_new2 ("select * from orders where id>1"));
      ii_execute (param_self,
		  rb_str_new2 ("select * from customers where id<10"));

      ii_execute (param_self,
		  rb_str_new2
		  ("select column_name, key_sequence from iiindex_columns where index_name='$pk_projects'"));
      ii_execute (param_self,
		  rb_str_new2
		  ("select column_name, key_sequence from iiindex_columns"));

      ii_execute (param_self, rb_str_new2 ("ROLLBACK"));


      printf ("\n%d", loop++);
    }
  ii_disconnect (param_self);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);

  return param_self;
}


VALUE
ii_set_debug_flag (VALUE param_self, VALUE param_debug_flag,
		   VALUE param_debug_flag_value)
{
  int new_debug_value = FALSE;
  char function_name[] = "ii_set_debug_flag";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  Check_Type (param_debug_flag, T_STRING);
  Check_Type (param_debug_flag_value, T_STRING);

  if (!strcmp (RSTRING (param_debug_flag_value)->ptr, "TRUE"))
    new_debug_value = TRUE;

  if (!strcmp (RSTRING (param_debug_flag)->ptr, "GLOBAL_DEBUG"))
    ii_globals.debug = new_debug_value;
  else if (!strcmp (RSTRING (param_debug_flag)->ptr, "DEBUG_TRANSACTIONS"))
    ii_globals.debug_transactions = new_debug_value;
  else if (!strcmp (RSTRING (param_debug_flag)->ptr, "DEBUG_SQL"))
    ii_globals.debug_sql = new_debug_value;
  else if (!strcmp (RSTRING (param_debug_flag)->ptr, "DEBUG_TERMINATION"))
    ii_globals.debug_termination = new_debug_value;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return Qnil;
}


// this routine is in for the Ruby integration
void
Init_Ingres ()
{
  char function_name[] = "Init_Ingres";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  cIngres = rb_define_class ("Ingres", rb_cObject);

  rb_define_method (cIngres, "initialize", ii_init, 0);
  rb_define_method (cIngres, "connect", ii_connect, 1);
  rb_define_method (cIngres, "connect_with_credentials",
		    ii_connect_with_credentials, 3);
  rb_define_method (cIngres, "disconnect", ii_disconnect, 0);
  rb_define_method (cIngres, "execute", ii_execute, 1);
  rb_define_method (cIngres, "pexecute", ii_pexecute, -1);
  rb_define_method (cIngres, "exec", ii_execute, 1);
  rb_define_method (cIngres, "tables", ii_tables, 0);
  rb_define_method (cIngres, "current_database", ii_current_database, 0);
  rb_define_method (cIngres, "data_types", ii_return_data_types, 0);
  rb_define_method (cIngres, "rows_affected", ii_rows_affected, 0);
  rb_define_method (cIngres, "column_list_of_names", ii_column_names, 0);
  rb_define_method (cIngres, "data_sizes", ii_data_sizes, 0);

  rb_define_method (cIngres, "test_default", ii_test_default, 0);
  rb_define_method (cIngres, "crash_it", ii_crash_it, 0);
  rb_define_method (cIngres, "set_debug_flag", ii_set_debug_flag, 2);

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
}


# define        NULLCHAR        ('\0')	/* string terminator */
# define        EOS                NULLCHAR
# define        CM_A_DBL1        0x80	/* 1st byte of double byte character */
# define        CM_A_SPACE        0x10	/* SPACE */
# define cmdblspace(str) (((*(str)&0377) == 0xA1) && ((*((str)+1)&0377) == 0xA1))

extern u_i2 *CM_AttrTab;
char *CM_CaseTab;

# if defined(NT_GENERIC)
FUNC_EXTERN u_i2 *CMgetAttrTab (
#ifdef  CL_PROTOTYPED
				 void
#endif
  );

FUNC_EXTERN char *CMgetCaseTab (
#ifdef  CL_PROTOTYPED
				 void
#endif
  );
# endif


# define CMbytecnt(ptr)        (1)
# define CMnext(str)        (++(str))
# define CMdbl1st(str)        (FALSE)

# if defined(_MSC_VER) || (defined(NT_GENERIC) && defined(IMPORT_DLL_DATA))
# define CMwhite(str)        ((((u_i2 *)CMgetAttrTab())[*(str)&0377] & CM_A_SPACE) != 0)
# else /* NT_GENERIC && IMPORT_DLL_DATA */
# define CMwhite(str)        ((CM_AttrTab[*(str)&0377] & CM_A_SPACE) != 0)
# endif	/* NT_GENERIC && IMPORT_DLL_DATA */



size_t
STtrmwhite (register char *string)
{
  register size_t len;
  register char *nw = NULL;
  register size_t nwl;
  char function_name[] = "STtrmwhite";
  if (ii_globals.debug)
    printf ("Entering %s.\n", function_name);

  /*
   ** after the loop, nw points to the first character beyond
   ** the last non-white character.  Done this way because you
   ** can't reverse scan a string with CM efficiently
   */
  len = 0;
  nwl = 0;
  nw = string;
  while (*string != EOS)
    {
      len += CMbytecnt (string);
      if (!CMwhite (string))
	{
	  CMnext (string);
	  nw = string;
	  nwl = len;
	}
      else
	{
	  CMnext (string);
	}
    }

  if (nw != string)
    *nw = EOS;

  if (ii_globals.debug)
    printf ("Exiting %s.\n", function_name);
  return nwl;
}


static int
utf8_to_utf16 (UTF8 * sourceStart,
	       const UTF8 * sourceEnd,
	       UCS2 * targetStart, const UCS2 * targetEnd, long *reslen)
{
  int result = FALSE;
  register UTF8 *source = sourceStart;
  register UCS2 *target = targetStart;
  register UCS4 ch;
  register u_i2 extraBytesToWrite;
  const i4 halfShift = 10;

  const UCS4 halfBase = 0x0010000UL, halfMask = 0x3FFUL,
    kReplacementCharacter = 0x0000FFFDUL,
    kMaximumUCS2 = 0x0000FFFFUL, kMaximumUCS4 = 0x7FFFFFFFUL,
    kSurrogateHighStart = 0xD800UL, kSurrogateHighEnd = 0xDBFFUL,
    kSurrogateLowStart = 0xDC00UL, kSurrogateLowEnd = 0xDFFFUL;

  UCS4 offsetsFromUTF8[6] = { 0x00000000UL, 0x00003080UL, 0x000E2080UL,
    0x03C82080UL, 0xFA082080UL, 0x82082080UL
  };

  char bytesFromUTF8[256] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4,
    4, 4, 4, 5, 5, 5, 5
  };

  while (source < sourceEnd)
    {
      ch = 0;
      extraBytesToWrite = bytesFromUTF8[*source];
      if (source + extraBytesToWrite > sourceEnd)
	{
	  *reslen = target - targetStart;
	  return TRUE;
	}

      switch (extraBytesToWrite)	/* note: code falls through cases! */
	{
	case 5:
	  ch += *source++;
	  ch <<= 6;
	case 4:
	  ch += *source++;
	  ch <<= 6;
	case 3:
	  ch += *source++;
	  ch <<= 6;
	case 2:
	  ch += *source++;
	  ch <<= 6;
	case 1:
	  ch += *source++;
	  ch <<= 6;
	case 0:
	  ch += *source++;
	}
      ch -= offsetsFromUTF8[extraBytesToWrite];

      if (target >= targetEnd)
	{
	  *reslen = target - targetStart;
	  return result;
	}

      if (ch <= kMaximumUCS2)
	*target++ = ch;
      else if (ch > kMaximumUCS4)
	*target++ = kReplacementCharacter;
      else
	{
	  if (target + 1 >= targetEnd)
	    {
	      *reslen = target - targetStart;
	      return result;
	    }
	  ch -= halfBase;
	  *target++ = (ch >> halfShift) + kSurrogateHighStart;
	  *target++ = (ch & halfMask) + kSurrogateLowStart;
	}
    }
  *reslen = target - targetStart;
  return result;
}



static int
utf16_to_utf8 (UCS2 * sourceStart,
	       const UCS2 * sourceEnd,
	       UTF8 * targetStart, const UTF8 * targetEnd, long *reslen)
{
  int result = FALSE;
  register UCS2 *source = sourceStart;
  register UTF8 *target = targetStart;
  register UCS4 ch, ch2;
  register u_i2 bytesToWrite;
  register const UCS4 byteMask = 0xBF;
  register const UCS4 byteMark = 0x80;
  const i4 halfShift = 10;
  UTF8 firstByteMark[7] = { 0x00, 0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC };

  const UCS4 halfBase = 0x0010000UL, halfMask = 0x3FFUL,
    kReplacementCharacter = 0x0000FFFDUL,
    kMaximumUCS2 = 0x0000FFFFUL, kMaximumUCS4 = 0x7FFFFFFFUL,
    kSurrogateHighStart = 0xD800UL, kSurrogateHighEnd = 0xDBFFUL,
    kSurrogateLowStart = 0xDC00UL, kSurrogateLowEnd = 0xDFFFUL;


  while (source < sourceEnd)
    {
      bytesToWrite = 0;
      ch = *source++;
      if (ch >= kSurrogateHighStart && ch <= kSurrogateHighEnd
	  && source < sourceEnd)
	{
	  ch2 = *source;
	  if (ch2 >= kSurrogateLowStart && ch2 <= kSurrogateLowEnd)
	    {
	      ch = ((ch - kSurrogateHighStart) << halfShift)
		+ (ch2 - kSurrogateLowStart) + halfBase;
	      ++source;
	    }
	}
      if (ch < 0x80)
	bytesToWrite = 1;
      else if (ch < 0x800)
	bytesToWrite = 2;
      else if (ch < 0x10000)
	bytesToWrite = 3;
      else if (ch < 0x200000)
	bytesToWrite = 4;
      else if (ch < 0x4000000)
	bytesToWrite = 5;
      else if (ch <= kMaximumUCS4)
	bytesToWrite = 6;
      else
	{
	  bytesToWrite = 2;
	  ch = kReplacementCharacter;
	}

      target += bytesToWrite;
      if (target > targetEnd)
	{
	  target -= bytesToWrite;
	  *reslen = target - targetStart;
	  return result;
	}
      switch (bytesToWrite)	/* note: code falls through cases! */
	{
	case 6:
	  *--target = (ch | byteMark) & byteMask;
	  ch >>= 6;
	case 5:
	  *--target = (ch | byteMark) & byteMask;
	  ch >>= 6;
	case 4:
	  *--target = (ch | byteMark) & byteMask;
	  ch >>= 6;
	case 3:
	  *--target = (ch | byteMark) & byteMask;
	  ch >>= 6;
	case 2:
	  *--target = (ch | byteMark) & byteMask;
	  ch >>= 6;
	case 1:
	  *--target = ch | firstByteMark[bytesToWrite];
	}
      target += bytesToWrite;
    }
  *reslen = target - targetStart;
  return result;
}

/*
vim:  ts=4 sw=4 expandtab
*/
