/*
**      Copyright (c) 2009 Ingres Corporation
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

/*      static char     Sccsid[] = "$Id$";        */

/*
**      INGRES.H
**
**      History
**              08/24/2009 (grant.croker@ingres.com)
**                  Created
**              11/09/2009 (grant.croker@ingres.com)
**                  Add the ability to set the date format for Ingres date values
**                  from within Ruby 
**              03/30/2010 (grant.croker@ingres.com)
**                      Fix #565 - Compilation errors with Redhat ES 5.4 (x86-64)
*/

/* Constants */
#define RUBY_INGRES_VERSION "1.4.0"
#define RUBY_INGRES_API IIAPI_VERSION

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
#define LOB_SEGMENT_SIZE 8192

/* Ingres 2.6 is missing a define for IIAPI_CPV_DFRMT_ISO4 */
#if !defined(IIAPI_CPV_DFRMT_ISO4)
  #define IIAPI_CPV_DFRMT_ISO4 9
#endif

/* Query types */

#define INGRES_SQL_SELECT             0
#define INGRES_SQL_INSERT             1
#define INGRES_SQL_UPDATE             2
#define INGRES_SQL_DELETE             3
#define INGRES_SQL_COMMIT             4
#define INGRES_SQL_ROLLBACK           5
#define INGRES_SQL_OPEN               6
#define INGRES_SQL_CLOSE              7
#define INGRES_SQL_CONNECT            8
#define INGRES_SQL_DISCONNECT         9 
#define INGRES_SQL_GETDBEVENT         10
#define INGRES_SQL_SAVEPOINT          11
#define INGRES_SQL_AUTOCOMMIT         12
#define INGRES_SQL_EXECUTE_PROCEDURE  13
#define INGRES_SQL_CALL               14
#define INGRES_SQL_COPY               15
#define INGRES_SQL_CREATE             16
#define INGRES_SQL_ALTER              17
#define INGRES_SQL_DROP               18
#define INGRES_SQL_GRANT              19
#define INGRES_SQL_REVOKE             20
#define INGRES_SQL_MODIFY             21
#define INGRES_SQL_SET                22
/* Not really an SQL Statement but INGRES_START_TRANSACTION is expected to be handled as one */
#define INGRES_START_TRANSACTION      23
#define INGRES_SQL_ROLLBACK_WORK_TO   24
#define INGRES_SQL_ROLLBACK_TO        25

#define INGRES_NO_OF_COMMANDS         26
static struct
{
  char        *command;
  int         code;
  int         length ;
} SQL_COMMANDS [INGRES_NO_OF_COMMANDS] =
{
  { "SELECT", INGRES_SQL_SELECT, 6 },
  { "INSERT", INGRES_SQL_INSERT, 6 },
  { "UPDATE", INGRES_SQL_UPDATE, 6 },
  { "DELETE", INGRES_SQL_DELETE, 6 },
  { "COMMIT", INGRES_SQL_COMMIT, 6 },
  { "ROLLBACK", INGRES_SQL_ROLLBACK, 8 },
  { "OPEN", INGRES_SQL_OPEN, 4 },
  { "CLOSE", INGRES_SQL_CLOSE, 5 },
  { "CONNECT", INGRES_SQL_CONNECT, 7 },
  { "DISCONNECT", INGRES_SQL_DISCONNECT, 10 },
  { "GET DBEVENT", INGRES_SQL_GETDBEVENT, 11 },
  { "SAVEPOINT", INGRES_SQL_SAVEPOINT, 9 },
  { "SET AUTOCOMMIT", INGRES_SQL_AUTOCOMMIT, 14 },
  { "EXECUTE PROCEDURE", INGRES_SQL_EXECUTE_PROCEDURE, 17 },
  { "CALL", INGRES_SQL_CALL, 4 },
  { "COPY", INGRES_SQL_COPY, 4 },
  { "CREATE", INGRES_SQL_CREATE, 6 },
  { "ALTER", INGRES_SQL_ALTER, 5 },
  { "DROP", INGRES_SQL_DROP, 4 },
  { "GRANT", INGRES_SQL_GRANT, 5 },
  { "REVOKE", INGRES_SQL_REVOKE, 6 },
  { "MODIFY", INGRES_SQL_MODIFY, 6 },
  { "SET", INGRES_SQL_SET, 3 },
  { "START TRANSACTION", INGRES_START_TRANSACTION, 17 },
  { "ROLLBACK WORK TO", INGRES_SQL_ROLLBACK_WORK_TO, 16 },
  { "ROLLBACK TO", INGRES_SQL_ROLLBACK_TO, 11 },
};

#define INGRES_NO_CONN_PARAMS  1
static struct
{
  char *paramName;
  II_LONG paramID;
} CONN_PARAMS [INGRES_NO_CONN_PARAMS] =
{
  {"date_format", IIAPI_CP_DATE_FORMAT },  /* for SELECT VARCHAR(date_col) */
};

#define INGRES_NO_ENV_PARAMS  1
static struct
{
  char *paramName;
  II_LONG paramID;
} ENV_PARAMS [INGRES_NO_ENV_PARAMS] =
{
  {"date_format", IIAPI_EP_DATE_FORMAT },  /* for SELECT date_col */
};

/* Cursor Modes */
#define INGRES_CURSOR_READONLY 0
#define INGRES_CURSOR_UPDATE 1

typedef struct _II_GLOBALS
{
  II_PTR envHandle;
  int debug;
  int debug_connection;
  int debug_sql;
  int debug_termination;
  int debug_transactions;
} II_GLOBALS;

typedef struct _II_SAVEPOINT_ENTRY
{
  char   *savePtName;
  II_PTR savePtHandle;
  II_PTR nextSavePtEntry;
} II_SAVEPOINT_ENTRY;

typedef struct _II_CONN
{
  int autocommit;
  II_PTR connHandle;
  II_PTR tranHandle;
  II_PTR stmtHandle;
  II_PTR envHandle;
  II_LONG fieldCount;
  II_LONG lobSegmentSize;
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
  int queryType;
  VALUE keep_me;
  VALUE resultset;
  VALUE r_column_names;
  VALUE r_data_sizes;
  VALUE r_data_types;
  II_LONG columnCount;
  II_PTR savePtList;
  II_SAVEPOINT_ENTRY *lastSavePtEntry;/* Pointer to the last savePtEntry on savePtList */
} II_CONN;

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

#define rb_define_singleton_alias(klass,new,old) rb_define_alias(rb_singleton_class(klass),new,old)


/*
 * Function declarations
 */
VALUE ii_execute (int param_argc, VALUE * param_argv, VALUE param_self);
VALUE ii_connect (int param_argc, VALUE * param_argv, VALUE param_self);
static VALUE ing_disconnect (VALUE param_self);
VALUE ing_connect (VALUE param_self, VALUE param_targetDB);
static void ii_conn_init(II_CONN *ii_conn);
VALUE ing_init (int argc, VALUE *argv, VALUE self);
void ing_api_init ();
static VALUE rb_ingres_alloc(VALUE klass);
static void free_ii_conn (II_CONN *ing_conn);
static void ing_starttransaction();
static void ing_commit();
static void ing_rollback();
VALUE ing_query (char *param_sqlText);
static int ing_querytype(char *queryText);
VALUE ing_execute (VALUE argc, VALUE *argv, VALUE self);
II_PTR executeQuery (II_CONN *ii_conn, char *param_sqlText);
static void ing_conn_init(II_CONN *ing_conn);
void ii_api_query_close (II_CONN *ii_conn);
void ii_api_connect (II_CONN *ii_conn, char *param_targetDB, char *param_username, char *param_password);
void ii_api_commit (II_CONN *ii_conn);
static int ii_query_type(char *queryText);
void ii_api_rollback (II_CONN *ii_conn, II_SAVEPOINT_ENTRY *savePtEntry);
void ii_api_disconnect( II_CONN *ii_conn);
void ii_api_savepoint (II_CONN *ii_conn, VALUE savePtName);
void ii_free_savePtEntries (II_SAVEPOINT_ENTRY *savePtEntry);

void ii_api_set_connect_param (II_CONN *ii_conn, II_LONG paramID, VALUE paramValue);
void ii_api_set_env_param (II_LONG paramID, VALUE paramValue);

/* Transaction control */
VALUE ii_commit (VALUE param_self);
VALUE ii_rollback (int param_argc, VALUE * param_argv, VALUE param_self);
VALUE ii_savepoint (VALUE param_self, VALUE param_savepointName);
VALUE ii_savepoint_name(II_CONN *ii_conn, char *statement);

/* Memory Allocation/Deallocation */
void *ii_allocate (size_t nitems, size_t size);
void *ii_reallocate (void *oldPtr, size_t nitems, size_t size);
void ii_free (void **ptr);

/* TODO - The following has been taken from the Ingres CL and should be removed/replaced at some point */
# define        NULLCHAR        ('\0')	/* string terminator */
# define        EOS             NULLCHAR
# define        CM_A_DBL1       0x80	/* 1st byte of double byte character */
# define        CM_A_SPACE      0x10	/* SPACE */
# define        cmdblspace(str) (((*(str)&0377) == 0xA1) && ((*((str)+1)&0377) == 0xA1))

# if defined(NT_GENERIC)
FUNC_EXTERN u_i2 *CMgetAttrTab ();
FUNC_EXTERN char *CMgetCaseTab ();
# endif

# define CMbytecnt(ptr)         (1)
# define CMnext(str)            (++(str))
# define CMdbl1st(str)          (FALSE)

# if defined(_MSC_VER) || (defined(NT_GENERIC) && defined(IMPORT_DLL_DATA))
# define CMwhite(str)           ((((u_i2 *)CMgetAttrTab())[*(str)&0377] & CM_A_SPACE) != 0)
# else /* NT_GENERIC && IMPORT_DLL_DATA */
# define CMwhite(str)           ((CM_AttrTab[*(str)&0377] & CM_A_SPACE) != 0)
# endif	/* NT_GENERIC && IMPORT_DLL_DATA */

/* available via Ingres Compatibility Layer */
/* TODO - Remove dependency on the Ingres CL */
size_t STtrmwhite (register char *string);

/* Certain Ruby releases shipped with different Linux distributions do not contain
 * the necessary defines. E.g. RedHat ES 5.4 ships with Ruby 1.8.5.
 */
#ifdef RUBY_VERSION_CODE
#  if RUBY_VERSION_CODE < 186
#    define RARRAY_LEN(s) (RARRAY(s)->len)
#    define RSTRING_PTR(s) (RSTRING(s)->ptr)
#    define RSTRING_LEN(s) (RSTRING(s)->len)
#  endif /* RUBY_VERSION_CODE < 186 */
#endif

/*
vim:  ts=2 sw=2 expandtab
*/
