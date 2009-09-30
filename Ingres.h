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
**              24-Aug-2009 (Grant.Croker@ingres.com)
**                  Created
*/

/* Constants */
#define RUBY_INGRES_VERSION "1.4.0-dev"
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


/* Query types */

#define INGRES_SQL_SELECT 1
#define INGRES_SQL_INSERT 2
#define INGRES_SQL_UPDATE 3
#define INGRES_SQL_DELETE 4
#define INGRES_SQL_COMMIT 5
#define INGRES_SQL_ROLLBACK 6
#define INGRES_SQL_OPEN 7
#define INGRES_SQL_CLOSE 8
#define INGRES_SQL_CONNECT 9
#define INGRES_SQL_DISCONNECT 10
#define INGRES_SQL_GETDBEVENT 11
#define INGRES_SQL_SAVEPOINT 12
#define INGRES_SQL_AUTOCOMMIT 13
#define INGRES_SQL_EXECUTE_PROCEDURE 14
#define INGRES_SQL_CALL 15
#define INGRES_SQL_COPY 16
#define INGRES_SQL_CREATE 17
#define INGRES_SQL_ALTER 18
#define INGRES_SQL_DROP 19
#define INGRES_SQL_GRANT 20
#define INGRES_SQL_REVOKE 21
#define INGRES_SQL_MODIFY 22
#define INGRES_SQL_SET 23
/* Not really an SQL Statement but INGRES_START_TRANSACTION is expected to be handled as one */
#define INGRES_START_TRANSACTION 24

#define INGRES_NO_OF_COMMANDS 24
static struct
{
  char        *command;
  int         code;
} SQL_COMMANDS [INGRES_NO_OF_COMMANDS] =
{
  { "SELECT", INGRES_SQL_SELECT },
  { "INSERT", INGRES_SQL_INSERT },
  { "UPDATE", INGRES_SQL_UPDATE },
  { "DELETE", INGRES_SQL_DELETE },
  { "COMMIT", INGRES_SQL_COMMIT },
  { "ROLLBACK", INGRES_SQL_ROLLBACK },
  { "OPEN", INGRES_SQL_OPEN },
  { "CLOSE", INGRES_SQL_CLOSE },
  { "CONNECT", INGRES_SQL_CONNECT },
  { "DISCONNECT", INGRES_SQL_DISCONNECT },
  { "GET DBEVENT", INGRES_SQL_GETDBEVENT },
  { "SAVEPOINT", INGRES_SQL_SAVEPOINT },
  { "SET AUTOCOMMIT", INGRES_SQL_AUTOCOMMIT },
  { "EXECUTE PROCEDURE", INGRES_SQL_EXECUTE_PROCEDURE },
  { "CALL", INGRES_SQL_CALL },
  { "COPY", INGRES_SQL_COPY },
  { "CREATE", INGRES_SQL_CREATE },
  { "ALTER", INGRES_SQL_ALTER },
  { "DROP", INGRES_SQL_DROP },
  { "GRANT", INGRES_SQL_GRANT },
  { "REVOKE", INGRES_SQL_REVOKE },
  { "MODIFY", INGRES_SQL_MODIFY },
  { "SET", INGRES_SQL_SET },
  { "START TRANSACTION", INGRES_START_TRANSACTION },
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
void ii_api_rollback (II_CONN *ii_conn );
void ii_api_disconnect( II_CONN *ii_conn);

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

/*
vim:  ts=2 sw=2 expandtab
*/
