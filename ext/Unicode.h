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
**      UNCIODE.H
**
**      History
**              24-Aug-2009 (Grant.Croker@ingres.com)
**                  Created
*/

#include <stdlib.h>

typedef unsigned short ucs2;
typedef unsigned long ucs4;
typedef unsigned char utf8;

typedef short i2;
typedef long i4;
typedef unsigned short u_i2;
#ifndef _WIN32
typedef long long __int64;
#endif

#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 0
#endif

#define UTF8				utf8
#define UCS2				ucs2
#define UCS4				ucs4

int utf8_to_utf16 (UTF8 * sourceStart, const UTF8 * sourceEnd, UCS2 * targetStart, const UCS2 * targetEnd, long *reslen);
int utf16_to_utf8 (UCS2 * sourceStart, const UCS2 * sourceEnd, UTF8 * targetStart, const UTF8 * targetEnd, long *reslen);
/*
vim:  ts=2 sw=2 expandtab
*/
