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
**      UNCIODE.C
**
**      History
**              24-Aug-2009 (Grant.Croker@ingres.com)
**                  Created / Imported from Ingres.c
*/

#include "Unicode.h"

int utf8_to_utf16 (UTF8 * sourceStart, const UTF8 * sourceEnd, UCS2 * targetStart, const UCS2 * targetEnd, long *reslen)
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

    UCS4 offsetsFromUTF8[6] = { 0x00000000UL, 0x00003080UL, 0x000E2080UL, 0x03C82080UL, 0xFA082080UL, 0x82082080UL
                              };

    char bytesFromUTF8[256] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5
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

        switch (extraBytesToWrite)  /* note: code falls through cases! */
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
        {
            *target++ = ch;
        }
        else if (ch > kMaximumUCS4)
        {
            *target++ = kReplacementCharacter;
        }
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

int utf16_to_utf8 (UCS2 * sourceStart, const UCS2 * sourceEnd, UTF8 * targetStart, const UTF8 * targetEnd, long *reslen)
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
        if (ch >= kSurrogateHighStart && ch <= kSurrogateHighEnd && source < sourceEnd)
        {
            ch2 = *source;
            if (ch2 >= kSurrogateLowStart && ch2 <= kSurrogateLowEnd)
            {
                ch = ((ch - kSurrogateHighStart) << halfShift) + (ch2 - kSurrogateLowStart) + halfBase;
                ++source;
            }
        }
        if (ch < 0x80)
        {
            bytesToWrite = 1;
        }
        else if (ch < 0x800)
        {
            bytesToWrite = 2;
        }
        else if (ch < 0x10000)
        {
            bytesToWrite = 3;
        }
        else if (ch < 0x200000)
        {
            bytesToWrite = 4;
        }
        else if (ch < 0x4000000)
        {
            bytesToWrite = 5;
        }
        else if (ch <= kMaximumUCS4)
        {
            bytesToWrite = 6;
        }
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
        switch (bytesToWrite)  /* note: code falls through cases! */
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
vim:  ts=2 sw=2 expandtab
*/
