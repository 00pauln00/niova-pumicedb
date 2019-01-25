/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef _LOCAL_CTL_INTERFACE_
#define _LOCAL_CTL_INTERFACE_ 1

#include "common.h"
#include "ctor.h"

void
lctli_subsystem_init(void)
    __attribute__ ((constructor (LCTLI_SUBSYS_PRIORITY)));

void
lctli_subsystem_destroy(void)
    __attribute__ ((destructor (LCTLI_SUBSYS_PRIORITY)));

#endif //_LOCAL_CTL_INTERFACE_
